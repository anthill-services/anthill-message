
from anthill.common.model import Model
from anthill.common.database import DatabaseError
from anthill.common.validate import validate
from anthill.common.profile import Profile, ProfileError

from . import MessageError, MessageFlags, CLASS_USER

import ujson


class MessageQueryError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class LastReadMessageAdapter(object):
    def __init__(self, data):
        self.recipient_class = data.get("message_recipient_class")
        self.recipient = data.get("message_recipient")
        self.time = data.get("last_message_time")
        self.uuid = data.get("last_message_uuid")

    def dump(self):
        return {
            "recipient_class": self.recipient_class,
            "recipient": self.recipient,
            "time": str(self.time),
            "uuid": self.uuid
        }


class MessageAdapter(object):
    def __init__(self, data):
        self.message_id = data.get("message_id")
        self.message_uuid = data.get("message_uuid")
        self.recipient_class = str(data.get("message_recipient_class"))
        self.sender = str(data.get("message_sender"))
        self.recipient = str(data.get("message_recipient"))
        self.time = data.get("message_time")
        self.message_type = data.get("message_type")
        self.payload = data.get("message_payload")
        if isinstance(self.payload, str):
            self.payload = ujson.loads(self.payload)
        self.delivered = data.get("message_delivered")

        flags = data.get("message_flags", "").lower().split(",")

        self.flags = MessageFlags(flags)

    def dump(self):
        return {
            "recipient_class": self.recipient_class,
            "recipient": self.recipient,
            "sender": self.sender,
            "time": self.time,
            "message_type": self.message_type,
            "payload": self.payload
        }


class MessagesQuery(object):
    def __init__(self, gamespace_id, db):
        self.gamespace_id = gamespace_id
        self.db = db

        self.message_sender = None
        self.message_recipient_class = None
        self.message_recipient = None
        self.message_type = None
        self.message_delivered = None

        self.offset = 0
        self.limit = 0

    def __values__(self):
        conditions = [
            "`gamespace_id`=%s"
        ]

        data = [
            str(self.gamespace_id)
        ]

        if self.message_sender:
            conditions.append("`message_sender`=%s")
            data.append(str(self.message_sender))

        if self.message_recipient_class:
            conditions.append("`message_recipient_class`=%s")
            data.append(str(self.message_recipient_class))

        if self.message_recipient:
            conditions.append("`message_recipient` LIKE %s")
            data.append(self.message_recipient)

        if self.message_type:
            conditions.append("`message_type`=%s")
            data.append(str(self.message_type))

        if self.message_delivered is not None:
            conditions.append("`message_delivered`=%s")
            data.append(str(int(bool(self.message_delivered))))

        return conditions, data

    async def query(self, one=False, count=False):
        conditions, data = self.__values__()

        query = """
            SELECT {0} * FROM `messages`
            WHERE {1}
        """.format(
            "SQL_CALC_FOUND_ROWS" if count else "",
            " AND ".join(conditions))

        query += """
            ORDER BY `message_time` DESC
        """

        if self.limit:
            query += """
                LIMIT %s,%s
            """
            data.append(int(self.offset))
            data.append(int(self.limit))

        query += ";"

        if one:
            try:
                result = await self.db.get(query, *data)
            except DatabaseError as e:
                raise MessageQueryError("Failed to add message: " + e.args[1])

            if not result:
                return None

            return MessageAdapter(result)
        else:
            async with self.db.acquire() as db:
                try:
                    result = await db.query(query, *data)
                except DatabaseError as e:
                    raise MessageQueryError("Failed to add message: " + e.args[1])

                count_result = 0

                if count:
                    count_result = await db.get(
                        """
                            SELECT FOUND_ROWS() AS count;
                        """)
                    count_result = count_result["count"]

                items = map(MessageAdapter, result)

                if count:
                    return (items, count_result)

                return items


class MessagesHistoryModel(Model):

    def __init__(self, db, app):
        self.db = db
        self.app = app

    def get_setup_tables(self):
        return ["messages", "last_read_message"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            async with self.db.acquire() as db:
                if gamespace_only:
                    await db.execute(
                        """
                            DELETE FROM `last_read_message`
                            WHERE `gamespace_id`=%s AND `account_id` IN %s;
                        """, gamespace, accounts)
                    await db.execute(
                        """
                            DELETE FROM `messages`
                            WHERE `gamespace_id`=%s AND `message_sender` IN %s;
                        """, gamespace, accounts)
                    await db.execute(
                        """
                            DELETE FROM `messages`
                            WHERE `gamespace_id`=%s AND `message_recipient_class`=%s AND `message_recipient` IN %s;
                        """, gamespace, CLASS_USER, accounts)
                else:
                    await db.execute(
                        """
                            DELETE FROM `last_read_message`
                            WHERE `account_id` IN %s;
                        """, accounts)
                    await db.execute(
                        """
                            DELETE FROM `messages`
                            WHERE `message_sender` IN %s;
                        """, accounts)
                    await db.execute(
                        """
                            DELETE FROM `messages`
                            WHERE `message_recipient_class`=%s AND `message_recipient` IN %s;
                        """, CLASS_USER, accounts)
        except DatabaseError as e:
            raise MessageError(500, "Failed to delete messages: " + e.args[1])

    def messages_query(self, gamespace):
        return MessagesQuery(gamespace, self.db)

    @validate(gamespace="int", sender="int", message_uuid="str", recipient_class="str",
              recipient_key="str", time="datetime", message_type="str", payload="json",
              flags=MessageFlags, delivered="bool")
    async def add_message(self, gamespace, sender, message_uuid, recipient_class,
                    recipient_key, time, message_type, payload, flags, delivered=False):

        if not isinstance(payload, dict):
            raise MessageError(400, "payload should be a dict")

        try:
            message_id = await self.db.insert(
                """
                    INSERT INTO `messages`
                    (`gamespace_id`, `message_uuid`, `message_recipient_class`, `message_sender`,
                        `message_recipient`, `message_time`, `message_type`, `message_payload`,
                        `message_delivered`, `message_flags`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, gamespace, message_uuid, recipient_class, sender,
                recipient_key, time, message_type, ujson.dumps(payload), int(delivered), flags.dump())
        except DatabaseError as e:
            raise MessageError(500, "Failed to add message: " + e.args[1])
        else:
            return message_id

    async def get_message(self, gamespace, message_id):
        try:
            message = await self.db.get(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_id`=%s AND `gamespace_id`=%s;
                """, message_id, gamespace)
        except DatabaseError as e:
            raise MessageError(500, "Failed to get a message: " + e.args[1])

        if not message:
            raise MessageNotFound()

        return MessageAdapter(message)

    async def list_incoming_messages(self, gamespace, recipient_class, recipient, limit=100):
        try:
            messages = await self.db.query(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s
                    ORDER BY `message_time` DESC
                    LIMIT %s;
                """, recipient_class, recipient, gamespace, limit)
        except DatabaseError as e:
            raise MessageError(500, "Failed to list incoming messages: " + e.args[1])

        return list(map(MessageAdapter, messages))

    @validate(gamespace="int", account_id="int", limit="int", offset="int")
    async def list_messages_account_with_count(self, gamespace, account_id, limit=100, offset=0):
        async with self.db.acquire() as db:
            result = await self.list_messages_account_with_count_db(gamespace, account_id, db, limit, offset)
            return result

    @validate(gamespace="int", account_id="int", limit="int", offset="int")
    async def list_messages_account_with_count_db(self, gamespace, account_id, db, limit=100, offset=0):
        messages = await self.list_messages_account(gamespace, account_id, limit, offset, db=db)
        try:
            count_result = await db.get(
                """
                    SELECT FOUND_ROWS() AS count;
                """)
        except DatabaseError as e:
            raise MessageError(500, "Failed to count found rows for account messages: " + e.args[1])

        count_result = count_result["count"]
        return messages, count_result

    @validate(gamespace="int", account_id="int", recipient_account_id="int", limit="int", offset="int")
    async def list_messages_recipient_count(self, gamespace, account_id, recipient_account_id, limit=100, offset=0):

        """
        Returns messages that were sent between account_id and recipient_account_id
        """

        if limit < 1 or limit > 10000 or offset < 0 or offset > 10000:
            raise MessageError(400, "Bad limit/offset")

        async with self.db.acquire() as db:
            try:
                messages = await (db or self.db).query(
                    """
                        SELECT SQL_CALC_FOUND_ROWS * 
                        FROM `messages` 
                        WHERE `gamespace_id`=%s AND `message_recipient_class`=%s AND 
                              `message_recipient`=%s AND `message_sender`=%s
                        UNION DISTINCT
                        (
                            SELECT * 
                            FROM `messages` 
                            WHERE `gamespace_id`=%s AND `message_recipient_class`=%s AND 
                                  `message_recipient`=%s AND `message_sender`=%s
                        )
                        ORDER BY `message_id` DESC
                        LIMIT %s, %s;
                    """, gamespace, CLASS_USER, str(account_id), str(recipient_account_id),
                    gamespace, CLASS_USER, str(recipient_account_id), str(account_id),
                    offset, limit)
            except DatabaseError as e:
                raise MessageError(500, "Failed to list incoming messages for account: " + e.args[1])

            count_result = await db.get(
                """
                    SELECT FOUND_ROWS() AS count;
                """)
            count_result = count_result["count"]

            return list(map(MessageAdapter, messages)), count_result

    @validate(gamespace="int", account_id="int", limit="int", offset="int")
    async def list_messages_account(self, gamespace, account_id, limit=100, offset=0, db=None):
        """
        Returns last N..M (offset to limit) messages being sent or received by the account,
            including the ones being sent to the groups the account participates in.
        """

        if limit < 1 or limit > 10000 or offset < 0 or offset > 10000:
            raise MessageError(400, "Bad limit/offset")

        try:
            messages = await (db or self.db).query(
                # now this I call a query. yet it executes in 1ms with 40000 messages in db
                """
                    SELECT SQL_CALC_FOUND_ROWS * 
                    FROM `messages` 
                    WHERE `messages`.`gamespace_id`=%s
                    AND (`messages`.`message_recipient_class`, `messages`.`message_recipient`) IN (
                        SELECT `groups`.`group_class`, `groups`.`group_key` 
                        FROM `groups`, `group_participants`
                        WHERE `groups`.`group_class`=`messages`.`message_recipient_class` 
                            AND `groups`.`group_key`=`messages`.`message_recipient`
                            AND `groups`.`group_id`=`group_participants`.`group_id` 
                            AND `group_participants`.`participation_account`=%s
                    )
                    UNION DISTINCT
                    (
                        SELECT * 
                        FROM `messages` 
                        WHERE `gamespace_id`=%s AND `message_recipient_class`=%s AND `message_recipient`=%s
                    )
                    UNION DISTINCT
                    (
                        SELECT * 
                        FROM `messages` 
                        WHERE `gamespace_id`=%s AND `message_sender`=%s
                    )
                    ORDER BY `message_id` DESC
                    LIMIT %s, %s;
                """, gamespace, str(account_id), gamespace, CLASS_USER,
                str(account_id), gamespace, str(account_id), offset, limit)
        except DatabaseError as e:
            raise MessageError(500, "Failed to list incoming messages for account: " + e.args[1])

        return list(map(MessageAdapter, messages))

    async def read_incoming_messages(self, gamespace, recipient_class, recipient, receiver):
        try:
            async with self.db.acquire(auto_commit=False) as db:
                messages = await db.query(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE `message_recipient_class`=%s AND `message_recipient`=%s
                            AND `gamespace_id`=%s AND `message_delivered`=0
                        FOR UPDATE;
                    """, recipient_class, recipient, gamespace)

                mark_delivered_ids = []
                remove_ids = []

                for m in map(MessageAdapter, messages):
                    recv = await receiver(m)
                    if recv:
                        if MessageFlags.REMOVE_DELIVERED in m.flags:
                            remove_ids.append(m.message_id)
                        else:
                            mark_delivered_ids.append(m.message_id)

                if mark_delivered_ids:
                    await db.query(
                        """
                            UPDATE `messages`
                            SET `message_delivered`=1
                            WHERE `gamespace_id`=%s AND `message_id` IN %s;
                        """, gamespace, mark_delivered_ids
                    )

                if remove_ids:
                    await db.query(
                        """
                            DELETE FROM `messages`
                            WHERE `gamespace_id`=%s AND `message_id` IN %s;
                        """, gamespace, remove_ids
                    )

                await db.commit()

        except DatabaseError as e:
            raise MessageError(500, "Failed to read incoming messages: " + e.args[1])

    async def delete_messages(self, gamespace, recipient_class, recipient):
        try:
            await self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, recipient_class, recipient, gamespace)
        except DatabaseError as e:
            raise MessageError(500, "Failed to delete messages: " + e.args[1])

    async def delete_messages_like(self, gamespace, recipient_class, recipient_like):
        try:
            await self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_recipient_class` LIKE %s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, recipient_class, recipient_like, gamespace)
        except DatabaseError as e:
            raise MessageError(500, "Failed to delete messages: " + e.args[1])

    async def delete_message(self, gamespace, message_id):
        try:
            await self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_id`=%s AND `gamespace_id`=%s;
                """, message_id, gamespace)
        except DatabaseError as e:
            raise MessageError(500, "Failed to delete a message: " + e.args[1])

    async def delete_message_concurrent(self, gamespace, sender, message_uuid):
        async with self.db.acquire(auto_commit=False) as db:
            try:
                message = await db.get(
                    """
                        SELECT `message_recipient_class`, `message_recipient`, `message_flags`, `message_sender`,
                            `message_type`
                        FROM `messages`
                        WHERE `message_uuid`=%s AND `gamespace_id`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """, message_uuid, gamespace)

                if message is None:
                    raise MessageNotFound()

                # sender can always delete his message
                if str(message["message_sender"]) != str(sender):
                    flags = MessageFlags(message["message_flags"].lower().split(","))

                    if MessageFlags.DELETABLE not in flags:
                        raise MessageError(409, "This message is not deletable")

                message_recipient_class = message["message_recipient_class"]
                message_recipient = message["message_recipient"]
                message_type = message["message_type"]

                await self.app.message_queue.delete_message(
                    gamespace, sender, message_type, message_recipient_class, message_recipient, message_uuid)

                await db.execute(
                    """
                        DELETE FROM `messages`
                        WHERE `message_uuid`=%s AND `gamespace_id`=%s
                        LIMIT 1;
                    """, message_uuid, gamespace)

            except DatabaseError as e:
                raise MessageError(500, "Failed to delete a message: " + e.args[1])
            finally:
                await db.commit()

    async def update_message_concurrent(self, gamespace, sender, message_uuid, update):
        async with self.db.acquire(auto_commit=False) as db:
            try:
                message = await db.get(
                    """
                        SELECT `message_recipient_class`, `message_recipient`, `message_payload`, 
                            `message_flags`, `message_sender`, `message_type`
                        FROM `messages`
                        WHERE `message_uuid`=%s AND `gamespace_id`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """, message_uuid, gamespace)

                if message is None:
                    raise MessageNotFound()

                # sender can always edit his message
                if str(message["message_sender"]) != str(sender):
                    flags = MessageFlags(message["message_flags"].lower().split(","))

                    if MessageFlags.EDITABLE not in flags:
                        raise MessageError(409, "This message is not editable")

                message_recipient_class = message["message_recipient_class"]
                message_recipient = message["message_recipient"]
                message_payload = message["message_payload"]
                message_type = message["message_type"]

                try:
                    updated = Profile.merge_data(message_payload, update, None, merge=True)
                except ProfileError as e:
                    raise MessageError(400, e.message)

                await self.app.message_queue.update_message(
                    gamespace, sender, message_type, message_recipient_class,
                    message_recipient, message_uuid, updated)

                await db.execute(
                    """
                        UPDATE `messages`
                        SET `message_payload`=%s
                        WHERE `message_uuid`=%s AND `gamespace_id`=%s
                        LIMIT 1;
                    """, ujson.dumps(updated), message_uuid, gamespace)

            except DatabaseError as e:
                raise MessageError(500, "Failed to delete a message: " + e.args[1])
            finally:
                await db.commit()

    async def list_read_messages(self, gamespace_id, account_id, db=None):
        try:
            read_messages = await (db or self.db).query(
                """
                    SELECT *
                    FROM `last_read_message`
                    WHERE `gamespace_id`=%s AND `account_id`=%s;
                """, gamespace_id, account_id)
        except DatabaseError as e:
            raise MessageError(500, "Failed to get a message: " + e.args[1])

        return list(map(LastReadMessageAdapter, read_messages))

    async def get_message_uuid(self, gamespace, message_uuid):
        try:
            message = await self.db.get(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_uuid`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                """, message_uuid, gamespace)
        except DatabaseError as e:
            raise MessageError(500, "Failed to get a message: " + e.args[1])

        if not message:
            raise MessageNotFound()

        return MessageAdapter(message)

    async def mark_message_as_read(self, gamespace, account_id, message_uuid):
        async with self.db.acquire() as db:
            try:
                message = await db.get(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE `message_uuid`=%s AND `gamespace_id`=%s
                        LIMIT 1;
                    """, message_uuid, gamespace)
            except DatabaseError as e:
                raise MessageError(500, "Failed to get a message: " + e.args[1])

            if not message:
                raise MessageNotFound()

            recipient_class = message["message_recipient_class"]
            recipient = message["message_recipient"]
            time = message["message_time"]

            rows_updated = await db.execute(
                # this statement add a new record and updates if a record already exists, but only
                # if the message is newer than the older one
                """
                INSERT INTO `last_read_message`
                (gamespace_id, account_id, message_recipient_class, 
                    message_recipient, last_message_time, last_message_uuid) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    last_message_time = IF(
                        VALUES(last_message_time) > last_message_time,
                        VALUES(last_message_time),
                        last_message_time
                    ),
                    last_message_uuid = VALUES(last_message_uuid);
                """, gamespace, account_id, recipient_class, recipient, time, message_uuid
            )

            return bool(rows_updated)


class MessageNotFound(Exception):
    pass

