
from tornado.gen import coroutine, Return
from tornado.queues import QueueEmpty

from common.model import Model
from common.database import DatabaseError
from common.validate import validate

from . import MessageError, DeliveryFlags

import ujson
import logging


class MessageQueryError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


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
        self.delivered = data.get("message_delivered")

        flags = data.get("message_flags", "").lower().split(",")

        self.flags = DeliveryFlags(flags)


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

    @coroutine
    def query(self, one=False, count=False):
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
                result = yield self.db.get(query, *data)
            except DatabaseError as e:
                raise MessageQueryError("Failed to add message: " + e.args[1])

            if not result:
                raise Return(None)

            raise Return(MessageAdapter(result))
        else:
            try:
                result = yield self.db.query(query, *data)
            except DatabaseError as e:
                raise MessageQueryError("Failed to add message: " + e.args[1])

            count_result = 0

            if count:
                count_result = yield self.db.get(
                    """
                        SELECT FOUND_ROWS() AS count;
                    """)
                count_result = count_result["count"]

            items = map(MessageAdapter, result)

            if count:
                raise Return((items, count_result))

            raise Return(items)


class MessagesHistoryModel(Model):

    def __init__(self, db):
        self.db = db

    def get_setup_tables(self):
        return ["messages"]

    def get_setup_db(self):
        return self.db

    def messages_query(self, gamespace):
        return MessagesQuery(gamespace, self.db)

    @coroutine
    @validate(gamespace="int", sender="int", message_uuid="str", recipient_class="str",
              recipient_key="str", time="datetime", message_type="str", payload="json",
              flags=DeliveryFlags, delivered="bool")
    def add_message(self, gamespace, sender, message_uuid, recipient_class,
                    recipient_key, time, message_type, payload, flags, delivered=False):

        if not isinstance(payload, dict):
            raise MessageError("payload should be a dict")

        try:
            message_id = yield self.db.insert(
                """
                    INSERT INTO `messages`
                    (`gamespace_id`, `message_uuid`, `message_recipient_class`, `message_sender`,
                        `message_recipient`, `message_time`, `message_type`, `message_payload`,
                        `message_delivered`, `message_flags`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, gamespace, message_uuid, recipient_class, sender,
                recipient_key, time, message_type, ujson.dumps(payload), int(delivered), flags.dump())
        except DatabaseError as e:
            raise MessageError("Failed to add message: " + e.args[1])
        else:
            raise Return(message_id)

    @coroutine
    def get_message(self, gamespace, message_id):
        try:
            message = yield self.db.get(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_id`=%s AND `gamespace_id`=%s;
                """, message_id, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to get a message: " + e.args[1])

        if not message:
            raise MessageNotFound()

        raise Return(MessageAdapter(message))

    @coroutine
    def list_incoming_messages(self, gamespace, recipient_class, recipient, limit=100):
        try:
            messages = yield self.db.query(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s
                    ORDER BY `message_time` DESC
                    LIMIT %s;
                """, recipient_class, recipient, gamespace, limit)
        except DatabaseError as e:
            raise MessageError("Failed to list incoming messages: " + e.args[1])

        raise Return(map(MessageAdapter, messages))

    @coroutine
    def read_incoming_messages(self, gamespace, recipient_class, recipient, receiver):
        try:
            with (yield self.db.acquire(auto_commit=False)) as db:
                messages = yield db.query(
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
                    recv = yield receiver(m)
                    if recv:
                        if DeliveryFlags.REMOVE_DELIVERED in m.flags:
                            remove_ids.append(m.message_id)
                        else:
                            mark_delivered_ids.append(m.message_id)

                if mark_delivered_ids:
                    yield db.query(
                        """
                            UPDATE `messages`
                            SET `message_delivered`=1
                            WHERE `gamespace_id`=%s AND `message_id` IN %s;
                        """, gamespace, mark_delivered_ids
                    )

                if remove_ids:
                    yield db.query(
                        """
                            DELETE FROM `messages`
                            WHERE `gamespace_id`=%s AND `message_id` IN %s;
                        """, gamespace, remove_ids
                    )

                yield db.commit()

        except DatabaseError as e:
            raise MessageError("Failed to read incoming messages: " + e.args[1])

    @coroutine
    def delete_messages(self, gamespace, recipient_class, recipient):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, recipient_class, recipient, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to delete messages: " + e.args[1])

    @coroutine
    def delete_messages_like(self, gamespace, recipient_class, recipient_like):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_recipient_class` LIKE %s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, recipient_class, recipient_like, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to delete messages: " + e.args[1])

    @coroutine
    def delete_message(self, gamespace, message_id):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_id`=%s AND `gamespace_id`=%s;
                """, message_id, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to delete a message: " + e.args[1])


class MessageNotFound(Exception):
    pass

