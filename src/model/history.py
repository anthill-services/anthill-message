
from tornado.gen import coroutine, Return
from common.model import Model
from common.database import DatabaseError

import ujson


class MessageAdapter(object):
    def __init__(self, data):
        self.message_id = data.get("message_id")
        self.message_uuid = data.get("message_uuid")
        self.message_recipient_class = data.get("message_recipient_class")
        self.sender = data.get("message_sender")
        self.recipient = data.get("message_recipient")
        self.time = data.get("message_time")
        self.message_type = data.get("message_type")
        self.payload = data.get("message_payload")
        self.delivered = data.get("message_delivered")


class MessagesHistoryModel(Model):

    def __init__(self, db):
        self.db = db

    def get_setup_tables(self):
        return ["messages"]

    def get_setup_db(self):
        return self.db

    @coroutine
    def add_message(self, gamespace, message_uuid, message_recipient_class,
                    sender, recipient, time, message_type, payload, delivered=False):

        if not isinstance(payload, dict):
            raise MessageError("payload should be a dict")

        try:
            message_id = yield self.db.insert(
                """
                    INSERT INTO `messages`
                    (`gamespace_id`, `message_uuid`, `message_recipient_class`, `message_sender`,
                        `message_recipient`, `message_time`, `message_type`, `message_payload`, `message_delivered`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, gamespace, message_uuid, message_recipient_class, sender,
                recipient, time, message_type, ujson.dumps(payload), int(delivered))
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
    def list_incoming_messages(self, gamespace, message_recipient_class, recipient):
        try:
            messages = yield self.db.query(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, message_recipient_class, recipient, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to list incoming messages: " + e.args[1])

        raise Return(map(MessageAdapter, messages))

    @coroutine
    def read_incoming_messages(self, gamespace, message_recipient_class, recipient):
        try:
            with (yield self.db.acquire(auto_commit=False)) as db:
                messages = yield db.query(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE `message_recipient_class`=%s AND `message_recipient`=%s
                            AND `gamespace_id`=%s AND `message_delivered`=0
                        FOR UPDATE;
                    """, message_recipient_class, recipient, gamespace)

                message_ids = [m["message_id"] for m in messages]

                if message_ids:
                    yield db.query(
                        """
                            UPDATE `messages`
                            SET `message_delivered`=1
                            WHERE `gamespace_id`=%s AND `message_id` IN %s;
                        """, gamespace, message_ids
                    )

                yield db.commit()

        except DatabaseError as e:
            raise MessageError("Failed to list incoming messages: " + e.args[1])

        raise Return(map(MessageAdapter, messages))

    @coroutine
    def list_paged_incoming_messages(self, gamespace, message_recipient_class, recipient, items_in_page, page):
        try:
            with (yield self.db.acquire()) as db:
                pages_count = yield db.get("""
                    SELECT COUNT(*) as `count`
                    FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, message_recipient_class, recipient, gamespace)

                import math
                pages = int(math.ceil(float(pages_count["count"]) / float(items_in_page)))

                limit_a = (page - 1) * items_in_page
                limit_b = page * items_in_page

                messages = yield db.query(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s
                        ORDER BY `message_time` ASC
                        LIMIT %s, %s;
                    """, message_recipient_class, recipient, gamespace, limit_a, limit_b)
        except DatabaseError as e:
            raise MessageError("Failed to list incoming messages: " + e.args[1])

        result = map(MessageAdapter, messages), pages

        raise Return(result)

    @coroutine
    def list_outgoing_messages(self, gamespace, message_recipient_class, sender):
        try:
            messages = yield self.db.query(
                """
                    SELECT *
                    FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_sender`=%s AND `gamespace_id`=%s;
                """, message_recipient_class, sender, gamespace)
        except DatabaseError as e:
            raise MessageError("Failed to list outgoing messages: " + e.args[1])

        raise Return(map(MessageAdapter, messages))

    @coroutine
    def delete_messages(self, gamespace, message_recipient_class, recipient):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `messages`
                    WHERE `message_recipient_class`=%s AND `message_recipient`=%s AND `gamespace_id`=%s;
                """, message_recipient_class, recipient, gamespace)
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


class MessageError(Exception):
    def __init__(self, message):
        self.message = message
