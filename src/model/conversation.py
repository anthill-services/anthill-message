
import ujson
import logging
import datetime
import uuid

from tornado.gen import coroutine, Return, Future
from . import CLASS_USER, CLASS_GROUP


class ProcessError(object):
    def __init__(self, message):
        self.message = message


class MessageSendError(object):
    def __init__(self, message):
        self.message = message


class MessageDelivery(object):

    """
    A class represents a delivery of a single message to some recipient.
    """

    def __init__(self, online, gamespace_id, connection):
        self.connection = connection
        self.gamespace_id = gamespace_id
        self.online = online

    @coroutine
    def init(self):
        pass

    @coroutine
    def send_messages(self, sender, messages):
        for message in messages:
            if any(t not in message for t in ["recipient_class", "recipient_key", "message_type", "payload"]):
                raise MessageSendError(
                    "Messages expected to be a list of {'recipient_class', 'recipient_key', `message_type`, 'payload'}")

        yield [self.send_message(m["recipient_class"], m["recipient_key"], sender, m["message_type"], m["payload"]) for m in messages]

    @coroutine
    def send_message(self, recipient_class, recipient_key, sender, message_type, payload):
        """
        Tries to deliver a message to recipient (recipient_class, recipient_key)
        :param recipient_class: a class of recipient (user, group, etc)
        :param recipient_key: a key in such class (account id, group id etc)
        :param sender: account id of the sender
        :param payload: a dict containing the message to be delivered
        :param message_type: a kind of a message (like a subject)
        :returns whenever message was delivered, or not (in that case it's stored in a database)
        """

        if not isinstance(payload, dict):
            raise MessageSendError("Payload expected to be a dict")

        message_uuid = str(uuid.uuid4())

        body = ujson.dumps({
            AccountConversation.GAMESPACE: self.gamespace_id,
            AccountConversation.MESSAGE_UUID: message_uuid,
            AccountConversation.SENDER: sender,
            AccountConversation.RECIPIENT_CLASS: recipient_class,
            AccountConversation.RECIPIENT_KEY: recipient_key,
            AccountConversation.TYPE: message_type,
            AccountConversation.PAYLOAD: payload
        })

        exchange_id = AccountConversation.__id__(recipient_class, recipient_key)

        delivered = False

        channel = yield self.connection.channel()

        try:
            f = Future()

            def delivered_(m):
                import pika
                if f.running():
                    f.set_result(isinstance(m.method, pika.spec.Basic.Ack))

            def closed(ch, reason, param):
                if f.running():
                    f.set_result(False)

            yield channel.confirm_delivery(delivered_)
            yield channel.add_on_close_callback(closed)

            from pika import BasicProperties

            properties = BasicProperties(
                content_type='text/plain',
                delivery_mode=1)

            yield channel.basic_publish(
                exchange_id,
                '',
                body,
                properties=properties,
                mandatory=True)

            delivered = yield f
        finally:
            if channel.is_open:
                yield channel.close()

        logging.info("Message '{0}' {1} been delivered.".format(message_uuid, "has" if delivered else "has not"))

        history = self.online.history

        yield history.add_message(
            self.gamespace_id,
            message_uuid,
            recipient_class,
            sender,
            recipient_key,
            datetime.datetime.utcnow(),
            message_type,
            payload,
            delivered)


class AccountConversation(object):

    GAMESPACE = "gsps"
    MESSAGE_UUID = "msgu"
    SENDER = "sndr"
    RECIPIENT_CLASS = "class"
    RECIPIENT_KEY = "key"
    TYPE = "type"
    PAYLOAD = "payload"

    EXCHANGE_PREFIX = "conv"

    MAX_EXCHANGES = 255

    """
    A class represents a single communication point for an account.
    """
    def __init__(self, online, gamespace_id, account_id, connection):
        self.online = online

        self.gamespace_id = gamespace_id
        self.account_id = account_id
        self.connection = connection

        self.receive_channel = None
        self.receive_exchange = None
        self.receive_queue = None

        self.handler = None

    @coroutine
    def init(self):
        logging.debug("Initializing conversation")

        self.receive_channel = yield self.connection.channel()

        exchange_name = AccountConversation.__id__(CLASS_USER, self.account_id)

        self.receive_exchange = yield self.receive_channel.exchange(
            exchange=exchange_name,
            exchange_type='fanout',
            auto_delete=True)

        self.receive_queue = yield self.receive_channel.queue(exclusive=True, arguments={
            "x-message-ttl": 1000
        })

        yield self.receive_queue.bind(exchange=self.receive_exchange)

        groups = self.online.groups
        history = self.online.history

        account_groups = yield groups.list_group_ids_account_participates(self.gamespace_id, self.account_id)
        for account_group in account_groups:
            exchange_name = AccountConversation.__id__(CLASS_GROUP, account_group)
            group_exchange = yield self.receive_channel.exchange(
                exchange=exchange_name,
                exchange_type='fanout',
                auto_delete=True)

            yield self.receive_exchange.bind(exchange=group_exchange)

        stored_messages = yield history.read_incoming_messages(self.gamespace_id, CLASS_USER, self.account_id)

        if self.handler:
            for message in stored_messages:
                yield self.handler(
                    self.gamespace_id,
                    message.message_uuid,
                    message.sender,
                    message.recipient_class,
                    message.recipient,
                    message.message_type,
                    message.payload)

        yield self.receive_queue.consume(self.__on_message__)

        logging.debug("Conversation initialized!")
        logging.debug("Conversation for account {0} started.".format(self.account_id))

    def handle(self, message_callback):
        self.handler = message_callback

    @coroutine
    def release(self):

        yield self.receive_queue.delete()

        self.connection = None

        self.receive_channel = None
        self.receive_exchange = None
        self.receive_queue = None

        logging.debug("Conversation for account {0} released.".format(self.account_id))

    @coroutine
    def __process__(self, body):
        try:
            payload = ujson.loads(body)
        except (KeyError, ValueError):
            raise ProcessError("Corrupted body")

        try:
            gamespace_id = payload[AccountConversation.GAMESPACE]
            message_uuid = payload[AccountConversation.MESSAGE_UUID]
            sender = payload[AccountConversation.SENDER]
            recipient_class = payload[AccountConversation.RECIPIENT_CLASS]
            recipient_key = payload[AccountConversation.RECIPIENT_KEY]
            message_type = payload[AccountConversation.TYPE]
            payload = payload[AccountConversation.PAYLOAD]
        except KeyError as e:
            raise ProcessError("Missing field: " + e.args[0])

        if gamespace_id != self.gamespace_id:
            raise ProcessError("Bad gamespace")

        if self.handler:
            yield self.handler(gamespace_id, message_uuid, sender, recipient_class,
                               recipient_key, message_type, payload)

    def __del__(self):
        logging.info("Conversation released!")

    @staticmethod
    def __id__(clazz, key):
        return AccountConversation.EXCHANGE_PREFIX + "." + str(clazz) + "." + str(key)

    @coroutine
    def __on_message__(self, channel, method, properties, body):
        try:
            yield self.__process__(body)
        except ProcessError as e:
            logging.error("Failed to process incoming message: " + e.message)

    @coroutine
    def send_message(self, recipient_class, recipient_key, sender, message_type, payload):
        delivery = yield self.online.delivery(self.gamespace_id)

        result = yield delivery.send_message(
            recipient_class,
            recipient_key,
            sender,
            message_type,
            payload)

        raise Return(result)
