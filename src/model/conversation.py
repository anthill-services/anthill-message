
import ujson
import logging
import datetime
import uuid

from tornado.gen import coroutine, Return, Future
from group import GroupsModel
from . import CLASS_USER, CLASS_GROUP, DeliveryFlags

from pika import BasicProperties


class ProcessError(Exception):
    def __init__(self, message):
        self.message = message


class AccountConversation(object):

    GAMESPACE = "gsps"
    MESSAGE_UUID = "msgu"
    SENDER = "sndr"
    RECIPIENT_CLASS = "class"
    RECIPIENT_KEY = "key"
    TYPE = "type"
    PAYLOAD = "payload"
    FLAGS = "fl"

    EXCHANGE_PREFIX = "conv"

    MAX_EXCHANGES = 255

    """
    A class represents a single communication point for an account.
    """
    def __init__(self, online, gamespace_id, account_id, connection):
        self.online = online

        self.gamespace_id = gamespace_id
        self.account_id = str(account_id)
        self.connection = connection

        self.receive_channel = None
        self.receive_exchange = None
        self.receive_queue = None
        self.receive_consumer = None

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

        participants = yield groups.list_participants_by_account(self.gamespace_id, self.account_id)
        for participant in participants:
            exchange_name = AccountConversation.__id__(CLASS_GROUP, GroupsModel.calculate_recipient(participant))
            group_exchange = yield self.receive_channel.exchange(
                exchange=exchange_name,
                exchange_type='fanout',
                auto_delete=True)

            yield self.receive_exchange.bind(exchange=group_exchange)

        def receiver(m):
            return self.handler(
                self.gamespace_id,
                m.message_uuid,
                m.sender,
                m.recipient_class,
                m.recipient,
                m.message_type,
                m.payload)

        yield history.read_incoming_messages(
            self.gamespace_id, CLASS_USER, self.account_id, receiver)

        self.receive_consumer = yield self.receive_queue.consume(self.__on_message__)

        logging.info("Conversation for account {0} started.".format(self.account_id))

    def handle(self, message_callback):
        self.handler = message_callback

    # noinspection PyBroadException
    @coroutine
    def release(self):

        if self.receive_consumer:
            try:
                yield self.receive_consumer.cancel()
            except Exception:
                logging.exception("Failed to cancel the consumer")

        if self.receive_queue:
            try:
                yield self.receive_queue.delete()
            except Exception:
                logging.exception("Failed to delete the queue")

        if self.receive_channel:
            try:
                yield self.receive_channel.close()
            except Exception:
                logging.exception("Failed to close the channel")

        self.connection = None

        self.receive_channel = None
        self.receive_exchange = None
        self.receive_queue = None
        self.receive_consumer = None

        logging.info("Conversation for account {0} released.".format(self.account_id))

    @coroutine
    def __process__(self, channel, method, properties, body):
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

        if str(gamespace_id) != str(self.gamespace_id):
            raise ProcessError("Bad gamespace")

        if self.handler:
            # try to process the message by a listener
            # noinspection PyBroadException
            try:
                result = yield self.handler(
                    gamespace_id, message_uuid, sender, recipient_class, recipient_key, message_type, payload)
            except Exception:
                logging.exception("Failed to handle the message")
                result = False

            raise Return(result)

        raise Return(False)

    def __del__(self):
        logging.info("Conversation released!")

    @staticmethod
    def __id__(clazz, key):
        return AccountConversation.EXCHANGE_PREFIX + "." + str(clazz) + "." + str(key)

    @coroutine
    def __on_message__(self, channel, method, properties, body):
        try:
            delivered = yield self.__process__(channel, method, properties, body)
        except ProcessError as e:
            logging.error("Failed to process incoming message: " + e.message)
            delivered = False

        channel.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=BasicProperties(correlation_id=properties.correlation_id),
            body='true' if delivered else 'false')
