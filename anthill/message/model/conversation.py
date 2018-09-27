
from tornado.gen import multi
from tornado.ioloop import IOLoop

from . group import GroupsModel
from . import CLASS_USER, MessageFlags

from pika import BasicProperties
from hashlib import sha1
from base64 import b64encode

import ujson
import logging
import datetime
import uuid
import pytz


class ProcessError(Exception):
    def __init__(self, message):
        self.message = message


class AccountConversation(object):

    ACTION = "a"
    GAMESPACE = "gsps"
    MESSAGE_UUID = "msgu"
    SENDER = "sndr"
    RECIPIENT_CLASS = "class"
    RECIPIENT_KEY = "key"
    TIME = "tm"
    TYPE = "type"
    PAYLOAD = "payload"
    FLAGS = "fl"

    ACTION_NEW_MESSAGE = "m"
    ACTION_MESSAGE_DELETED = "d"
    ACTION_MESSAGE_UPDATED = "u"

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
        self.custom_exchange = None
        self.receive_queue = None
        self.receive_consumer = None

        self.on_message = None
        self.on_deleted = None
        self.on_updated = None

        self.actions = {
            AccountConversation.ACTION_NEW_MESSAGE: self.__action_new_message__,
            AccountConversation.ACTION_MESSAGE_UPDATED: self.__action_message_updated__,
            AccountConversation.ACTION_MESSAGE_DELETED: self.__action_message_deleted__
        }

    async def init(self, message_types=None):
        self.receive_channel = await self.connection.channel()

        exchange_name = AccountConversation.__id__(CLASS_USER, self.account_id)

        self.receive_exchange = await self.receive_channel.exchange(
            exchange=exchange_name,
            exchange_type='fanout',
            auto_delete=True)

        self.receive_queue = await self.receive_channel.queue(exclusive=True, arguments={
            "x-message-ttl": 1000
        })

        if message_types:
            message_types.sort()
            tmp = "".join(message_types)
            custom_exchange_name = 'c.' + str(self.account_id) + "." + str(len(tmp)) + "-" + \
                                   b64encode(sha1(tmp).digest())

            self.custom_exchange = await self.receive_channel.exchange(
                exchange=custom_exchange_name,
                exchange_type='headers',
                auto_delete=True)

            # bind for each message type
            await multi([
                self.custom_exchange.bind(exchange=self.receive_exchange, arguments={
                    AccountConversation.TYPE: message_type
                })
                for message_type in message_types
            ])

            await self.receive_queue.bind(exchange=self.custom_exchange)
        else:
            await self.receive_queue.bind(exchange=self.receive_exchange)

        groups = self.online.groups
        history = self.online.history

        participants = await groups.list_participants_by_account(self.gamespace_id, self.account_id)
        for participant in participants:
            exchange_name = AccountConversation.__id__(participant.group_class, participant.calculate_recipient())
            group_exchange = await self.receive_channel.exchange(
                exchange=exchange_name,
                exchange_type='fanout',
                auto_delete=True)

            await self.receive_exchange.bind(exchange=group_exchange)

        def receiver(m):
            return self.on_message(
                self.gamespace_id,
                m.message_uuid,
                m.sender,
                m.recipient_class,
                m.recipient,
                m.message_type,
                m.payload,
                m.time,
                m.flags.as_list())

        await history.read_incoming_messages(
            self.gamespace_id, CLASS_USER, self.account_id, receiver)

        self.receive_consumer = await self.receive_queue.consume(self.__on_message_sync__)

        logging.info("Conversation for account {0} started.".format(self.account_id))

    def set_on_message(self, callback):
        self.on_message = callback

    def set_on_deleted(self, callback):
        self.on_deleted = callback

    def set_on_updated(self, callback):
        self.on_updated = callback

    # noinspection PyBroadException
    async def release(self):

        if self.receive_queue:
            try:
                await self.receive_queue.delete()
            except Exception:
                logging.exception("Failed to delete the queue")

        if self.receive_channel:
            try:
                self.receive_channel.close()
            except Exception:
                logging.exception("Failed to close the channel")

        self.connection = None

        self.receive_channel = None
        self.receive_exchange = None
        self.custom_exchange = None
        self.receive_queue = None
        self.receive_consumer = None

        logging.info("Conversation for account {0} released.".format(self.account_id))

    def __action_new_message__(self, gamespace_id, message_uuid, sender, message):

        try:
            message_type = message[AccountConversation.TYPE]
            recipient_class = message[AccountConversation.RECIPIENT_CLASS]
            recipient_key = message[AccountConversation.RECIPIENT_KEY]
            payload = message[AccountConversation.PAYLOAD]
            time = message[AccountConversation.TIME]
            flags = message[AccountConversation.FLAGS]
        except KeyError:
            return

        if self.on_message:
            return self.on_message(gamespace_id, message_uuid, sender, recipient_class,
                                   recipient_key, message_type, payload,
                                   datetime.datetime.fromtimestamp(time, tz=pytz.utc),
                                   flags)

    def __action_message_deleted__(self, gamespace_id, message_uuid, sender, message):
        if self.on_deleted:
            return self.on_deleted(gamespace_id, message_uuid, sender)

    def __action_message_updated__(self, gamespace_id, message_uuid, sender, message):

        try:
            payload = message[AccountConversation.PAYLOAD]
        except KeyError:
            return

        if self.on_updated:
            return self.on_updated(gamespace_id, message_uuid, sender, payload)

    async def __process__(self, channel, method, properties, body):
        try:
            message = ujson.loads(body)
        except (KeyError, ValueError):
            raise ProcessError("Corrupted body")

        try:
            action = message[AccountConversation.ACTION]
            gamespace_id = message[AccountConversation.GAMESPACE]
            message_uuid = message[AccountConversation.MESSAGE_UUID]
            sender = message[AccountConversation.SENDER]
        except KeyError as e:
            raise ProcessError("Missing field: " + e.args[0])

        if str(gamespace_id) != str(self.gamespace_id):
            raise ProcessError("Bad gamespace")

        action_method = self.actions.get(action, None)

        if action_method:
            # try to process the message by a listener
            # noinspection PyBroadException
            try:
                result = await action_method(gamespace_id, message_uuid, sender, message)
            except Exception:
                logging.exception("Failed to handle the message")
                result = False

            return result

        return False

    def __del__(self):
        logging.info("Conversation released!")

    @staticmethod
    def __id__(clazz, key):
        return AccountConversation.EXCHANGE_PREFIX + "." + str(clazz) + "." + str(key)

    def __on_message_sync__(self, channel, method, properties, body):
        IOLoop.current().spawn_callback(self.__on_message__, channel, method, properties, body)

    async def __on_message__(self, channel, method, properties, body):
        try:
            delivered = await self.__process__(channel, method, properties, body)
        except ProcessError as e:
            logging.error("Failed to process incoming message: " + e.message)
            delivered = False

        channel.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=BasicProperties(correlation_id=properties.correlation_id),
            body='true' if delivered else 'false')
