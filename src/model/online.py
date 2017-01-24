
from tornado.gen import coroutine, Return
from common.options import options
from common.model import Model

import common.rabbitconn
import common.aqmp

from conversation import AccountConversation, MessageDelivery
from . import CLASS_GROUP, CLASS_USER
from group import GroupsModel


class BindError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class OnlineModel(Model):
    def __init__(self, groups, history):
        self.groups = groups
        self.history = history

        self.connections = common.rabbitconn.RabbitMQConnectionPool(
            options.message_broker,
            options.message_broker_max_connections)

    @coroutine
    def release(self):
        for connection in self.connections:
            yield connection.close()

    @coroutine
    def conversation(self, gamespace_id, account_id):
        connection = yield self.connections.get()

        conversation = AccountConversation(self, gamespace_id, account_id, connection)

        raise Return(conversation)

    @coroutine
    def delivery(self, gamespace_id):
        connection = yield self.connections.get()

        delivery = MessageDelivery(self, gamespace_id, connection)
        yield delivery.init()

        raise Return(delivery)

    @coroutine
    def get_account_exchange(self, account_id, channel):
        """
        Returns accounts exchange, if account is online
        :param account_id: account ID
        :param channel: a channel is need to be acquired to get exchange from
        :return: exchange if user is online, None otherwise
        """

        exchange_name = AccountConversation.__id__(
            CLASS_USER, account_id)

        try:
            exchange = yield channel.exchange(
                exchange=exchange_name,
                exchange_type='fanout',
                passive=True)
        except common.aqmp.AMQPExchangeError as e:
            if e.code == 404:
                raise Return(None)
            raise BindError(e.code, e.message)
        else:
            raise Return(exchange)

    @coroutine
    def bind_account_to_group(self, account_id, participation):
        connection = yield self.connections.get()

        channel = yield connection.channel()

        try:
            account_online = yield self.get_account_exchange(account_id, channel)

            if not account_online:
                return

            group_exchange_name = AccountConversation.__id__(
                CLASS_GROUP, GroupsModel.calculate_recipient(participation))

            group_exchange = yield channel.exchange(
                exchange=group_exchange_name,
                exchange_type='fanout',
                auto_delete=True)

            yield account_online.bind(exchange=group_exchange)
        finally:
            yield channel.close()
