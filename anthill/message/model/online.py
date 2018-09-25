
from anthill.common import rabbitconn, aqmp
from anthill.common.options import options
from anthill.common.model import Model

from . import CLASS_USER
from . conversation import AccountConversation
from . group import GroupsModel


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

        self.groups.online = self

        self.connections = rabbitconn.RabbitMQConnectionPool(
            options.message_broker,
            options.message_broker_max_connections,
            connection_name="message.conversations")

    async def release(self):
        for connection in self.connections:
            await connection.close()

    async def conversation(self, gamespace_id, account_id):
        connection = await self.connections.get()

        conversation = AccountConversation(self, gamespace_id, account_id, connection)

        return conversation

    # noinspection PyMethodMayBeStatic
    async def get_account_exchange(self, account_id, channel):
        """
        Returns accounts exchange, if account is online
        :param account_id: account ID
        :param channel: a channel is need to be acquired to get exchange from
        :return: exchange if user is online, None otherwise
        """

        exchange_name = AccountConversation.__id__(
            CLASS_USER, account_id)

        try:
            exchange = await channel.exchange(
                exchange=exchange_name,
                exchange_type='fanout',
                passive=True)
        except aqmp.AMQPExchangeError as e:
            if e.code == 404:
                return None
            raise BindError(e.code, e.message)
        else:
            return exchange

    async def bind_account_to_group(self, account_id, participation):
        connection = await self.connections.get()

        channel = await connection.channel()

        try:
            account_online = await self.get_account_exchange(account_id, channel)

            if not account_online:
                return

            group_exchange_name = AccountConversation.__id__(
                participation.group_class, participation.calculate_recipient())

            group_exchange = await channel.exchange(
                exchange=group_exchange_name,
                exchange_type='fanout',
                auto_delete=True)

            await account_online.bind(exchange=group_exchange)
        finally:
            await channel.close()
