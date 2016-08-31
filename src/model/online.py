
from tornado.gen import coroutine, Return
from common.options import options
from common.model import Model

import common.rabbitconn
import common.aqmp

from conversation import AccountConversation, MessageDelivery


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
