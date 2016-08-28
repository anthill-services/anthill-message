
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError

from common.access import scoped, AccessToken
from common.handler import AuthenticatedHandler

from model.conversation import MessageSendError

import logging


class SubscriptionHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self, transport):
        subscriptions = []

        self.dumps(subscriptions)

    @scoped()
    @coroutine
    def post(self, transport):
        pass

    @scoped()
    @coroutine
    def delete(self, transport):
        pass


class InboxHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self, transport):
        inbox = []

        self.dumps(inbox)

    @scoped()
    @coroutine
    def delete(self, transport):
        pass


class SendMessagesHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self):
        online = self.application.online

        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        delivery = yield online.delivery(gamespace_id)

        try:
            messages = ujson.loads(self.get_argument("messages"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted messages")

        try:
            yield delivery.send_messages(self.token.account, messages)
        except MessageSendError as e:
            raise HTTPError(400, "Failed to deliver a message: " + e.message)


class SendMessageHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, recipient_class, recipient_key):
        online = self.application.online

        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        delivery = yield online.delivery(gamespace_id)

        try:
            payload = ujson.loads(self.get_argument("payload"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted payload")

        try:
            yield delivery.send_message(recipient_class, recipient_key, self.token.account, payload)
        except MessageSendError as e:
            raise HTTPError(400, "Failed to deliver a message: " + e.message)


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @coroutine
    def send_batch(self, gamespace, sender, messages):
        online = self.application.online

        logging.info("Delivering batched messages...")

        delivery = yield online.delivery(gamespace)
        delivery.send_messages(sender, messages)
