
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError
from tornado.ioloop import IOLoop

from common.access import scoped, AccessToken
from common.handler import AuthenticatedHandler, JsonRPCWSHandler
from common.jsonrpc import JsonRPCError

from model.conversation import MessageSendError

import logging
import common


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


class ConversationEndpointHandler(JsonRPCWSHandler):
    def __init__(self, application, request, **kwargs):
        super(ConversationEndpointHandler, self).__init__(application, request, **kwargs)
        self.conversation = None

    def required_scopes(self):
        return ["message_listen"]

    @coroutine
    def opened(self, *args, **kwargs):
        online = self.application.online

        account_id = common.to_int(self.token.account)
        gamespace = self.token.get(AccessToken.GAMESPACE)

        if not account_id:
            raise HTTPError(400, "Bad account")

        self.conversation = yield online.conversation(gamespace, account_id)
        self.conversation.handle(self._message)
        yield self.conversation.init()

        logging.debug("Exchange has been opened!")

    @coroutine
    def _message(self, gamespace_id, message_id, sender, recipient_class, recipient_key, message_type, payload):
        try:
            yield self.rpc(
                self,
                "message",
                gamespace_id=gamespace_id,
                message_id=message_id,
                sender=sender,
                recipient_class=recipient_class,
                recipient_key=recipient_key,
                message_type=message_type,
                payload=payload)
        except JsonRPCError as e:
            raise Return(False)

        raise Return(True)

    @coroutine
    def send_message(self, recipient_class, recipient_key, sender, message_type, message):
        try:
            payload = ujson.loads(message)
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted message")

        yield self.conversation.send_message(
            recipient_class,
            recipient_key,
            sender,
            message_type,
            payload)

        raise Return("ok")

    @coroutine
    def closed(self):
        if not self.conversation:
            return

        yield self.conversation.release()
        self.conversation = None


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
