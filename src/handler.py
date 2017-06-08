
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError
from tornado.ioloop import IOLoop

from common.access import scoped, AccessToken
from common.handler import AuthenticatedHandler, JsonRPCWSHandler
from common.jsonrpc import JsonRPCError
from common.internal import InternalError
from common.validate import validate

from model.group import GroupParticipantNotFound, GroupNotFound, GroupsModel, GroupError, UserAlreadyJoined, \
    GroupAdapter

from model.history import MessageQueryError
from model import MessageSendError, DeliveryFlags

import logging
import common


class ReadGroupInboxHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self, group_class, group_key):
        groups = self.application.groups
        history = self.application.history

        limit = common.to_int(self.get_argument("limit", 100))

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        try:
            group = yield groups.find_group_with_participation(
                gamespace_id, group_class, group_key, account_id)
        except GroupParticipantNotFound:
            raise HTTPError(406, "Account is not joined in that group")
        except GroupNotFound:
            raise HTTPError(404, "No such group")

        message_recipient_class = group.group_class
        message_recipient = group.calculate_recipient()
        message_type = self.get_argument("type", None)

        q = history.messages_query(gamespace_id)

        q.message_recipient_class = message_recipient_class
        q.message_recipient = message_recipient

        if message_type:
            q.message_type = message_type

        q.limit = limit

        try:
            messages, count = yield q.query(count=True)
        except MessageQueryError as e:
            raise HTTPError(500, e.message)

        self.dumps({
            "reply_to": {
                "recipient_class": message_recipient_class,
                "recipient": message_recipient,
            },
            "total_count": count,
            "messages": [
                {
                    "uuid": message.message_uuid,
                    "recipient_class": message.recipient_class,
                    "sender": message.sender,
                    "recipient": message.recipient,
                    "time": str(message.time),
                    "type": message.message_type,
                    "payload": message.payload
                }
                for message in reversed(messages)
            ]
        })


class JoinGroupHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, group_class, group_key):
        groups = self.application.groups

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        try:
            group = yield groups.find_group(gamespace_id, group_class, group_key)
        except GroupNotFound:
            raise HTTPError(404, "No such group")

        role = self.get_argument("role")

        try:
            participation = yield groups.join_group(gamespace_id, group, account_id, role)
        except GroupError as e:
            raise HTTPError(400, e.message)
        except UserAlreadyJoined:
            raise HTTPError(409, "User already joined")

        message_recipient_class = group.group_class
        message_recipient = participation.calculate_recipient()

        self.dumps({
            "reply_to": {
                "recipient_class": message_recipient_class,
                "recipient": message_recipient,
            }
        })


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

    @validate(recipient_class="str", recipient_key="str", message_type="str", message="json_dict",
              flags="json_list_of_strings")
    def send_message(self, recipient_class, recipient_key, message_type, message, flags):
        
        sender = str(self.token.account)
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        message_queue = self.application.message_queue

        return message_queue.add_message(
            gamespace_id,
            sender,
            recipient_class,
            recipient_key,
            message_type,
            message,
            DeliveryFlags(flags))

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
        message_queue = self.application.message_queue

        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        try:
            messages = ujson.loads(self.get_argument("messages"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted messages")

        try:
            yield message_queue.add_messages(gamespace_id, self.token.account, messages)
        except MessageSendError as e:
            raise HTTPError(400, "Failed to deliver a message: " + e.message)


class SendMessageHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, recipient_class, recipient_key):
        message_queue = self.application.message_queue
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        message_type = self.get_argument("message_type")

        try:
            message_flags = ujson.loads(self.get_argument("message_flags", "[]"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted Flags")

        try:
            payload = ujson.loads(self.get_argument("payload"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted payload")

        try:
            yield message_queue.add_message(
                gamespace_id, self.token.account, recipient_class, recipient_key, message_type, payload,
                DeliveryFlags(message_flags))

        except MessageSendError as e:
            raise HTTPError(400, "Failed to deliver a message: " + e.message)


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @coroutine
    @validate(gamespace="int", group_class="str_name", group_key="str", clustered="bool", cluster_size="int",
              join_account_id="int", join_role="str_name")
    def create_group(self, gamespace, group_class, group_key, clustered=False, cluster_size=1000,
                     join_account_id=None, join_role=None):
        groups = self.application.groups

        try:
            group_id = yield groups.new_group(gamespace, group_class, group_key, clustered, cluster_size)
        except GroupError as e:
            raise InternalError(500, e.message)

        if join_account_id and join_role:
            group = GroupAdapter({
                "group_class": group_class,
                "group_key": group_key,
                "group_id": group_id,
                "group_clustered": clustered,
                "group_cluster_size": cluster_size,
            })

            try:
                participation = yield groups.join_group(gamespace, group, join_account_id, join_role)
            except GroupError as e:
                try:
                    yield groups.delete_group(gamespace, group)
                except GroupError:
                    pass

                raise InternalError(500, e.message)
            else:
                raise Return({
                    "participation_id": participation.participation_id,
                    "cluster_id": participation.cluster_id,
                    "id": group_id,
                    "recipient_class": group.group_class,
                    "recipient": participation.calculate_recipient()
                })

        raise Return({
            "id": group_id
        })

    @coroutine
    @validate(gamespace="int", group_class="str_name", group_key="str", account_id="int", role="str_name",
              notify="json_dict")
    def join_group(self, gamespace, group_class, group_key, account_id, role="member", notify=None):
        groups = self.application.groups

        try:
            group = yield groups.find_group(gamespace, group_class, group_key)
        except GroupNotFound as e:
            raise InternalError(404, "No such group")
        except GroupError as e:
            raise InternalError(500, e.message)

        try:
            participation = yield groups.join_group(gamespace, group, account_id, role, notify=notify)
        except GroupError as e:
            raise InternalError(500, e.message)
        except UserAlreadyJoined:
            raise InternalError(409, "User already joined")

        raise Return({
            "participation_id": participation.participation_id,
            "cluster_id": participation.cluster_id,
            "recipient_class": group.group_class,
            "recipient": participation.calculate_recipient()
        })

    @coroutine
    @validate(gamespace="int", group_class="str_name", group_key="str", account_id="int", notify="json_dict")
    def leave_group(self, gamespace, group_class, group_key, account_id, notify=None):
        groups = self.application.groups

        try:
            group = yield groups.find_group(gamespace, group_class, group_key)
        except GroupNotFound as e:
            raise InternalError(404, "No such group")
        except GroupError as e:
            raise InternalError(500, e.message)

        try:
            yield groups.leave_group(gamespace, group, account_id, notify=notify)
        except GroupError as e:
            raise InternalError(500, e.message)

        raise Return({
            "status": "OK"
        })

    @coroutine
    def send_batch(self, gamespace, sender, messages):
        message_queue = self.application.message_queue
        logging.info("Delivering batched messages...")

        yield message_queue.add_messages(gamespace, sender, messages)

    @coroutine
    @validate(gamespace="int", sender="int", recipient_class="str", recipient_key="str",
              message_type="str", payload="json_dict", flags="json_list_of_str_name")
    def send_message(self, gamespace, sender, recipient_class, recipient_key, message_type, payload, flags):
        message_queue = self.application.message_queue

        yield message_queue.add_message(
            gamespace, sender, recipient_class, recipient_key,
            message_type, payload, DeliveryFlags(flags))
