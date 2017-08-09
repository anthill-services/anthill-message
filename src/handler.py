
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

from model.history import MessageQueryError, MessageError, MessageNotFound
from model import MessageSendError, MessageFlags, CLASS_USER

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
                    "gamespace": int(gamespace_id),
                    "time": str(message.time),
                    "type": message.message_type,
                    "payload": message.payload
                }
                for message in reversed(messages)
            ]
        })


class MessageHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self, uuid):
        history = self.application.history
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        try:
            message = yield history.get_message_uuid(gamespace_id, uuid)
        except MessageNotFound:
            raise HTTPError(404, "No such message")
        except MessageError as e:
            raise HTTPError(e.code, e.message)

        self.dumps(message.dump())

    @scoped()
    @coroutine
    def update(self, uuid):

        try:
            payload = ujson.loads(self.get_argument("payload"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Payload is corrupted")

        history = self.application.history
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        try:
            yield history.update_message_concurrent(gamespace_id, account_id, uuid, payload)
        except MessageNotFound:
            raise HTTPError(404, "No such message")
        except MessageError as e:
            raise HTTPError(e.code, e.message)

    @scoped()
    @coroutine
    def delete(self, uuid):

        history = self.application.history
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        try:
            yield history.delete_message_concurrent(gamespace_id, account_id, uuid)
        except MessageNotFound:
            raise HTTPError(404, "No such message")
        except MessageError as e:
            raise HTTPError(e.code, e.message)


class ReadMessagesHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):
        history = self.application.history

        limit = common.to_int(self.get_argument("limit", 100))
        offset = common.to_int(self.get_argument("offset", 0))

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        with (yield history.db.acquire()) as db:
            try:
                messages, count = yield history.list_messages_account_with_count_db(
                    gamespace_id, account_id, db=db, limit=limit, offset=offset)
            except MessageError as e:
                raise HTTPError(e.code, "Account is not joined in that group")

            read_messages = yield history.list_read_messages(gamespace_id, account_id)

            self.dumps({
                "reply_to": {
                    "recipient_class": CLASS_USER,
                    "recipient": str(account_id),
                },
                "last_read_messages": [
                    read_message.dump()
                    for read_message in read_messages
                ],
                "total_count": count,
                "messages": [
                    {
                        "uuid": message.message_uuid,
                        "recipient_class": message.recipient_class,
                        "sender": message.sender,
                        "recipient": message.recipient,
                        "gamespace": int(gamespace_id),
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
            raise HTTPError(e.code, e.message)
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
        self.authoritative = False

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

        self.conversation.set_on_message(self._message)
        self.conversation.set_on_deleted(self._deleted)
        self.conversation.set_on_updated(self._updated)

        self.authoritative = self.token.has_scope("message_authoritative")

        yield self.conversation.init()

        logging.debug("Exchange has been opened!")

    @coroutine
    def _message(self, gamespace_id, message_id, sender, recipient_class,
                 recipient_key, message_type, payload, time, flags):

        try:
            yield self.send_rpc(
                self,
                "message",
                gamespace_id=gamespace_id,
                message_id=message_id,
                sender=sender,
                recipient_class=recipient_class,
                recipient_key=recipient_key,
                message_type=message_type,
                payload=payload,
                time=str(time),
                flags=flags)
        except JsonRPCError as e:
            raise Return(False)

        raise Return(True)

    @coroutine
    def _deleted(self, gamespace_id, message_id, sender):

        try:
            yield self.send_rpc(
                self,
                "message_deleted",
                gamespace_id=gamespace_id,
                sender=sender,
                message_id=message_id)
        except JsonRPCError as e:
            raise Return(False)

        raise Return(True)

    @coroutine
    def _updated(self, gamespace_id, message_id, sender, payload):

        try:
            yield self.send_rpc(
                self,
                "message_updated",
                gamespace_id=gamespace_id,
                sender=sender,
                message_id=message_id,
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
            MessageFlags(flags),
            authoritative=self.authoritative)

    @coroutine
    @validate(message_id="str")
    def delete_message(self, message_id):

        sender = str(self.token.account)
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        history = self.application.history

        try:
            result = yield history.delete_message_concurrent(
                gamespace_id,
                sender,
                message_id)
        except MessageNotFound:
            raise JsonRPCError(404, "No such message")
        except MessageError as e:
            raise JsonRPCError(e.code, e.message)

        raise Return(result)

    @coroutine
    @validate(message_id="str")
    def mark_as_read(self, message_id):

        account_id = str(self.token.account)
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        history = self.application.history

        try:
            result = yield history.mark_message_as_read(
                gamespace_id,
                account_id,
                message_id)
        except MessageNotFound:
            raise JsonRPCError(404, "No such message.")
        except MessageError as e:
            raise JsonRPCError(e.code, e.message)

        raise Return(result)

    @coroutine
    @validate(message_id="str", payload="json_dict")
    def update_message(self, message_id, payload):

        sender = str(self.token.account)
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        history = self.application.history

        try:
            result = yield history.update_message_concurrent(
                gamespace_id,
                sender,
                message_id,
                payload)
        except MessageNotFound:
            raise JsonRPCError(404, "No such message")
        except MessageError as e:
            raise JsonRPCError(e.code, e.message)

        raise Return(result)

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

        authoritative = self.token.has_scope("message_authoritative")

        try:
            messages = ujson.loads(self.get_argument("messages"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted messages")

        try:
            yield message_queue.add_messages(gamespace_id, self.token.account, messages,
                                             authoritative=authoritative)
        except MessageSendError as e:
            raise HTTPError(e.code, "Failed to deliver a message: " + e.message)


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

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield message_queue.add_message(
                gamespace_id, self.token.account, recipient_class, recipient_key, message_type, payload,
                MessageFlags(message_flags),
                authoritative=authoritative)

        except MessageSendError as e:
            raise HTTPError(e.message, "Failed to deliver a message: " + e.message)


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
            raise InternalError(e.code, e.message)

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

                raise InternalError(e.code, e.message)
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
              notify="json_dict", authoritative="bool")
    def join_group(self, gamespace, group_class, group_key, account_id, role="member",
                   notify=None, authoritative=False):
        groups = self.application.groups

        try:
            group = yield groups.find_group(gamespace, group_class, group_key)
        except GroupNotFound as e:
            raise InternalError(404, "No such group")
        except GroupError as e:
            raise InternalError(e.code, e.message)

        try:
            participation = yield groups.join_group(gamespace, group, account_id, role,
                                                    notify=notify, authoritative=authoritative)
        except GroupError as e:
            raise InternalError(e.code, e.message)
        except UserAlreadyJoined:
            raise InternalError(409, "User already joined")

        raise Return({
            "participation_id": participation.participation_id,
            "cluster_id": participation.cluster_id,
            "recipient_class": group.group_class,
            "recipient": participation.calculate_recipient()
        })

    @coroutine
    @validate(gamespace="int", group_class="str_name", group_key="str", account_id="int",
              notify="json_dict", authoritative="bool")
    def leave_group(self, gamespace, group_class, group_key, account_id, notify=None, authoritative=False):
        groups = self.application.groups

        try:
            group = yield groups.find_group(gamespace, group_class, group_key)
        except GroupNotFound as e:
            raise InternalError(404, "No such group")
        except GroupError as e:
            raise InternalError(e.code, e.message)

        try:
            yield groups.leave_group(gamespace, group, account_id, notify=notify, authoritative=authoritative)
        except GroupError as e:
            raise InternalError(e.code, e.message)

        raise Return({
            "status": "OK"
        })

    @coroutine
    def send_batch(self, gamespace, sender, messages, authoritative=False):
        message_queue = self.application.message_queue
        logging.info("Delivering batched messages...")

        yield message_queue.add_messages(gamespace, sender, messages, authoritative=authoritative)

    @coroutine
    @validate(gamespace="int", sender="int", recipient_class="str", recipient_key="str",
              message_type="str", payload="json_dict", flags="json_list_of_str_name",
              authoritative="bool")
    def send_message(self, gamespace, sender, recipient_class, recipient_key, message_type,
                     payload, flags, authoritative=False):
        message_queue = self.application.message_queue

        yield message_queue.add_message(
            gamespace, sender, recipient_class, recipient_key,
            message_type, payload, MessageFlags(flags),
            authoritative=authoritative)
