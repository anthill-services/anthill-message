
import common.admin as a
from common.internal import Internal, InternalError
from common import to_int

from common.access import AccessToken
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop

from model.group import GroupError, GroupNotFound, GroupExistsError, UserAlreadyJoined, GroupParticipantNotFound
from model.history import MessageError
from model import CLASS_USER, CLASS_GROUP

import logging
import ujson
import common
import math


class IndexController(a.AdminController):
    def render(self, data):
        return [
            a.links("Message service", [
                a.link("users", "Edit user conversations", icon="user"),
                a.link("groups", "Edit groups", icon="users"),
                a.link("history", "Message history", icon="history")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]


class UsersController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([], "Edit user conversations"),
            a.split([
                a.form(title="Find by credential", fields={
                    "credential": a.field("User credential", "text", "primary", "non-empty"),
                }, methods={
                    "search_credential": a.method("Search", "primary")
                }, data=data),
                a.form(title="Find by account number", fields={
                    "account": a.field("Account number", "text", "primary", "number")
                }, methods={
                    "search_account": a.method("Search", "primary")
                }, data=data)
            ]),
            a.links("Navigate", [
                a.link("index", "Go back")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def search_account(self, account):
        raise a.Redirect("user", account=account)

    @coroutine
    def search_credential(self, credential):
        internal = Internal()

        try:
            account = yield internal.request(
                "login",
                "get_account",
                credential=credential)

        except InternalError as e:
            if e.code == 400:
                raise a.ActionError("Failed to find credential: bad username")
            if e.code == 404:
                raise a.ActionError("Failed to find credential: no such user")

            raise a.ActionError(e.body)

        raise a.Redirect("user", account=account["id"])


class UserController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("users", "Conversations")
            ], "User @" + self.context.get("account")),
            a.links("Groups user participate in", links=[
                a.link("group", "@" + str(group.group_id), icon="users", group_id=group.group_id)
                for group in data["user_groups"]
            ]),
            a.links("Navigate", [
                a.link("users", "Go back"),
                a.link("add_user_participation", "Join a Group", icon="plus", account=self.context.get("account")),
                a.link("messages", "Read / Write messages", icon="pencil", account=self.context.get("account"))
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def get(self, account):

        groups = self.application.groups

        try:
            user_groups = yield groups.list_groups_account_participates(self.gamespace, account)
        except GroupError as e:
            raise a.ActionError("Failed to get user conversations: " + e.message)

        raise Return({
            "user_groups": user_groups
        })


class GroupsController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([], "Groups"),
            a.split([
                a.form(title="Find by a class", fields={
                    "group_class": a.field("Group class", "text", "primary", "non-empty", order=1),
                    "group_key": a.field("Group key (optional)", "text", "primary", order=2),
                }, methods={
                    "search_class": a.method("Search by class", "primary")
                }, data=data),
                a.form(title="Find by ID", fields={
                    "group_id": a.field("Group ID", "text", "primary", "number"),
                }, methods={
                    "search_id": a.method("Search by ID", "primary")
                }, data=data)
            ]),
            a.links("Navigate", [
                a.link("users", "Go back"),
                a.link("new_group", "Add a group", icon="plus")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def search_id(self, group_id):

        groups = self.application.groups

        try:
            group = yield groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")
        except GroupError as e:
            raise a.ActionError("Failed to find a group:" + e.message)

        raise a.Redirect("group", group_id=group.group_id)

    @coroutine
    def search_class(self, group_class, group_key=None):

        if not group_key:
            raise a.Redirect("groups_by_class", group_class=group_class)

        groups = self.application.groups

        try:
            group = yield groups.find_group(self.gamespace, group_class, group_key)
        except GroupNotFound:
            raise a.ActionError("No such group")
        except GroupError as e:
            raise a.ActionError("Failed to find a group:" + e.message)

        raise a.Redirect("group", group_id=group.group_id)


class FindGroupsByClassController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("groups", "Groups")
            ], "By class: " + self.context.get("group_class")),
            a.links("Groups By Class", links=[
                a.link("group", group.key, icon="users", group_id=group.group_id)
                for group in data["groups"]
            ]),
            a.links("Navigate", [
                a.link("groups", "Go back"),
                a.link("new_group", "Add a group", icon="plus")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def get(self, group_class):

        groups = self.application.groups

        try:
            groups = yield groups.list_groups(self.gamespace, group_class)
        except GroupError as e:
            raise a.ActionError("Failed to list groups:" + e.message)

        raise Return({
            "groups": groups
        })


class NewGroupController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("groups", "Groups")
            ], "New group"),
            a.form(title="Create a new group", fields={
                "group_class": a.field("Group class", "text", "primary", "non-empty", order=1),
                "group_key": a.field("Group key", "text", "primary", "non-empty", order=2),
                "store_messages": a.field("Store messages", "switch", "primary", "non-empty", order=3),
                "clustered": a.field("Clustered (cannot change later)", "switch", "primary", "non-empty", order=4),
                "cluster_size": a.field("Cluster Size", "text", "primary", "number", order=5),
            }, methods={
                "create": a.method("Create", "primary")
            }, data=data),
            a.links("Navigate", [
                a.link("groups", "Go back")
            ])
        ]

    @coroutine
    def get(self, **context):
        raise Return({
            "store_messages": "true",
            "clustered": "false",
            "cluster_size": 1000
        })

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def create(self, group_class, group_key, store_messages="false", clustered="false", cluster_size=1000):
        groups = self.application.groups

        try:
            group_id = yield groups.add_group(
                self.gamespace,
                group_class,
                group_key,
                store_messages == "true",
                clustered == "true",
                cluster_size)
        except GroupExistsError:
            raise a.ActionError("Such group already exists")
        except GroupError as e:
            raise a.ActionError("Failed to create a group:" + e.message)

        raise a.Redirect(
            "group",
            message="A new group has been created",
            group_id=group_id)


class AddGroupParticipantController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("groups", "Groups"),
                a.link("group", "@" + str(self.context.get("group_id")), group_id=self.context.get("group_id")),
                a.link(None, "Participants")
            ], "New"),
            a.form(title="Create a new group participant", fields={
                "account": a.field("Account", "text", "primary", "non-empty", order=1),
                "role": a.field("Role", "text", "primary", "non-empty", order=2),
            }, methods={
                "create": a.method("Create", "primary")
            }, data=data),
            a.links("Navigate", [
                a.link("groups", "Go back")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def create(self, account, role):
        groups = self.application.groups

        group_id = self.context.get("group_id")

        try:
            group = yield groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            participation = yield groups.join_group(self.gamespace, group, account, role)
        except UserAlreadyJoined:
            raise a.ActionError("Such user is already in a group")
        except GroupError as e:
            raise a.ActionError("Failed to join a group:" + e.message)

        raise a.Redirect(
            "group",
            message="User has been joined to the group",
            group_id=group_id)


class AddUserParticipantController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("users", "Users"),
                a.link("user", "@" + str(self.context.get("account")), account=self.context.get("account")),
            ], "Participate in a group"),
            a.form(title="Create a group participation", fields={
                "group_id": a.field("Group ID", "text", "primary", "non-empty", order=1),
                "role": a.field("Role", "text", "primary", "non-empty", order=2),
            }, methods={
                "create": a.method("Create", "primary")
            }, data=data),
            a.links("Navigate", [
                a.link("groups", "Go back")
            ])
        ]

    @coroutine
    def get(self, account):
        raise Return({})

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def create(self, group_id, role):
        groups = self.application.groups

        account = self.context.get("account")

        try:
            group = yield groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            participation = yield groups.join_group(self.gamespace, group, account, role)
        except UserAlreadyJoined:
            raise a.ActionError("Such user is already in a group")
        except GroupError as e:
            raise a.ActionError("Failed to join a group:" + e.message)

        raise a.Redirect(
            "user",
            message="User has been joined to the group",
            account=account)


class GroupParticipantController(a.AdminController):
    def render(self, data):
        participation = data["participation"]

        return [
            a.breadcrumbs([
                a.link("groups", "Groups"),
                a.link("group", "@" + str(participation.group_id), group_id=participation.group_id),
                a.link(None, "Participants")
            ], "User @" + str(participation.account)),
            a.form(title="Group participant", fields={
                "account": a.field("Account", "readonly", "primary", order=1),
                "role": a.field("Role", "text", "primary", "non-empty", order=2),
            }, methods={
                "update": a.method("Update", "primary", order=1),
                "leave": a.method("Leave a group", "primary", order=2)
            }, data=data),
            a.links("Navigate", [
                a.link("group", "Go back", group_id=participation.group_id)
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def get(self, participation_id):
        groups = self.application.groups

        try:
            participation = yield groups.get_group_participation(self.gamespace, participation_id)
        except GroupParticipantNotFound:
            raise a.ActionError("No such participation")

        raise Return({
            "participation": participation,
            "account": participation.account,
            "role": participation.role
        })

    @coroutine
    def update(self, role, **ignored):
        groups = self.application.groups
        participation_id = self.context.get("participation_id")

        try:
            yield groups.updated_group_participation(self.gamespace, participation_id, role)
        except GroupError as e:
            raise a.ActionError("Failed to update a group participation:" + e.message)

        raise a.Redirect(
            "group_participation",
            message="A group participation has been updated",
            participation_id=participation_id)

    @coroutine
    def leave(self, **ignored):
        groups = self.application.groups
        participation_id = self.context.get("participation_id")

        try:
            participation = yield groups.get_group_participation(self.gamespace, participation_id)
        except GroupParticipantNotFound:
            raise a.ActionError("No such participation")

        try:
            yield groups.leave_group(self.gamespace, participation.group_id, participation.account)
        except GroupError as e:
            raise a.ActionError("Failed to leave a group:" + e.message)

        raise a.Redirect(
            "group",
            message="A user has been removed from a group",
            group_id=participation.group_id)


class GroupController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("groups", "Groups"),
            ], "@" + str(self.context.get("group_id"))),
            a.form(title="Group", fields={
                "group_class": a.field("Group class", "text", "primary", "non-empty", order=1),
                "group_key": a.field("Group key", "text", "primary", "non-empty", order=2),
                "store_messages": a.field("Store messages", "switch", "primary", "non-empty", order=3),
                "clustered": a.field("Clustered", "switch", "primary", "non-empty", order=4, readonly=True),
                "cluster_size": a.field("Cluster Size", "text", "primary", "number", order=5),
            }, methods={
                "update": a.method("Update", "primary"),
                "delete": a.method("Delete", "danger")
            }, data=data),
            a.links("Group participants", links=[
                a.link("group_participation", "@" + str(user.account), icon="user", badge=user.role,
                       participation_id=user.participation_id)
                for user in data["participants"]
            ] + [
                a.link("add_group_participation", "New participant", icon="plus", group_id=self.context.get("group_id"))
            ]),
            a.links("Navigate", [
                a.link("groups", "Go back"),
                a.link("history", "See messages in the group", icon="history",
                       message_recipient_class="group",
                       message_recipient=str(self.context.get("group_id")) + "-%"),
                a.link("groups_by_class", "See groups by class: " + data["group_class"],
                       group_class=data["group_class"])
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def get(self, group_id):
        groups = self.application.groups

        try:
            group = yield groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")
        except GroupError as e:
            raise a.ActionError(e.message)

        try:
            participants = yield groups.list_group_participants(self.gamespace, group_id)
        except GroupError as e:
            raise a.ActionError(e.message)

        raise Return({
            "group_class": group.group_class,
            "group_key": group.key,
            "store_messages": "true" if group.store_messages else "false",
            "clustered": "true" if group.clustered else "false",
            "cluster_size": group.cluster_size,
            "participants": participants
        })

    @coroutine
    def update(self, group_class, group_key, store_messages="false", cluster_size=1000, **ignored):
        groups = self.application.groups
        group_id = self.context.get("group_id")

        try:
            yield groups.update_group(
                self.gamespace,
                group_id,
                group_class,
                group_key,
                store_messages == "true",
                cluster_size)
        except GroupError as e:
            raise a.ActionError("Failed to update a group:" + e.message)

        raise a.Redirect(
            "group",
            message="A group has been updated",
            group_id=group_id)

    @coroutine
    def delete(self, **ignored):
        groups = self.application.groups
        group_id = self.context.get("group_id")

        try:
            yield groups.delete_group(self.gamespace, group_id)
        except GroupError as e:
            raise a.ActionError("Failed to delete a group:" + e.message)

        raise a.Redirect(
            "groups",
            message="A group has been deleted")


class MessagesController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("users", "Messages")
            ], "User @" + self.context.get("account")),
            a.script("static/admin/messages.js",
                     account=self.context.get("account")),
            a.links("Navigate", [
                a.link("users", "Go back"),
                a.link("history", "Incoming Messages History", icon="download",
                       message_recipient_class="user", message_recipient=self.context.get("account")),
                a.link("history", "Outgoing Messages History", icon="upload",
                       message_sender=self.context.get("account")),
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]


class MessagesStreamController(a.StreamAdminController):
    def __init__(self, app, token, handler):
        super(MessagesStreamController, self).__init__(app, token, handler)

        self.conversation = None

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def prepared(self, account):
        online = self.application.online
        account_id = common.to_int(account)

        if not account_id:
            raise a.ActionError("Bad account")

        self.conversation = yield online.conversation(self.gamespace, account_id)
        self.conversation.handle(self._message)
        self.conversation.init()

        logging.debug("Exchange has been opened!")

    @coroutine
    def _message(self, gamespace_id, message_id, sender, recipient_class, recipient_key, message_type, payload):
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

    @coroutine
    def send_message(self, recipient_class, recipient_key, sender, message_type, message):
        try:
            payload = ujson.loads(message)
        except (KeyError, ValueError):
            raise a.StreamCommandError(400, "Corrupted message")

        yield self.conversation.send_message(recipient_class, recipient_key, sender, message_type, payload)

        raise Return("ok")

    @coroutine
    def opened(self, **kwargs):
        pass # yield self.rpc(self, "servers", result)

    def on_close(self):
        IOLoop.current().add_callback(self.conversation.release)
        self.conversation = None


class MessagesHistoryController(a.AdminController):

    MESSAGES_PER_PAGE = 20

    def render(self, data):
        messages = [
            {
                "sender": message.sender,
                "recipient": str(message.recipient_class) + " " + str(message.recipient),
                "time": str(message.time),
                "delivered": "yes" if message.delivered else "no",
                "message_type": message.message_type,
                "payload": [a.json_view(message.payload)],
                "id": [
                    a.link("message", message.message_id, icon="envelope-o", message_id=message.message_id)
                ]
            }
            for message in data["messages"]
        ]

        return [
            a.breadcrumbs([], "History"),
            a.content("Messages", [
                {
                    "id": "id",
                    "title": "ID"
                }, {
                    "id": "sender",
                    "title": "From"
                }, {
                    "id": "recipient",
                    "title": "Recipient"
                }, {
                    "id": "time",
                    "title": "Time"
                }, {
                    "id": "delivered",
                    "title": "Delivered"
                }, {
                    "id": "message_type",
                    "title": "Type"
                }, {
                    "id": "payload",
                    "title": "Payload",
                    "width": "40%"
                }], messages, "default", empty="No messages to display. At least one filter is required."),
            a.pages(data["pages"]),
            a.form("Filters", fields={
                "message_sender":
                    a.field("Message Sender", "text", "primary", order=1),
                "message_recipient_class":
                    a.field("Message Recipient Class", "text", "primary", order=2),
                "message_recipient":
                    a.field("Message Recipient", "text", "primary", order=3),
                "message_type":
                    a.field("Message Type", "text", "primary", order=4),
                "message_delivered":
                    a.field("Message Delivered", "select", "primary", values=data["message_delivered_values"], order=5)
            }, methods={
                "filter": a.method("Filter", "primary")
            }, data=data, icon="filter"),
            a.links("Navigate", [
                a.link("index", "Go back")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @coroutine
    def filter(self, **args):

        page = self.context.get("page", 1)

        filters = {
            "page": page
        }

        filters.update({
            k: v for k, v in args.iteritems() if v
        })

        raise a.Redirect("history", **filters)

    @coroutine
    def get(self,
            page=1,
            message_sender=None,
            message_recipient_class=None,
            message_recipient=None,
            message_type=None,
            message_delivered=None):

        page = to_int(page)

        if message_sender or message_recipient_class or message_recipient or message_type or message_delivered:

            history = self.application.history

            q = history.messages_query(self.gamespace)

            q.offset = (page-1) * MessagesHistoryController.MESSAGES_PER_PAGE
            q.limit = MessagesHistoryController.MESSAGES_PER_PAGE
            q.message_sender = message_sender
            q.message_recipient_class = message_recipient_class
            q.message_recipient = message_recipient
            q.message_type = message_type

            if message_delivered:
                q.message_delivered = message_delivered == "yes"

            messages, count = yield q.query(count=True)
            pages = int(math.ceil(float(count) / float(MessagesHistoryController.MESSAGES_PER_PAGE)))
        else:
            messages, pages = [], 0

        raise Return({
            "messages": messages,
            "pages": pages,
            "message_sender": message_sender,
            "message_recipient_class": message_recipient_class,
            "message_recipient": message_recipient,
            "message_type": message_type,
            "message_delivered": message_delivered,
            "message_delivered_values": {
                "": "Choose",
                "yes": "Yes",
                "no": "No"
            }
        })
