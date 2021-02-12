
import anthill.common.admin as a
from anthill.common.internal import Internal, InternalError
from anthill.common.validate import validate
from anthill.common.jsonrpc import JsonRPCError
from anthill.common import to_int

from .model.group import GroupError, GroupNotFound, GroupExistsError, UserAlreadyJoined, GroupParticipantNotFound
from .model.history import MessageError, MessageNotFound
from .model import MessageFlags

import logging
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
                a.link("index", "Go back", icon="chevron-left")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    async def search_account(self, account):
        raise a.Redirect("user", account=account)

    # noinspection PyMethodMayBeStatic
    async def search_credential(self, credential):
        internal = Internal()

        try:
            account = await internal.request(
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
                a.link("users", "Go back", icon="chevron-left"),
                a.link("add_user_participation", "Join a Group", icon="plus", account=self.context.get("account")),
                a.link("messages", "Read / Write messages", icon="pencil", account=self.context.get("account")),
                a.link("user_messages", "User Messages History", icon="comments-o",
                       account_id=self.context.get("account")),
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(account="int")
    async def get(self, account):

        groups = self.application.groups

        try:
            user_groups = await groups.list_groups_account_participates(self.gamespace, account)
        except GroupError as e:
            raise a.ActionError("Failed to get user conversations: " + e.message)

        return {
            "user_groups": user_groups
        }


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
                a.link("users", "Go back", icon="chevron-left"),
                a.link("new_group", "Add a group", icon="plus")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(group_id="int")
    async def search_id(self, group_id):

        groups = self.application.groups

        try:
            group = await groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")
        except GroupError as e:
            raise a.ActionError("Failed to find a group:" + e.message)

        raise a.Redirect("group", group_id=group.group_id)

    @validate(group_class="str", group_key="str")
    async def search_class(self, group_class, group_key=None):

        if not group_key:
            raise a.Redirect("groups_by_class", group_class=group_class)

        groups = self.application.groups

        try:
            group = await groups.find_group(self.gamespace, group_class, group_key)
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
                a.link("groups", "Go back", icon="chevron-left"),
                a.link("new_group", "Add a group", icon="plus")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(group_class="str")
    async def get(self, group_class):

        groups = self.application.groups

        try:
            groups = await groups.list_groups(self.gamespace, group_class)
        except GroupError as e:
            raise a.ActionError("Failed to list groups:" + e.message)

        return {
            "groups": groups
        }


class NewGroupController(a.AdminController):
    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("groups", "Groups")
            ], "New group"),
            a.form(title="Create a new group", fields={
                "group_class": a.field("Group class", "text", "primary", "non-empty", order=1),
                "group_key": a.field("Group key", "text", "primary", "non-empty", order=2),
                "clustered": a.field("Clustered (cannot change later)", "switch", "primary", "non-empty", order=4),
                "cluster_size": a.field("Cluster Size", "text", "primary", "number", order=5),
            }, methods={
                "create": a.method("Create", "primary")
            }, data=data),
            a.links("Navigate", [
                a.link("groups", "Go back", icon="chevron-left")
            ])
        ]

    async def get(self, **context):
        return {
            "clustered": "false",
            "cluster_size": 1000
        }

    def access_scopes(self):
        return ["message_admin"]

    @validate(group_class="str", group_key="str", cluster_size="int", clustered="bool")
    async def create(self, group_class, group_key, cluster_size, clustered=False):
        groups = self.application.groups

        try:
            group_id = await groups.new_group(
                self.gamespace,
                group_class,
                group_key,
                clustered,
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
                a.link("groups", "Go back", icon="chevron-left")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(account="int", role="str_name")
    async def create(self, account, role):
        groups = self.application.groups

        group_id = self.context.get("group_id")

        try:
            group = await groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            participation = await groups.join_group(self.gamespace, group, account, role)
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
                a.link("groups", "Go back", icon="chevron-left")
            ])
        ]

    async def get(self, account):
        return {}

    def access_scopes(self):
        return ["message_admin"]

    @validate(group_id="int", role="str_name")
    async def create(self, group_id, role):
        groups = self.application.groups

        account = self.context.get("account")

        try:
            group = await groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            participation = await groups.join_group(self.gamespace, group, account, role)
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
                a.link("group", "Go back", icon="chevron-left", group_id=participation.group_id)
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(participation_id="int")
    async def get(self, participation_id):
        groups = self.application.groups

        try:
            participation = await groups.get_group_participation(self.gamespace, participation_id)
        except GroupParticipantNotFound:
            raise a.ActionError("No such participation")

        return {
            "participation": participation,
            "account": participation.account,
            "role": participation.role
        }

    @validate(role="str_name")
    async def update(self, role, **ignored):
        groups = self.application.groups
        participation_id = self.context.get("participation_id")

        try:
            await groups.updated_group_participation(self.gamespace, participation_id, role)
        except GroupError as e:
            raise a.ActionError("Failed to update a group participation:" + e.message)

        raise a.Redirect(
            "group_participation",
            message="A group participation has been updated",
            participation_id=participation_id)

    async def leave(self, **ignored):
        groups = self.application.groups
        participation_id = self.context.get("participation_id")

        try:
            participation = await groups.get_group_participation(self.gamespace, participation_id)
        except GroupParticipantNotFound:
            raise a.ActionError("No such participation")

        try:
            group = await groups.get_group(self.gamespace, participation.group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            await groups.leave_group(self.gamespace, group, participation.account, authoritative=True)
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
                "clustered": a.field("Clustered", "switch", "primary", "non-empty", order=4, readonly=True),
                "cluster_size": a.field("Cluster Size", "text", "primary", "number", order=5),
            }, methods={
                "update": a.method("Update", "primary"),
                "delete": a.method("Delete", "danger")
            }, data=data),
            a.links("Group participants", links=[
                                                    a.link("group_participation", "@" + str(user.account), icon="user",
                                                           badge=user.role,
                                                           participation_id=user.participation_id)
                                                    for user in data["participants"]
                                                ] + [
                                                    a.link("add_group_participation", "New participant", icon="plus",
                                                           group_id=self.context.get("group_id"))
                                                ]),
            a.links("Navigate", [
                a.link("groups", "Go back", icon="chevron-left"),
                a.link("history", "See messages in the group", icon="history",
                       message_recipient_class="group",
                       message_recipient=str(self.context.get("group_id")) + "-%"),
                a.link("groups_by_class", "See groups by class: " + data["group_class"],
                       icon="filter", group_class=data["group_class"])
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(group_id="int")
    async def get(self, group_id):
        groups = self.application.groups

        try:
            group = await groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")
        except GroupError as e:
            raise a.ActionError(e.message)

        try:
            participants = await groups.list_group_participants(self.gamespace, group_id)
        except GroupError as e:
            raise a.ActionError(e.message)

        return {
            "group_class": group.group_class,
            "group_key": group.key,
            "clustered": "true" if group.clustered else "false",
            "cluster_size": group.cluster_size,
            "participants": participants
        }

    @validate(group_class="str", group_key="str", cluster_size="int")
    async def update(self, group_class, group_key, cluster_size, **ignored):
        groups = self.application.groups
        group_id = self.context.get("group_id")

        try:
            await groups.update_group(
                self.gamespace,
                group_id,
                group_class,
                group_key,
                cluster_size)
        except GroupError as e:
            raise a.ActionError("Failed to update a group:" + e.message)

        raise a.Redirect(
            "group",
            message="A group has been updated",
            group_id=group_id)

    async def delete(self, **ignored):
        groups = self.application.groups
        group_id = self.context.get("group_id")

        try:
            group = await groups.get_group(self.gamespace, group_id)
        except GroupNotFound:
            raise a.ActionError("No such group")

        try:
            await groups.delete_group(self.gamespace, group)
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
            a.script(self.application.module_path("static/admin/messages.js"),
                     account=self.context.get("account")),
            a.links("Navigate", [
                a.link("users", "Go back", icon="chevron-left"),
                a.link("user_messages", "User Messages History", icon="comments-o",
                       account_id=self.context.get("account")),
                a.link("history", "Sent Messages By User", icon="upload",
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

    async def prepared(self, account):
        online = self.application.online
        account_id = to_int(account)

        if not account_id:
            raise a.ActionError("Bad account")

        self.conversation = await online.conversation(self.gamespace, account_id)
        self.conversation.set_on_message(self._on_message)
        self.conversation.set_on_deleted(self._on_message_deleted)
        self.conversation.set_on_updated(self._on_message_updated)
        await self.conversation.init()

        logging.debug("Exchange has been opened!")

    async def _on_message(self, gamespace_id, message_id, sender,
                          recipient_class, recipient_key, message_type, payload, time, flags):
        try:
            result = await self.send_request(
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
        except JsonRPCError:
            return False

        return result

    async def _on_message_deleted(self, gamespace_id, message_id, sender):
        try:
            result = await self.send_request(
                self,
                "message_deleted",
                gamespace_id=gamespace_id,
                sender=sender,
                message_id=message_id)
        except JsonRPCError:
            return False

        return result

    async def _on_message_updated(self, gamespace_id, message_id, sender, payload):
        try:
            result = await self.send_request(
                self,
                "message_updated",
                gamespace_id=gamespace_id,
                sender=sender,
                message_id=message_id,
                payload=payload)
        except JsonRPCError:
            return False

        return result

    @validate(recipient_class="str", recipient_key="str", sender="int",
              message_type="str", message="load_json", flags="json_list_of_strings")
    def send_message(self, recipient_class, recipient_key, sender, message_type, message, flags):

        gamespace_id = self.gamespace
        message_queue = self.application.message_queue

        return message_queue.add_message(
            gamespace_id,
            sender,
            recipient_class,
            recipient_key,
            message_type,
            message,
            MessageFlags(flags),
            authoritative=True)

    @validate(message_id="str", sender="int")
    async def delete_message(self, message_id, sender):

        gamespace_id = self.gamespace
        history = self.application.history

        try:
            await history.delete_message_concurrent(
                gamespace_id,
                sender,
                message_id)
        except MessageNotFound:
            raise a.StreamCommandError(404, "No such message")
        except MessageError as e:
            raise a.StreamCommandError(e.code, e.message)

    @validate(message_id="str", sender="int", payload="json_dict")
    async def update_message(self, message_id, sender, payload):

        gamespace_id = self.gamespace
        history = self.application.history

        try:
            result = await history.update_message_concurrent(
                gamespace_id,
                sender,
                message_id,
                payload)
        except MessageNotFound:
            raise a.StreamCommandError(404, "No such message")
        except MessageError as e:
            raise a.StreamCommandError(e.code, e.message)

        return result

    async def on_opened(self, **kwargs):
        pass

    async def on_closed(self):
        if self.conversation:
            await self.conversation.release()
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
                a.link("index", "Go back", icon="chevron-left")
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    async def filter(self, **args):

        page = self.context.get("page", 1)

        filters = {
            "page": page
        }

        filters.update({
            k: v for k, v in args.items() if v
        })

        raise a.Redirect("history", **filters)

    @validate(page="int", message_sender="int", message_recipient_class="str",
              message_recipient="str", message_type="str", message_delivered="int")
    async def get(self,
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

            q.offset = (page - 1) * MessagesHistoryController.MESSAGES_PER_PAGE
            q.limit = MessagesHistoryController.MESSAGES_PER_PAGE
            q.message_sender = message_sender
            q.message_recipient_class = message_recipient_class
            q.message_recipient = message_recipient
            q.message_type = message_type

            if message_delivered:
                q.message_delivered = message_delivered == "yes"

            messages, count = await q.query(count=True)
            pages = int(math.ceil(float(count) / float(MessagesHistoryController.MESSAGES_PER_PAGE)))
        else:
            messages, pages = [], 0

        return {
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
        }


class UserMessagesController(a.AdminController):
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

        account_id = self.context.get("account_id")

        return [
            a.breadcrumbs([
                a.link("users", "Messages"),
                a.link("messages", "User @{0}".format(account_id), account=account_id),

            ], "User Messages History"),
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
                }], messages, "default", empty="No messages to display."),
            a.pages(data["pages"]),
            a.links("Navigate", [
                a.link("messages", "Go back", icon="chevron-left", account=account_id)
            ])
        ]

    def access_scopes(self):
        return ["message_admin"]

    @validate(account_id="int", page="int")
    async def get(self, account_id, page=1):
        history = self.application.history

        offset = (page - 1) * UserMessagesController.MESSAGES_PER_PAGE

        messages, count = await history.list_messages_account_with_count(
            gamespace=self.gamespace, account_id=account_id,
            limit=UserMessagesController.MESSAGES_PER_PAGE, offset=offset)

        pages = int(math.ceil(float(count) / float(UserMessagesController.MESSAGES_PER_PAGE)))

        return {
            "messages": messages,
            "pages": pages
        }
