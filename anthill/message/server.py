
from anthill.common.options import options
from anthill.common import server, database, access

from . model.history import MessagesHistoryModel
from . model.group import GroupsModel
from . model.online import OnlineModel
from . model.queue import MessagesQueueModel
from . import handler as h
from . import admin
from . import options as _opts


class MessagesServer(server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(MessagesServer, self).__init__()

        self.db = database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.history = MessagesHistoryModel(self.db, self)
        self.groups = GroupsModel(self.db, self)
        self.online = OnlineModel(self.groups, self.history)
        self.message_queue = MessagesQueueModel(self.history)

    def get_metadata(self):
        return {
            "title": "Messages",
            "description": "Deliver messages from the user, to the user",
            "icon": "envelope"
        }

    def get_admin(self):
        return {
            "index": admin.IndexController,
            "users": admin.UsersController,
            "groups": admin.GroupsController,
            "new_group": admin.NewGroupController,
            "group": admin.GroupController,
            "groups_by_class": admin.FindGroupsByClassController,
            "group_participation": admin.GroupParticipantController,
            "add_group_participation": admin.AddGroupParticipantController,
            "add_user_participation": admin.AddUserParticipantController,
            "user": admin.UserController,
            "messages": admin.MessagesController,
            "history": admin.MessagesHistoryController,
            "user_messages": admin.UserMessagesController
        }

    def get_models(self):
        return [self.groups, self.history, self.online, self.message_queue]

    def get_internal_handler(self):
        return h.InternalHandler(self)

    def get_admin_stream(self):
        return {
            "stream_messages": admin.MessagesStreamController
        }

    def get_handlers(self):
        return [
            (r"/group/(\w+)/(.*)/join", h.JoinGroupHandler),
            (r"/group/(\w+)/(.*)", h.ReadGroupInboxHandler),
            (r"/send/(\w+)/(\w+)", h.SendMessageHandler),
            (r"/send", h.SendMessagesHandler),
            (r"/messages", h.ReadMessagesHandler),
            (r"/messages/with/(.*)", h.ReadMessagesRecipientHandler),
            (r"/message/(.*)", h.MessageHandler),
            (r"/listen", h.ConversationEndpointHandler)
        ]


if __name__ == "__main__":
    stt = server.init()
    access.AccessToken.init([access.public()])
    server.start(MessagesServer)
