
from common.options import options

import handler
import common.server
import common.database
import common.access
import common.sign
import common.keyvalue

import admin as a
import options as _opts

from model.history import MessagesHistoryModel
from model.group import GroupsModel
from model.online import OnlineModel
from model.queue import MessagesQueueModel


class MessagesServer(common.server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(MessagesServer, self).__init__()

        self.db = common.database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.history = MessagesHistoryModel(self.db)
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
            "index": a.IndexController,
            "users": a.UsersController,
            "groups": a.GroupsController,
            "new_group": a.NewGroupController,
            "group": a.GroupController,
            "groups_by_class": a.FindGroupsByClassController,
            "group_participation": a.GroupParticipantController,
            "add_group_participation": a.AddGroupParticipantController,
            "add_user_participation": a.AddUserParticipantController,
            "user": a.UserController,
            "messages": a.MessagesController,
            "history": a.MessagesHistoryController
        }

    def get_models(self):
        return [self.groups, self.history, self.online, self.message_queue]

    def get_internal_handler(self):
        return handler.InternalHandler(self)

    def get_admin_stream(self):
        return {
            "stream_messages": a.MessagesStreamController
        }

    def get_handlers(self):
        return [
            (r"/group/(\w+)/(.*)/join", handler.JoinGroupHandler),
            (r"/group/(\w+)/(.*)", handler.ReadGroupInboxHandler),
            (r"/send/(\w+)/(\w+)", handler.SendMessageHandler),
            (r"/send", handler.SendMessagesHandler),
            (r"/listen", handler.ConversationEndpointHandler)
        ]


if __name__ == "__main__":
    stt = common.server.init()
    common.access.AccessToken.init([common.access.public()])
    common.server.start(MessagesServer)
