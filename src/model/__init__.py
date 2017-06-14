
from common.validate import validate
from common import Flags


CLASS_USER = "user"


class MessageError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class MessageSendError(Exception):
    def __init__(self, message):
        self.message = message


class MessageFlags(Flags):
    # This message will be removed once delivered
    REMOVE_DELIVERED = 'remove_delivered'
    # This message can be edited by anyone who has message's UUID
    EDITABLE = 'editable'
    # This message can be deleted by anyone who has message's UUID
    DELETABLE = 'deletable'
