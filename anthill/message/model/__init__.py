
from anthill.common.validate import validate
from anthill.common import Flags


CLASS_USER = "user"


class MessageError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class MessageSendError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message


class MessageFlags(Flags):
    # This message will be removed once delivered
    REMOVE_DELIVERED = 'remove_delivered'
    # This message will be delivered only to those who're online atm, otherwise not even stored
    DO_NOT_STORE = 'do_not_store'
    # This message can be edited by anyone who has message's UUID
    EDITABLE = 'editable'
    # This message can be deleted by anyone who has message's UUID
    DELETABLE = 'deletable'
    # This message has been sent from authoritative server and should be trusted
    SERVER = 'server'
