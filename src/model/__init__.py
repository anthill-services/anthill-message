
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


class DeliveryFlags(Flags):
    REMOVE_DELIVERED = 'remove_delivered'
