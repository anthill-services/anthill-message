
from common.validate import validate


CLASS_USER = "user"
CLASS_GROUP = "group"


class MessageError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class MessageSendError(Exception):
    def __init__(self, message):
        self.message = message


class DeliveryFlags(object):
    REMOVE_DELIVERED = 'remove_delivered'

    @staticmethod
    @validate(flags="json_list_of_strings")
    def parse(flags):
        return list(set(flags) & {
            DeliveryFlags.REMOVE_DELIVERED
        })
