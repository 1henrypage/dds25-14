
from enum import Enum

class MsgType(str, Enum):
    """
    KEEP THESE SHORT SO THAT MESSAGE PAYLOADS ARE AS SMALL AS POSSIBLE
    """
    CREATE = "CT"
    BATCH_INIT = "BI"
    FIND = "FD"
    ADD = "AD"
    SUBTRACT = "ST"
    CHECKOUT = "CO"
    SUBTRACT_BULK = "SB"
    ADD_BULK = "AB"
    CHECKOUT_REPLY = "CR"

    def __str__(self):
        return self.value

