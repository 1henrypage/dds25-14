
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

    def __str__(self):
        return self.value

