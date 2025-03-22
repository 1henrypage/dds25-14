
from enum import Enum

from aio_pika.abc import AbstractIncomingMessage


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
    SAGA_INIT = "SI"
    SAGA_PAYMENT_RESPONSE = "SP"
    SAGA_STOCK_RESPONSE = "SS"
    SAGA_PAYMENT_REVERSE = "SPR"
    SAGA_STOCK_REVERSE = "SSR"

    def __str__(self):
        return self.value

    def priority(self):
        if self in (MsgType.SAGA_PAYMENT_RESPONSE, MsgType.SAGA_STOCK_RESPONSE,
                    MsgType.SAGA_INIT, MsgType.SAGA_PAYMENT_REVERSE, MsgType.SAGA_STOCK_REVERSE):
            return 1
        return 0

