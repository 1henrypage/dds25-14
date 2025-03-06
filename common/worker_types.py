from enum import Enum

class WorkerType(str, Enum):
    """
    KEEP THESE SHORT SO THAT MESSAGE PAYLOADS ARE AS SMALL AS POSSIBLE
    """
    STOCK = "ST"
    PAYMENT = "PM"

    def __str__(self):
        return self.value