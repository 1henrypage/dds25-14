import asyncio
import uuid
from typing import MutableMapping

from aio_pika import Message, connect
from aio_pika.abc import (
    AbstractChannel, AbstractConnection, AbstractIncomingMessage, AbstractQueue
)

class QueuePublisherClient:
    connection: AbstractConnection
    channel :AbstractChannel
    callback_queue: AbstractQueue

    def __init__(self, queue_url: str):
        self.futures: MutableMapping[str, asyncio.Future]= {}
        self.queue_url = queue_url

    async def connect(self) -> "QueuePublisherClient":
        self.connection = await connect(self.queue_url)
        self.channel = await self.connection.channel()
            self.callback_queue = await


