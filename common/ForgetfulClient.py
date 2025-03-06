import uuid
from typing import Any

from aio_pika import connect_robust, Message, DeliveryMode
from aio_pika.abc import AbstractConnection, AbstractChannel
from msgspec import msgpack

from common.msg_types import MsgType


class ForgetfulClient:
    """A simple forgetful client that publishes messages without waiting for a response."""

    connection: AbstractConnection
    channel: AbstractChannel
    rabbitmq_url: str
    routing_key: str

    def __init__(self, rabbitmq_url: str, routing_key: str):
        self.rabbitmq_url = rabbitmq_url
        self.routing_key = routing_key

    async def connect(self):
        """Establish connection to the broker."""
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()

        # Declare a queue to publish compensating messages
        await self.channel.declare_queue(self.routing_key, durable=True)

    async def call(self, msg: Any, msg_type: MsgType):
        """Publish a message to the queue."""
        if not self.connection or not self.channel:
            raise Exception("Client not connected to the broker.")

        correlation_id = str(uuid.uuid4())

        await self.channel.default_exchange.publish(
            Message(
                msgpack.encode(msg),
                content_type="application/msgpack",
                correlation_id=correlation_id,
                delivery_mode=DeliveryMode.PERSISTENT,
                type=msg_type,
            ),
            routing_key=self.routing_key,
        )

    async def close(self):
        """Close the connection."""
        if self.channel:
            await self.channel.close()  # Close the channel
        if self.connection:
            await self.connection.close()  # Close the connection