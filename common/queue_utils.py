import asyncio
import uuid
import os
import logging
from typing import MutableMapping, Callable, Any

from msgspec import msgpack

from aio_pika import Message, connect_robust, DeliveryMode
from aio_pika.abc import (
    AbstractChannel, AbstractConnection, AbstractIncomingMessage, AbstractQueue
)

class RpcClient:
    connection: AbstractConnection
    channel: AbstractChannel
    callback_queue: AbstractQueue
    rabbitmq_url: str
    routing_key: str

    def __init__(self, routing_key, rabbitmq_url) -> None:
        self.futures: MutableMapping[str, asyncio.Future] = {}
        self.rabbitmq_url = rabbitmq_url
        self.routing_key = routing_key

    async def connect(self) -> "RpcClient":
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response, no_ack=True)
        return self

    async def on_response(self, message: AbstractIncomingMessage) -> None:
        if message.correlation_id is None:
            print(f"Bad message {message!r}")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        future.set_result(message.body)

    async def call(self, msg, msg_type):
        correlation_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()

        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                msgpack.encode(msg),
                content_type="application/msgpack",
                correlation_id=correlation_id,
                delivery_mode=DeliveryMode.PERSISTENT,
                reply_to=self.callback_queue.name,
                type=msg_type,
            ),
            routing_key=self.routing_key,
        )

        return await future

    async def disconnect(self):
        if self.channel:
            await self.channel.close()  # Close the channel
        if self.connection:
            await self.connection.close()  # Close the connection


async def consume_events(process_message: Callable[[str, Any], Any]) -> None:
    # Perform connection
    connection = await connect_robust(os.environ['RABBITMQ_URL'])
    channel = await connection.channel()
    exchange = channel.default_exchange
    queue = await channel.declare_queue(os.environ['ROUTE_KEY'])

    async with queue.iterator() as qiterator:
        message: AbstractIncomingMessage
        async for message in qiterator:
            try:
                async with message.process(requeue=False):
                    assert message.reply_to is not None
                    message_type = message.type
                    message_body = msgpack.decode(message.body)
                    result = process_message(message_type, message_body)

                    await exchange.publish(
                        Message(
                            body=msgpack.encode(result),
                            content_type="application/msgpack",
                            correlation_id=message.correlation_id,
                            delivery_mode=DeliveryMode.PERSISTENT,
                        ),
                        routing_key=message.reply_to,
                    )
            except Exception:
                logging.exception("Processing error for message %r", message)




