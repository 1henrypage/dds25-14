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

from common.msg_types import MsgType


class RpcClient:
    """
    RPC Client to interact to queues and to listen for a response (through an exclusive response queue dedicated
    to this specific client). I thought about the futures problem a lot and could not come up with a better solution
    which did not involve and insane amount of overhead (examples: redirect to another endpoint which is responsible for polling
    a shared response queue between all consumers. We yield 2x network requests in this case. Other possibility is to FAN OUT all responses
    to all currently connected consumers, however, this means that all consumers are processing all messages and does not yield a performance
    improvement.
    """
    connection: AbstractConnection
    channel: AbstractChannel
    callback_queue: AbstractQueue
    rabbitmq_url: str
    routing_key: str

    def __init__(self, routing_key: str, rabbitmq_url: str) -> None:
        """
        Initialise the object, but don't connect!

        :param routing_key: The routing key for the egress queue
        :param rabbitmq_url: The URL of the RabbitMQ server
        """
        self.futures: MutableMapping[str, asyncio.Future] = {}
        self.rabbitmq_url = rabbitmq_url
        self.routing_key = routing_key

    async def connect(self) -> "RpcClient":
        """
        (async) Connects to the RabbitMQ server and creates a channel as well as a callback queue.
        :return: RPCClient itself
        """
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response, no_ack=True)
        return self

    async def on_response(self, message: AbstractIncomingMessage) -> None:
        """
        Callback function which sets the future with the response from a worker.

        :param message: The response message from the worker
        :return: Nothing, the future will have a result
        """
        if message.correlation_id is None:
            logging.error(f"Message doesn't have correlation ID: {message!r}")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        future.set_result(message.body)

    async def call(self, msg: Any, msg_type: MsgType):
        """
        Forwards a message into a queue with a correlation id and asynchronously
        listens for a response.

        :param msg: The message to forward
        :param msg_type: The type of message to forward
        :return: A future which will resolve with the response.
        """
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
        """
        Disconnects the client gracefully
        """

        if self.channel:
            await self.channel.close()  # Close the channel
        if self.connection:
            await self.connection.close()  # Close the connection


async def consume_events(process_message: Callable[[str, Any], Any]) -> None:
    """
    Event loop function which just listens for events in an egress queue. (THIS IS FOR THE WORKER)
    This will process messages and forward events in a specific response queue.

    :param process_message: Lambda function which should be defined as follows
    input: (message_type: str, content: idk)
    return wrapped_response
    you can see examples of this at the bottom of each worker.py file.
    :return: Nothing
    """
    # Perform connection
    connection = await connect_robust(os.environ['RABBITMQ_URL'])
    channel = await connection.channel()
    exchange = channel.default_exchange
    queue = await channel.declare_queue(os.environ['ROUTE_KEY'])

    async with queue.iterator() as qiterator:
        message: AbstractIncomingMessage
        async for message in qiterator:
            try:
                # TODO LOOK AT THE DOCUMENTATION FOR PARAMETERS THAT THIS TAKES
                # TODO EXPONENTIAL BACKOFF WILL NEED TO BE IMPLEMENTED MANUALLY
                async with message.process(requeue=False):
                    assert message.reply_to is not None
                    message_type = message.type
                    message_body = msgpack.decode(message.body)
                    result = process_message(message_type, message_body)

                    if MsgType(message_type) in [MsgType.CHECKOUT, MsgType.ADD_BULK, MsgType.SUBTRACT_BULK]:
                        continue

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
