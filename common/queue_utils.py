import asyncio
import uuid
import os
import logging
from typing import MutableMapping, Callable, Any

import redis
from msgspec import msgpack

from aio_pika import Message, connect_robust, DeliveryMode
from aio_pika.abc import (
    AbstractChannel, AbstractConnection, AbstractIncomingMessage, AbstractQueue, ExchangeType, AbstractExchange
)

from common.msg_types import MsgType

class OrderWorkerClient:
    """
    Client which just inserts stuff into a queue from the order. No response expectation
    """
    connection: AbstractConnection
    channel: AbstractChannel
    rabbitmq_url: str
    online: bool

    def __init__(self, rabbitmq_url: str) -> None:
        """
        Initialise the object, but don't connect!

        :param rabbitmq_url: The URL of the RabbitMQ server
        """
        self.rabbitmq_url = rabbitmq_url

    async def connect(self) -> "OrderWorkerClient":
        """
        (async) Connects to the RabbitMQ server and creates a channel.
        """
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.online = True
        return self

    async def call_with_route_no_reply(self, msg: Any, msg_type: MsgType, routing_key: str, reply_to: str, correlation_id: str = None):
        message = Message(
            msgpack.encode(msg),
            content_type="application/msgpack",
            correlation_id=correlation_id,
            type=msg_type,
            reply_to=reply_to,
            priority=msg_type.priority()
        )

        await self.channel.default_exchange.publish(message, routing_key=routing_key)


    async def disconnect(self):
        """
        Disconnects the client gracefully
        """
        self.online = False
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()




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
    online: bool

    def __init__(self, routing_key: str, rabbitmq_url: str) -> None:
        """
        Initialise the object, but don't connect!

        :param routing_key: The routing key for the ingress queue
        :param rabbitmq_url: The URL of the RabbitMQ server
        """
        self.futures: MutableMapping[str, asyncio.Future] = {}
        self.rabbitmq_url = rabbitmq_url
        self.routing_key = routing_key
        self.online = False

    async def connect(self) -> "RpcClient":
        """
        (async) Connects to the RabbitMQ server and creates a channel as well as a callback queue.
        :return: RPCClient itself
        """
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True, arguments={"x-max-priority": 1})
        await self.callback_queue.consume(self.on_response, no_ack=True)
        self.online = True
        return self

    async def on_response(self, message: AbstractIncomingMessage) -> None:
        """
        Callback function which sets the future with the response from a worker.

        :param message: The response message from the worker
        :return: Nothing, the future will have a result
        """
        if message.correlation_id is None:
            logging.debug(f"Message doesn't have correlation ID: {message!r}")
            return

        if message.correlation_id not in self.futures:
            logging.debug(f"Message has correlation id, but scaling out made something weird happen")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        if not future.done():
            future.set_result(message.body)

    async def call(self, msg: Any, msg_type: MsgType):
        """
        Forwards a message into a queue with a correlation id and asynchronously
        listens for a response.

        :param msg: The message to forward
        :param msg_type: The type of message to forward
        :return: A future which will resolve with the response.
        """

        if not self.online:
            raise RuntimeError("RpcClient is offline. Cannot send messages.")

        correlation_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()

        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                msgpack.encode(msg),
                content_type="application/msgpack",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
                type=msg_type,
                priority=msg_type.priority()
            ),
            routing_key=self.routing_key
        )

        return await future

    async def disconnect(self):
        """
        Disconnects the client gracefully
        """

        self.online = False

        if self.futures:
            await asyncio.gather(*self.futures.values(), return_exceptions=True)

        if self.channel:
            await self.channel.close()  # Close the channel
        if self.connection:
            await self.connection.close()  # Close the connection


async def consume_events(process_message: Callable[[AbstractIncomingMessage], Any],
                         get_message_response_type: Callable[[AbstractIncomingMessage], MsgType | None],
                         get_custom_reply_to: Callable[[AbstractIncomingMessage], str | None]) -> None:
    """
    Event loop function which just listens for events in an ingress queue. (THIS IS FOR THE WORKER)
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
    await channel.set_qos(prefetch_count=500)
    exchange = channel.default_exchange
    queue = await channel.declare_queue(os.environ['ROUTE_KEY'], arguments={"x-max-priority": 1})

    async with queue.iterator() as qiterator:
        message: AbstractIncomingMessage
        async for message in qiterator:
            try:
                # If we start processing this and the worker dies, the message is lost
                # For the actual implementation look here: https://github.com/mosquito/aio-pika/blob/master/aio_pika/message.py#L538
                # Parameters:
                #     requeue – Requeue message when exception
                #           -> True. If worker dies we want someone else to handle the message.
                #     reject_on_redelivered – When True message will be rejected only when message was redelivered
                #           -> False. If worker dies we want someone else to handle the message and not reject it.
                #     ignore_processed – Do nothing if message already processed (ack, nack, reject has been sent)
                #           -> True. We don't want to reprocess messages that we have already acknowledged.
                #               Also, set to True if u want to manually ack, nack or reject messages, which will be helpful for setting up the idempotency mechanism.
                # We will use idempotency keys to ensure that messages that were processed but not acknowledged are not reprocessed.
                async with message.process(requeue=True, reject_on_redelivered=False, ignore_processed=True):
                    # TODO: If processed, send an acknowledgement with the cached response but don't process again (Idempotency)
                    result = await process_message(message)
                    reply_to = await get_custom_reply_to(message) or message.reply_to

                    if reply_to and result is not None:
                        msg_type = get_message_response_type(message)
                        await exchange.publish(
                            Message(
                                body=msgpack.encode(result),
                                content_type="application/msgpack",
                                correlation_id=message.correlation_id,
                                type=msg_type,
                                priority= msg_type.priority() if msg_type else 0
                            ),
                            routing_key=reply_to
                        )
                    else:
                        logging.debug(f"Message does not have a reply queue {message!r}")
            except Exception:
                logging.exception("Processing error for message %r", message)
