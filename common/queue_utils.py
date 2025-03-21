import asyncio
import uuid
import os
import logging
from typing import MutableMapping, Callable, Any, Optional

from msgspec import msgpack

from aio_pika import Message, connect_robust, DeliveryMode
from aio_pika.abc import (
    AbstractChannel, AbstractConnection, AbstractIncomingMessage, AbstractQueue, ExchangeType, AbstractExchange
)
from aio_pika.exceptions import AMQPError
from common.msg_types import MsgType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry settings
BACKOFF_DELAYS = [1, 2, 4, 8, 16]  # Exponential backoff retry delays
MAX_RETRIES = len(BACKOFF_DELAYS)


# Retry decorator
def with_retry(
    max_retries: int = MAX_RETRIES,
    backoff_delays: list[int] = BACKOFF_DELAYS,
    retryable_exceptions: tuple[type[Exception], ...] = (ConnectionError, AMQPError)
):
    """Decorator for retrying async functions with exponential backoff."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {backoff_delays[attempt]}s..."
                        )
                        await asyncio.sleep(backoff_delays[attempt])
                    else:
                       logger.error(f"All {max_retries} attempts failed. Last error: {e}")
                       raise last_exception
        return wrapper
    return decorator

async def process_message_with_retry(
    message: AbstractIncomingMessage,
    process_message: Callable[[AbstractIncomingMessage], Any],
    exchange: AbstractExchange,
    get_message_response_type: Callable[[AbstractIncomingMessage], Optional[str]],
    get_custom_reply_to: Callable[[AbstractIncomingMessage], Optional[str]]
):
    """Processes a message with retry logic before forwarding it to a response queue."""

    @with_retry()
    async def process():
        async with message.process(requeue=False):  # Ensures message is acknowledged or requeued
            result = await process_message(message)  # Process the message

            reply_to = get_custom_reply_to(message) or message.reply_to  # Get reply address

            if reply_to and result is not None:
                await exchange.publish(
                    Message(
                        body=msgpack.encode(result),
                        content_type="application/msgpack",
                        correlation_id=message.correlation_id,
                        delivery_mode=DeliveryMode.PERSISTENT,
                        type=get_message_response_type(message),
                    ),
                    routing_key=reply_to,
                )
            else:
                logger.warning(f"Message does not have a reply queue: {message!r}")

    try:
        await process()
    except Exception as e:
        logger.error(f"Failed to process message after retries: {e}")


class OrderWorkerClient:
    """
    Client which just inserts stuff into a queue from the order. No response expectation
    """
    connection: AbstractConnection
    channel: AbstractChannel
    exchange: AbstractExchange
    rabbitmq_url: str
    exchange_key: str

    def __init__(self, rabbitmq_url: str, exchange_key: str) -> None:
        """
        Initialise the object, but don't connect!

        :param exchange_key: The exchange name or queue name, depending on mode
        :param rabbitmq_url: The URL of the RabbitMQ server
        :param publish_to_exchange: Whether to publish to an exchange (True) or a queue (False)
        """
        self.rabbitmq_url = rabbitmq_url
        self.key = exchange_key
        self.connection = None
        self.channel = None
        self.exchange = None

    @with_retry()
    async def connect(self) -> "OrderWorkerClient":
        """
        (async) Connects to the RabbitMQ server and creates a channel.
        """
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            self.key, ExchangeType.FANOUT, durable=True
        )
        return self

    @with_retry()
    async def order_fanout_call(self, msg: Any, msg_type: MsgType, reply_to: str, correlation_id: str = None):
        """
        Forwards a message into a fanout exchange with a correlation id.
        """

        message = Message(
            msgpack.encode(msg),
            content_type="application/msgpack",
            correlation_id=correlation_id or str(uuid.uuid4()),
            delivery_mode=DeliveryMode.PERSISTENT,
            type=msg_type,
            reply_to=reply_to,
        )

        await self.exchange.publish(message, routing_key="")  # Routing key ignored for fanout

    async def disconnect(self):
        """
        Disconnects the client gracefully
        """
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
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.online = False

    @with_retry()
    async def connect(self) -> "RpcClient":
        """
        (async) Connects to the RabbitMQ server and creates a channel as well as a callback queue.
        :return: RPCClient itself
        """
        self.connection = await connect_robust(self.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
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

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        if future:
            future.set_result(message.body)
            await message.ack()

    @with_retry()
    async def call(self, msg: Any, msg_type: MsgType, timeout: float = 30.0):
        """
        Forwards a message into a queue with a correlation id and waits for a response.

        :param msg: The message to forward
        :param msg_type: The type of message to forward
        :return: A future which will resolve with the response.
        """

        if not self.online:
            raise RuntimeError("RpcClient is offline. Cannot send messages.")

        correlation_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self.futures[correlation_id] = future

        message = Message(
            msgpack.encode(msg),
            content_type="application/msgpack",
            correlation_id=correlation_id,
            delivery_mode=DeliveryMode.PERSISTENT,
            reply_to=self.callback_queue.name,
            type=msg_type,
        )

        await self.channel.default_exchange.publish(
            message,
            routing_key=self.routing_key,
            mandatory=True,
        )

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            del self.futures[correlation_id]
            raise TimeoutError(f"RPC call timed out after {timeout} seconds.")

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


async def consume_events(
    process_message: Callable[[AbstractIncomingMessage], Any],
    get_message_response_type: Callable[[AbstractIncomingMessage], Optional[str]],
    get_custom_reply_to: Callable[[AbstractIncomingMessage], Optional[str]]
):
    """Consumer loop with automatic reconnection."""

    async def setup_connection():
        """Handles RabbitMQ connection setup and reconnection."""
        while True:
            try:
                logger.info("Connecting to RabbitMQ...")
                connection = await connect_robust(os.environ['RABBITMQ_URL'])
                channel = await connection.channel()
                exchange = channel.default_exchange
                queue = await channel.declare_queue(os.environ['ROUTE_KEY'], durable=True)
                if "ORDER_OUTBOUND" in os.environ:
                    order_outbound_exchange = await channel.declare_exchange(os.environ['ORDER_OUTBOUND'], ExchangeType.FANOUT, durable=True)
                    await queue.bind(order_outbound_exchange)
                
                logger.info("Connected to RabbitMQ successfully.")
                return connection, channel, exchange, queue
            except (AMQPError, ConnectionError) as e:
                logger.error(f"RabbitMQ connection failed: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    while True:
        connection, channel, exchange, queue = await setup_connection()
        async with queue.iterator() as qiterator:
            async for message in qiterator:
                try:
                    await process_message_with_retry(
                        message, process_message, exchange, get_message_response_type, get_custom_reply_to
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
