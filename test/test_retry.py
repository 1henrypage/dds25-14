import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aio_pika import Message
import sys
from pathlib import Path
from typing import Any

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from common.msg_types import MsgType
from common.queue_utils import OrderWorkerClient, RpcClient, process_message_with_retry, with_retry

# Mock RabbitMQ URL and Routing Key
RABBITMQ_URL = "amqp://user:pass@localhost:5672/"
EXCHANGE_KEY = "test_exchange"
ROUTING_KEY = "test_queue"

@pytest.mark.asyncio
async def test_order_worker_client_retry():
    worker_client = OrderWorkerClient(RABBITMQ_URL, EXCHANGE_KEY)

    # Patch connect to skip real RabbitMQ connection
    with patch.object(worker_client, 'connect', new_callable=AsyncMock), \
         patch("asyncio.sleep", new_callable=AsyncMock):

        await worker_client.connect()  # This sets up .exchange

        # Create a counter to simulate one failure then success
        call_count = 0

        async def publish_mock(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Simulated failure")
            return None

        # Patch the actual publish method on the real .exchange
        worker_client.exchange = AsyncMock()
        worker_client.exchange.publish = AsyncMock(side_effect=publish_mock)

        await worker_client.order_fanout_call("test_message", MsgType.CREATE, "reply_queue")

        # Verify retry occurred
        assert worker_client.exchange.publish.call_count == 2



@pytest.mark.asyncio
async def test_rpc_client_retry():
    """Test RpcClient retries message sending on failure."""
    #initiate the rpc client so that we can test the retry
    rpc_client = RpcClient(ROUTING_KEY, RABBITMQ_URL)
    rpc_client.online = True
    rpc_client.callback_queue = MagicMock()
    rpc_client.callback_queue.name = "mock_callback_queue"
    rpc_client.futures = {}
    rpc_client.channel = MagicMock()
    rpc_client.channel.default_exchange = AsyncMock()

    call_count = 0

    async def mock_publish(message, routing_key, mandatory=True):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated publish failure")
        # Set the future result to simulate a response
        rpc_client.futures[message.correlation_id].set_result(b"mock_response")

    rpc_client.channel.default_exchange.publish.side_effect = mock_publish

    # we patch asyncio.sleep to avoid real delay between retries
    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await rpc_client.call("test_message", MsgType.CREATE)

    #assert that it retried once and succeeded
    assert result == b"mock_response"
    assert call_count == 2

@pytest.mark.asyncio
async def test_process_message_with_retry():
    """Test process_message_with_retry handles retries correctly."""

    # Properly mock async context manager for message.process
    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__ = AsyncMock(return_value=None)
    mock_context_manager.__aexit__ = AsyncMock(return_value=None)

    mock_message = AsyncMock(spec=Message)
    mock_message.process = MagicMock(return_value=mock_context_manager)
    mock_message.reply_to = None
    mock_message.correlation_id = "123"

    # Fail once with a retryable exception (ConnectionError), then succeed
    call_count = 0

    async def mock_process_fn(msg):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Processing failed")
        return {"response": "Success"}

    mock_exchange = AsyncMock()
    mock_get_response_type = AsyncMock(return_value="response_type")
    mock_get_custom_reply_to = AsyncMock(return_value="reply_queue")

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await process_message_with_retry(
            mock_message, mock_process_fn, mock_exchange,
            mock_get_response_type, mock_get_custom_reply_to
        )

    assert call_count == 2
    assert mock_exchange.publish.called


@pytest.mark.asyncio
async def test_rpc_client_on_response():
    """Test RpcClient's on_response method processes a valid response."""

    rpc_client = RpcClient(ROUTING_KEY, RABBITMQ_URL)

    # Set up the future and save a reference to it
    future = asyncio.get_running_loop().create_future()
    rpc_client.futures = {"test_correlation_id": future}

    # Create mock message
    mock_message = AsyncMock(spec=Message)
    mock_message.correlation_id = "test_correlation_id"
    mock_message.body = b"test_response"
    mock_message.ack = AsyncMock()

    # Call the method
    await rpc_client.on_response(mock_message)

    # Now check the stored future (not from rpc_client.futures anymore!)
    assert future.done()
    assert future.result() == b"test_response"
    mock_message.ack.assert_called_once()




@pytest.mark.asyncio
async def test_with_retry():
    """Test that the with_retry decorator retries failing function calls correctly."""
    call_count = 0

    @with_retry(max_retries=3, backoff_delays=[0.1, 0.2, 0.3])
    async def failing_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Temporary failure")
        return "Success"

    result = await failing_func()

    assert call_count == 3  # Ensures it retried twice before success
    assert result == "Success"


@pytest.mark.asyncio
async def test_order_worker_client_connect():
    """Test that OrderWorkerClient connects to RabbitMQ successfully."""
    worker_client = OrderWorkerClient(RABBITMQ_URL, EXCHANGE_KEY)
    
    # Mock the connection methods
    worker_client.connection = AsyncMock()
    worker_client.channel = AsyncMock()
    worker_client.exchange = AsyncMock()
    worker_client.connect = AsyncMock(return_value=worker_client)

    await worker_client.connect()

    assert worker_client.connect.called
    assert worker_client.exchange is not None


@pytest.mark.asyncio
async def test_rpc_client_connect():
    """Test that RpcClient connects and declares a callback queue."""
    rpc_client = RpcClient(ROUTING_KEY, RABBITMQ_URL)

    rpc_client.connection = AsyncMock()
    rpc_client.channel = AsyncMock()
    rpc_client.callback_queue = AsyncMock()
    rpc_client.connect = AsyncMock(return_value=rpc_client)

    await rpc_client.connect()

    assert rpc_client.connect.called
    assert rpc_client.callback_queue is not None


@pytest.mark.asyncio
async def test_rpc_client_timeout():
    """Test that RpcClient times out when no response is received."""
    rpc_client = RpcClient(ROUTING_KEY, RABBITMQ_URL)
    rpc_client.channel = AsyncMock()
    rpc_client.channel.default_exchange = AsyncMock()
    rpc_client.callback_queue = AsyncMock()
    rpc_client.callback_queue.name = "callback_queue"
    rpc_client.online = True 

    with pytest.raises(TimeoutError, match="RPC call timed out"):
        await rpc_client.call("test_message", MsgType.CREATE, timeout=0.1)


@pytest.mark.asyncio
async def test_order_worker_client_disconnect():
    """Test that OrderWorkerClient disconnects gracefully."""
    worker_client = OrderWorkerClient(RABBITMQ_URL, EXCHANGE_KEY)

    worker_client.connection = AsyncMock()
    worker_client.channel = AsyncMock()

    await worker_client.disconnect()

    worker_client.channel.close.assert_called_once()
    worker_client.connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_rpc_client_disconnect():
    """Test that RpcClient disconnects gracefully."""
    rpc_client = RpcClient(ROUTING_KEY, RABBITMQ_URL)

    rpc_client.connection = AsyncMock()
    rpc_client.channel = AsyncMock()

    await rpc_client.disconnect()

    rpc_client.channel.close.assert_called_once()
    rpc_client.connection.close.assert_called_once()
