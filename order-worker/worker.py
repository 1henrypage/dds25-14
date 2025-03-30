import asyncio
import os
import random
import uuid
from saga import check_saga_completion, get_order_from_db
import redis
from aio_pika.abc import AbstractIncomingMessage
import aiohttp

from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events, OrderWorkerClient
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis
from common.idempotency_utils import is_duplicate_message, make_response_key
from model import OrderValue

GATEWAY_URL = os.environ['GATEWAY_URL']
db: redis.asyncio.cluster.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))
order_worker_client: OrderWorkerClient = None

SAGA_TIMEOUT = 30

async def recover_pending_operations():
    pending_keys = await db.keys("pending:*")
    for pending_key in pending_keys:
        try:
            _, correlation_id, message_type = pending_key.decode().split(":")
            entity_key = await db.get(pending_key)
            if not entity_key:
                continue

            order_id = entity_key.decode().strip("{}")
            response_key = make_response_key(order_id, correlation_id, message_type)

            if await db.exists(response_key):
                await db.delete(pending_key)
                continue

            print(f"[RECOVERY] Message {correlation_id} of type {message_type} with order {order_id} may need replay.")
        except Exception as e:
            print(f"[RECOVERY ERROR] While handling {pending_key}: {e}")

async def create_order(user_id: str, correlation_id: str, message_type: str):
    # Generate a random numeric ID between 0 and 1000000
    order_id = str(random.randint(0, 1000000))
    response_key = make_response_key(order_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        async with db.pipeline() as pipe:
            # Set pending key
            pipe.set(pending_key, order_id)
            # Create order hash
            pipe.hset(order_id, mapping={
                "paid": 0,
                "user_id": user_id,
                "total_cost": 0
            })
            # Set response
            response = create_response_message(content={'order_id': order_id}, is_json=True)
            pipe.set(response_key, msgpack.encode(response))
            # Delete pending key
            pipe.delete(pending_key)
            # Execute all commands
            await pipe.execute()

        return response
    except Exception as e:
        return create_error_message(str(e))

async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> dict:
        return {
            "paid": 0,
            "user_id": str(random.randint(0, n_users - 1)),
            "total_cost": 2 * item_price,
        }

    try:
        for i in range(n):
            entry = generate_entry()
            # Store with numeric ID
            await db.hset(i, mapping=entry)
            await db.rpush(f"{i}:items", str(random.randint(0, n_items - 1)), 1, str(random.randint(0, n_items - 1)), 1)
    except redis.exceptions.RedisError as e:
        return create_error_message(error=str(e))

    return create_response_message(
        content={"msg": "Batch init for orders successful"},
        is_json=True
    )

async def find_order(order_id: str):
    try:
        # Convert string order_id to integer for Redis lookup
        numeric_order_id = int(order_id)
        order_entry: OrderValue = await get_order_from_db(db, numeric_order_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    except ValueError:
        return create_error_message(f"Invalid order ID format: {order_id}")
    if order_entry is None:
        return create_error_message(f"Order: {order_id} not found")

    return create_response_message(
        content={
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        },
        is_json=True
    )

async def add_item(order_id: str, item_id: str, quantity: int, correlation_id: str, message_type: str):
    response_key = make_response_key(order_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        order_entry: OrderValue = await get_order_from_db(db, order_id)
        if order_entry is None:
            return create_error_message(f"Order: {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{GATEWAY_URL}/stock/find/{item_id}/priority") as item_reply:
            if item_reply.status != 200:
                return create_error_message(error=f"Item: {item_id} does not exist!")
            item_json: dict = await item_reply.json()

    try:
        await db.set(pending_key, order_id)
        response = create_response_message(content=f"Item: {item_id} added to: {order_id}.", is_json=False)
        await db.rpush(f"{order_id}:items", item_id, quantity)
        new_total = await db.hincrby(order_id, "total_cost", int(quantity * item_json["price"]))
        await db.set(response_key, msgpack.encode(response))
        await db.delete(pending_key)

        return create_response_message(content=f"Item: {item_id} added to: {order_id} price updated to: {new_total}", is_json=False)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def checkout(order_id: str, correlation_id: str, reply_to: str):
    try:
        # Convert string order_id to integer for Redis lookup
        numeric_order_id = int(order_id)
        order_entry: OrderValue = await get_order_from_db(db, numeric_order_id)
        if order_entry is None:
            return create_error_message(f"Order: {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    except ValueError:
        return create_error_message(f"Invalid order ID format: {order_id}")

    await db.hset(f"saga-{correlation_id}", mapping={'order_id': order_id, 'reply_to': reply_to})

    await order_worker_client.order_fanout_call(
        msg={"user_id": order_entry.user_id, "total_cost": order_entry.total_cost, "items": order_entry.items},
        msg_type=MsgType.SAGA_INIT,
        correlation_id=correlation_id,
        reply_to=os.environ['ROUTE_KEY']
    )

async def handle_payment_saga_response(status_code, correlation_id):
    await db.hsetnx(f"saga-{correlation_id}", "payment", "1" if status_code == 200 else "0")
    return await check_saga_completion(db, order_worker_client, correlation_id)

async def handle_stock_saga_response(status_code, correlation_id):
    await db.hsetnx(f"saga-{correlation_id}", "stock", "1" if status_code == 200 else "0")
    return await check_saga_completion(db, order_worker_client, correlation_id)

async def process_message(message: AbstractIncomingMessage):
    correlation_id = message.correlation_id
    message_type = message.type

    content = msgpack.decode(message.body)

    # Use correct id anchor based on message type
    if message_type == MsgType.CREATE:
        anchor_id = content['user_id']
    elif message_type in (MsgType.SAGA_STOCK_RESPONSE, MsgType.SAGA_PAYMENT_RESPONSE):
        # For saga responses, use correlation_id as anchor since they don't have order_id
        anchor_id = correlation_id
    elif message_type == MsgType.BATCH_INIT:
        # For batch init, use correlation_id as anchor since it doesn't have order_id
        anchor_id = correlation_id
    else:
        # For all other message types that require order_id
        anchor_id = content['order_id']

    cached_response = await is_duplicate_message(db, anchor_id, correlation_id, message_type)
    if cached_response:
        return cached_response

    if message_type == MsgType.CREATE:
        response = await create_order(user_id=content['user_id'], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.BATCH_INIT:
        response = await batch_init_users(n=content['n'], n_items=content['n_items'], n_users=content['n_users'], item_price=content['item_price'])
    elif message_type == MsgType.FIND:
        response = await find_order(order_id=content['order_id'])
    elif message_type == MsgType.ADD:
        response = await add_item(order_id=content['order_id'], item_id=content['item_id'], quantity=content['quantity'], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.CHECKOUT:
        response = await checkout(content['order_id'], correlation_id, reply_to=message.reply_to)
    elif message_type == MsgType.SAGA_PAYMENT_RESPONSE:
        response = await handle_payment_saga_response(status_code=content['status'], correlation_id=correlation_id)
    elif message_type == MsgType.SAGA_STOCK_RESPONSE:
        response = await handle_stock_saga_response(status_code=content['status'], correlation_id=correlation_id)
    else:
        response = create_error_message(error=f"Unknown message type: {message_type}")

    return response

async def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    if message.type in (MsgType.SAGA_STOCK_RESPONSE, MsgType.SAGA_PAYMENT_RESPONSE):
        reply_to = await db.hget(f"saga-{message.correlation_id}", 'reply_to')
        reply_to = reply_to.decode('utf-8') if reply_to else None
        return reply_to

async def main():
    global order_worker_client
    try:
        # Initialize the order worker client
        order_worker_client = await OrderWorkerClient(
            rabbitmq_url=os.environ['RABBITMQ_URL'],
            exchange_key=os.environ['ORDER_OUTBOUND_EXCHANGE_IDENTIFIER']
        ).connect()

        # First recover any pending operations
        await recover_pending_operations()
        
        # Then start consuming events
        await consume_events(
            process_message=process_message,
            get_message_response_type=lambda message: None,
            get_custom_reply_to=get_custom_reply_to
        )
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Ensure we close connections
        try:
            if order_worker_client:
                await order_worker_client.close()
            await db.close()
        except Exception as e:
            print(f"Error closing connections: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    except Exception as e:
        print(f"Fatal error: {e}")
