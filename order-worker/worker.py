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
from common.idempotency_utils import cache_response_with_db, cache_response_with_pipe, is_duplicate_message
from model import OrderValue

GATEWAY_URL = os.environ['GATEWAY_URL']
db: redis.asyncio.cluster.RedisCluster = configure_redis()
order_worker_client: OrderWorkerClient = None

SAGA_TIMEOUT = 2700

async def create_order(user_id: str):
    key = str(uuid.uuid4())
    try:
        await db.hset(key, mapping={
            "paid": 0,
            "user_id": user_id,
            "total_cost": 0
        })
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    return create_response_message(
        content = {'order_id': key},
        is_json=True
    )

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

    kv_pairs = {str(i): generate_entry() for i in range(n)}

    try:
        async with db.pipeline() as pipe:
            for key, value in kv_pairs.items():
                pipe.hset(key, mapping={k: v for k, v in value.items()})
                pipe.rpush(f"{key}:items", str(random.randint(0, n_items - 1)), 1, str(random.randint(0, n_items - 1)), 1)
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(error=str(e))

    return create_response_message(
        content = {"msg": "Batch init for orders successful"},
        is_json=True
    )

async def find_order(order_id: str):
    try:
        order_entry: OrderValue = await get_order_from_db(db, order_id)
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    if order_entry is None:
        return create_error_message(f"Order: {order_id} not found")

    return create_response_message(
        content = {
                "order_id": order_id,
                "paid": order_entry.paid,
                "items": order_entry.items,
                "user_id": order_entry.user_id,
                "total_cost": order_entry.total_cost
        },
        is_json=True
    )


async def add_item(order_id: str, item_id: str, quantity: int, correlation_id: str, message_type: str):

    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response

    try:
        order_entry: OrderValue = await get_order_from_db(db, order_id)
        if order_entry is None:
            await cache_response_with_db(correlation_id, message_type, False, db)
            return create_error_message(f"Order: {order_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{GATEWAY_URL}/stock/find/{item_id}/priority") as item_reply:
            if item_reply.status != 200:
                await cache_response_with_db(correlation_id,message_type,False,db)
                return create_error_message(
                    error=f"Item: {item_id} does not exist!"
                )
            item_json: dict = await item_reply.json()


    try:
        async with db.pipeline() as pipe:
            pipe.rpush(f"{order_id}:items", item_id, quantity)
            pipe.hincrby(order_id, "total_cost", int(quantity * item_json["price"]))
            cache_response_with_pipe(pipe,correlation_id,message_type,True)
            result = await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        await cache_response_with_db(db,correlation_id,message_type,False)
        return create_error_message(
            error = str(e)
        )

    return create_response_message(
        content=f"Item: {item_id} added to: {order_id} price updated to: {result[1]}",
        is_json=False
    )


async def checkout(order_id: str, correlation_id: str, reply_to: str, message_type: str):
    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response
    try:
        order_entry: OrderValue = await get_order_from_db(db, order_id)
        if order_entry is None:
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(f"Order: {order_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    try:
        async with db.pipeline() as pipe:
            pipe.hset(f"saga-{correlation_id}", mapping={'order_id': order_id, 'reply_to': reply_to})
            pipe.expire(f"saga-{correlation_id}", SAGA_TIMEOUT)
            cache_response_with_pipe(pipe,correlation_id,message_type,True)
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        await cache_response_with_db(db,correlation_id,message_type,False)
        return create_error_message(
            error = str(e)
        )

    await asyncio.gather(
        order_worker_client.call_with_route_no_reply(
            msg={"user_id": order_entry.user_id, "total_cost": order_entry.total_cost, "items": order_entry.items},
            msg_type=MsgType.SAGA_INIT,
            routing_key=os.environ['PAYMENT_ROUTE'],
            reply_to=os.environ['ROUTE_KEY'],
            correlation_id=correlation_id
        ),
        order_worker_client.call_with_route_no_reply(
            msg={"user_id": order_entry.user_id, "total_cost": order_entry.total_cost, "items": order_entry.items},
            msg_type=MsgType.SAGA_INIT,
            routing_key=os.environ['STOCK_ROUTE'],
            reply_to=os.environ['ROUTE_KEY'],
            correlation_id=correlation_id
        )
    )


async def handle_payment_saga_response(status_code, correlation_id):
    await db.hsetnx(f"saga-{correlation_id}", "payment", "1" if status_code == 200 else "0")
    return await check_saga_completion(db, order_worker_client, correlation_id)

async def handle_stock_saga_response(status_code, correlation_id):
    await db.hsetnx(f"saga-{correlation_id}", "stock", "1" if status_code == 200 else "0")
    return await check_saga_completion(db, order_worker_client, correlation_id)

async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return await create_order(user_id=content['user_id'])
    elif message_type == MsgType.BATCH_INIT:
        return await batch_init_users(n=content['n'], n_items=content['n_items'], n_users=content['n_users'], item_price=content['item_price'])
    elif message_type == MsgType.FIND:
        return await find_order(order_id=content['order_id'])
    elif message_type == MsgType.ADD:
        return await add_item(order_id=content['order_id'], item_id=content['item_id'], quantity=content['quantity'], correlation_id=message.correlation_id, message_type=message_type)
    elif message_type == MsgType.CHECKOUT:
        return await checkout(content['order_id'], message.correlation_id, reply_to=message.reply_to, message_type=message_type)
    elif message_type == MsgType.SAGA_PAYMENT_RESPONSE:
        return await handle_payment_saga_response(status_code=content['status'], correlation_id=message.correlation_id)
    elif message_type == MsgType.SAGA_STOCK_RESPONSE:
        return await handle_stock_saga_response(status_code=content['status'], correlation_id=message.correlation_id)

    return create_error_message(
        error = str(f"Unknown message type: {message_type}")
    )

async def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    if message.type in (MsgType.SAGA_STOCK_RESPONSE, MsgType.SAGA_PAYMENT_RESPONSE):
        reply_to = await db.hget(f"saga-{message.correlation_id}", 'reply_to')
        reply_to = reply_to.decode('utf-8') if reply_to else None
        return reply_to

async def main():
    global order_worker_client
    order_worker_client = await OrderWorkerClient(
        rabbitmq_url=os.environ['RABBITMQ_URL'],
    ).connect()

    await consume_events(
        process_message=process_message,
        get_message_response_type=lambda message: None,
        get_custom_reply_to=get_custom_reply_to
    )

if __name__ == "__main__":
    asyncio.run(main())



