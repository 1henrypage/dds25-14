import asyncio
import os
import random
import uuid
from saga import check_saga_completion
import redis
from aio_pika.abc import AbstractIncomingMessage
import aiohttp

from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events, OrderWorkerClient
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis
from model import OrderValue

GATEWAY_URL = os.environ['GATEWAY_URL']
db: redis.asyncio.cluster.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))
order_worker_client: OrderWorkerClient = None

SAGA_TIMEOUT = 30

async def get_order_from_db(order_id: str) -> OrderValue | None:
    """
    Gets an order from DB via id. Is NONE, if it doesn't exist

    :param order_id: The order ID
    :return: The order as a `OrderValue` object, none if it doesn't exist
    """
    entry: bytes = await db.get(order_id)
    return msgpack.decode(entry, type=OrderValue) if entry else None

async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except redis.exceptions.RedisError as e:
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

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        async with db.pipeline() as pipe:
            for key, value in kv_pairs.items():
                pipe.set(key, value)
            await pipe.execute()
    except redis.exceptions.RedisError as e:
        return create_error_message(
            error=str(e)
        )

    return create_response_message(
        content = {"msg": "Batch init for orders successful"},
        is_json=True
    )

async def find_order(order_id: str):
    try:
        order_entry: OrderValue = await get_order_from_db(order_id)
    except redis.exceptions.RedisError as e:
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


async def add_item(order_id: str, item_id: str, quantity: int):
    try:
        order_entry: OrderValue = await get_order_from_db(order_id)
        if order_entry is None:
            return create_error_message(f"Order: {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{GATEWAY_URL}/stock/find/{item_id}") as item_reply:
            if item_reply.status != 200:
                return create_error_message(
                    error=f"Item: {item_id} does not exist!"
                )
            item_json: dict = await item_reply.json()

    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError as e:
        return create_error_message(
            error = str(e)
        )

    return create_response_message(
        content=f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        is_json=False
    )


async def checkout(order_id: str, correlation_id: str, reply_to: str):
    try:
        order_entry: OrderValue = await get_order_from_db(order_id)
        if order_entry is None:
            return create_error_message(f"Order: {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    await db.hset(f"saga-{correlation_id}", mapping={'order_id': order_id, 'reply_to': reply_to})

    await order_worker_client.order_fanout_call(
        msg=order_entry,
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
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return await create_order(user_id=content['user_id'])
    elif message_type == MsgType.BATCH_INIT:
        return await batch_init_users(n=content['n'], n_items=content['n_items'], n_users=content['n_users'], item_price=content['item_price'])
    elif message_type == MsgType.FIND:
        return await find_order(order_id=content['order_id'])
    elif message_type == MsgType.ADD:
        return await add_item(order_id=content['order_id'], item_id=content['item_id'], quantity=content['quantity'])
    elif message_type == MsgType.CHECKOUT:
        return await checkout(content['order_id'], message.correlation_id, reply_to=message.reply_to)
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
        exchange_key=os.environ['ORDER_OUTBOUND_EXCHANGE_IDENTIFIER']
    ).connect()

    await consume_events(
        process_message=process_message,
        get_message_response_type=lambda message: None,
        get_custom_reply_to=get_custom_reply_to
    )

if __name__ == "__main__":
    asyncio.run(main())



