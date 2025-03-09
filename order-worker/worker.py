import asyncio
import time
import logging
import os
import random
import uuid
from collections import defaultdict

import redis
import requests
from aio_pika.abc import AbstractIncomingMessage

from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events, OrderWorkerClient
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis, acquire_locks, release_locks
from model import OrderValue

GATEWAY_URL = os.environ['GATEWAY_URL']
db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))
order_worker_client: OrderWorkerClient = None

SAGA_TIMEOUT = 30

def get_order_from_db(order_id: str) -> OrderValue | None:
    """
    Gets an order from DB via id. Is NONE, if it doesn't exist

    :param order_id: The order ID
    :return: The order as a `OrderValue` object, none if it doesn't exist
    """
    entry: bytes = db.get(order_id)
    return msgpack.decode(entry, type=OrderValue) if entry else None

def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(
        content = {'order_id': key},
        is_json=True
    )

def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
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
        db.mset(kv_pairs)
    except redis.exceptions.RedisError as e:
        return create_error_message(
            error=str(e)
        )

    return create_response_message(
        content = {"msg": "Batch init for orders successful"},
        is_json=True
    )

def find_order(order_id: str):
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
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
        order_entry: OrderValue = get_order_from_db(order_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if order_entry is None:
        return create_error_message(f"Order: {order_id} not found")

    item_reply = requests.get(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        return create_error_message(
            error = str(f"Item: {item_id} does not exist!")
        )
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
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
        order_entry: OrderValue = get_order_from_db(order_id)
        if order_entry is None:
            return create_error_message(f"Order: {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    call_msg = {
        'item_dict': items_quantities,
        'amount': order_entry.total_cost,
        'user_id': order_entry.user_id
    }

    saga_key = f"saga-{correlation_id}"
    db.hsetnx(saga_key, 'order_id', order_id)
    db.hsetnx(saga_key, 'reply_to', reply_to)





    await order_worker_client.order_fanout_call(
        msg=call_msg,
        msg_type=MsgType.SAGA_INIT,
        correlation_id=correlation_id,
        reply_to=os.environ['ROUTE_KEY']
    )

    return None



async def saga_processing(correlation_id: str, stock_status: int, payment_status: int, order_id: str):
    try:
        logging.error(f" order id: {order_id}")
        order_entry = get_order_from_db(order_id)
        if order_entry is None:
            logging.error("WHY THE FUCK ARE WE GOING HERE?!??!?!??!?!?!!")
            return create_error_message(f"Order for saga confirmation: {order_id} not found")


        if payment_status == 1 and stock_status == 1:
            # Both payment and stock successful, complete order

            order_entry.paid = True

            try:
                db.set(order_id, msgpack.encode(order_entry))  # Store the updated order entry
            except redis.exceptions.RedisError as e:

                return create_error_message(error=str(e))


            return create_response_message(
                content="Checkout successful!",
                is_json=False
            )
        elif payment_status == 1:
            # Payment successful but stock failed, reverse payment
            await order_worker_client.order_fanout_call(
                msg={"item_dict": order_entry.items, "amount": order_entry.total_cost, "user_id": order_entry.user_id},
                msg_type=MsgType.SAGA_PAYMENT_REVERSE,
                correlation_id=correlation_id,
                reply_to=None
            )
            return create_error_message(error="Stock service failed in the SAGA, payment reversed.")


        elif stock_status == 1:
            # Stock successful but payment failed, reverse stock
            logging.error("IMPORTANT CHECKPOINT")
            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in order_entry.items:
                items_quantities[item_id] += quantity

            await order_worker_client.order_fanout_call(
                msg={"item_dict": items_quantities, "amount": order_entry.total_cost, "user_id": order_entry.user_id},
                msg_type=MsgType.SAGA_STOCK_REVERSE,
                correlation_id=correlation_id,
                reply_to=None
            )
            return create_error_message(error="Payment service failed in the SAGA, stock reversed.")

        # If neither payment nor stock are successful, handle failure
        return create_error_message(error="Both payment and stock services failed in the SAGA.")

    except Exception as e:
        return create_error_message(error=str(e))


async def check_saga_completion(correlation_id):
    MAX_RETRIES = 5
    RETRY_DELAY=0.2
    """Check if both payment and stock responses are available atomically using a single lock with retries."""
    # Lock key for the saga correlation ID
    lock_key = f"saga-{correlation_id}"

    # Retry logic for acquiring the lock
    for attempt in range(MAX_RETRIES):
        lock = acquire_locks(db, [lock_key])
        if lock is not None:
            # Lock acquired successfully, proceed with checking saga
            try:
                # Now that we have acquired the lock, check the status in the hash
                result = db.hgetall(f"saga-{correlation_id}")

                # Check if both payment and stock responses are available
                if result and b"payment" in result and b"stock" in result:
                    payment_status = int(result[b"payment"])
                    stock_status = int(result[b"stock"])
                    order_id = result[b"order_id"].decode("utf-8")
                    processing_result = await saga_processing(
                        correlation_id=correlation_id,
                        stock_status=stock_status,
                        payment_status=payment_status,
                        order_id=order_id,
                    )

                    return processing_result

                return None  # Both statuses are not available yet
            finally:
                # Release the lock after processing
                release_locks(db, [lock_key])

        # If lock is not acquired, wait before retrying
        await asyncio.sleep(RETRY_DELAY)

    # If lock could not be acquired after max_retries, return None
    return None


async def handle_payment_saga_response(status_code, correlation_id):
    db.hsetnx(f"saga-{correlation_id}", "payment", "1" if status_code == 200 else "0")
    return await check_saga_completion(correlation_id)

async def handle_stock_saga_response(status_code, correlation_id):
    db.hsetnx(f"saga-{correlation_id}", "stock", "1" if status_code == 200 else "0")
    return await check_saga_completion(correlation_id)

async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return create_order(user_id=content['user_id'])
    elif message_type == MsgType.BATCH_INIT:
        return batch_init_users(n=content['n'], n_items=content['n_items'], n_users=content['n_users'], item_price=content['item_price'])
    elif message_type == MsgType.FIND:
        return find_order(order_id=content['order_id'])
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

def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    if message.type in (MsgType.SAGA_STOCK_RESPONSE, MsgType.SAGA_PAYMENT_RESPONSE):
        reply_to = db.hget(f"saga-{message.correlation_id}", 'reply_to')
        reply_to = reply_to.decode('utf-8') if reply_to else None
        return reply_to

    return None

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



