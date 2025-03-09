import os
import uuid

import redis
import asyncio
import logging

from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
import time
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, acquire_locks, release_locks
from common.request_utils import create_error_message, create_response_message

db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))


def create_item(price: int):
    key = str(uuid.uuid4())
    try:
        db.set(key + "-stock", 0)
        db.set(key + "-price", int(price))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(
        content={'item_id':key},
        is_json=True
    )


def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)

    # Create separate keys for stock and price for each user
    price_kv_pairs: dict[str, int] = {f"user:{i}-price": item_price for i in range(n)}
    stock_kv_pairs: dict[str, int] = {f"user:{i}-stock": starting_stock for i in range(n)}

    try:
        db.mset(price_kv_pairs)
        db.mset(stock_kv_pairs)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content={"msg": "Batch init for stock successful"},
        is_json=True
    )

def find_item(item_id: str):
    try:
        item_price = db.get(item_id + "-price")
        item_stock = db.get(item_id + "-stock")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if item_price is None or item_stock is None:
        return create_error_message(f"Item: {item_id} not found")

    return create_response_message(
        content={
            "stock": int(item_stock),
            "price": int(item_price)
        },
        is_json=True
    )

def add_stock(item_id: str, amount: int):
    try:
        if not db.exists(item_id + "-stock"):
            return create_error_message(f"Item: {item_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    # update stock, serialize and update database
    try:
        new_stock = int(db.incrby(item_id + "-stock", amount=amount))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content = f"Item: {item_id} stock updated to: {new_stock}",
        is_json=False
    )

def remove_stock(item_id: str, amount: int):
    key = item_id + "-stock"
    try:
        item_stock = db.get(key)
        if item_stock is None:
            return create_error_message(f"Item: {item_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    item_stock = int(item_stock)

    # update stock, serialize and update database
    item_stock -= int(amount)
    if item_stock < 0:
        return create_error_message(
            error=f"Item: {item_id} stock cannot get reduced below zero!"
        )

    try:
        db.set(key, int(item_stock))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content=f"Item: {item_id} stock updated to: {item_stock}",
        is_json=False
    )

def remove_stock_for_saga(item_dict: dict[str, int]):
    """Attempts to decrement stock safely while locking only relevant keys."""
    stock_keys = [f"{item_id}-stock" for item_id in item_dict.keys()]

    # TODO RETRY LOGIC MAKE MORE CLEVER
    MAX_RETRIES = 3
    RETRY_DELAY = 0.1

    for attempt in range(MAX_RETRIES):
        acquired_locks = acquire_locks(db, stock_keys)
        if not acquired_locks:
            time.sleep(RETRY_DELAY)
            continue  # Retry acquiring locks

        try:
            # Fetch current stock levels and check availability
            for item_id, amount in item_dict.items():
                stock_key = f"{item_id}-stock"
                current_stock = db.get(stock_key)

                # If stock is unavailable or insufficient, rollback
                if current_stock is None or int(current_stock) < amount:
                    # Rollback by releasing locks and returning error
                    return create_error_message("Not enough stock for one or more items")


            pipeline = db.pipeline()
            for item_id, amount in item_dict.items():
                pipeline.decrby(f"{item_id}-stock", amount)

            pipeline.execute()


            return create_response_message(
                content="All items' stock successfully updated for the saga.",
                is_json=False
            )

        except redis.exceptions.RedisError as e:
            return create_error_message(f"Redis error: {str(e)}")

        finally:
            release_locks(db, acquired_locks)

    return create_error_message("Failed to acquire necessary locks after multiple retries")

def reverse_stock_for_saga(item_dict: dict[str, int]):
    try:
        # Use a pipeline to send multiple INCRBY commands in a batch
        with db.pipeline() as pipe:
            for item_id, amount in item_dict.items():
                key = item_id + "-stock"
                pipe.incrby(key, amount)

            # Execute the batch commands
            pipe.execute()

        return create_response_message(
            content="Stock successfully restored for the saga reversal.",
            is_json=False
        )

    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))


async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return create_item(price=content["price"])
    elif message_type == MsgType.BATCH_INIT:
        return batch_init_users(n=content["n"], starting_stock=content["starting_stock"], item_price=content["item_price"])
    elif message_type == MsgType.FIND:
        return find_item(item_id=content["item_id"])
    elif message_type == MsgType.ADD:
        return add_stock(item_id=content["item_id"], amount=content["amount"])
    elif message_type == MsgType.SUBTRACT:
        return remove_stock(item_id=content["item_id"], amount=content["amount"])
    elif message_type == MsgType.SAGA_INIT:
        return remove_stock_for_saga(item_dict=content["item_dict"])
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        logging.error("SAGA REVERSAL INCOMING ON STOCK ")
        return reverse_stock_for_saga(item_dict=content["item_dict"])
    elif message_type == MsgType.SAGA_PAYMENT_REVERSE:
        return None # Ignore

    return create_error_message(error=f"Unknown message type: {message_type}")


def get_message_response_type(message: AbstractIncomingMessage) -> str:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_STOCK_RESPONSE

    return None

if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=lambda message: None
    ))
