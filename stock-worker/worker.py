import os
import uuid

import redis
import asyncio
import logging

from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, release_locks, attempt_acquire_locks
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
        with db.pipeline() as pipe:
            for key, value in price_kv_pairs.items():
                pipe.set(key, value)

            for key, value in stock_kv_pairs.items():
                pipe.set(key, value)
            pipe.execute()
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

def check_and_validate_stock(item_dict: dict[str, int]) :
    """Validates if there is enough stock for all items in the dictionary."""
    for item_id, amount in item_dict.items():
        stock_key = f"{item_id}-stock"
        current_stock = db.get(stock_key)
        if current_stock is None or int(current_stock) < amount:
            return create_error_message(f"Not enough stock for item: {item_id}")
    # No errors, stock is sufficient

def update_stock_in_db(item_dict: dict[str, int], operation_func):
    """Updates stock in the database via pipeline for the specified operation."""
    try:
        with db.pipeline() as pipe:
            for item_id, amount in item_dict.items():
                operation_func(pipe, f"{item_id}-stock", amount)
            pipe.execute()
    except redis.exceptions.RedisError as e:
        return create_error_message(f"Redis error: {e}")

def subtract_bulk(item_dict: dict[str, int]):
    """Attempts to decrement stock safely while locking only relevant keys."""
    stock_keys = [f"{item_id}-stock" for item_id in item_dict.keys()]
    # Attempt to acquire locks
    if not (acquired_locks := attempt_acquire_locks(db, stock_keys)):
        return create_error_message("Failed to acquire necessary locks after multiple retries")
    try:
        # Fetch current stock levels and check availability
        if validation_error := check_and_validate_stock(item_dict):
            return validation_error
        # If sufficient stock is available, update in a pipeline
        if updating_error := update_stock_in_db(item_dict, lambda p, k, a: p.decrby(k, a)):
            return updating_error
        return create_response_message(content="All items' stock successfully updated for the saga.", is_json=False)
    finally:
        release_locks(db, acquired_locks)

def add_bulk(item_dict: dict[str, int]):
    # Use a pipeline to send multiple INCRBY commands in a batch
    if updating_error :=update_stock_in_db(item_dict, lambda p, k, a: p.incrby(k, a)):
        return updating_error
    return create_response_message(
        content="Stock successfully restored for the saga reversal.",
        is_json=False
    )

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
        return add_stock(item_id=content["item_id"], amount=content["total_cost"])
    elif message_type == MsgType.SUBTRACT:
        return remove_stock(item_id=content["item_id"], amount=content["total_cost"])
    elif message_type == MsgType.SAGA_INIT:
        return subtract_bulk(item_dict=dict(content["items"]))
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        return add_bulk(item_dict=dict(content["items"]))
    elif message_type == MsgType.SAGA_PAYMENT_REVERSE:
        return None # Ignore

    return create_error_message(error=f"Unknown message type: {message_type}")


def get_message_response_type(message: AbstractIncomingMessage) -> str:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_STOCK_RESPONSE

if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=lambda message: None
    ))
