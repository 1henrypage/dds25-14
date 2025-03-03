import asyncio
import os
import uuid

import redis

from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, get_from_db
from common.request_utils import create_error_message, create_response_message

from model import StockValue

db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

def get_item_from_db(item_id: str) -> StockValue | None:
    return get_from_db(
        db=db,
        key=item_id,
        value_type=StockValue
    )

def create_item(price: int):
    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
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
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content={"msg": "Batch init for stock successful"},
        is_json=True
    )

def find_item(item_id: str):
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if item_entry is None:
        return create_error_message(f"Item: {item_id} not found")

    return create_response_message(
        content={
            "stock": item_entry.stock,
            "price": item_entry.price
        },
        is_json=True
    )

def add_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if item_entry is None:
        return create_error_message(f"Item: {item_id} not found")

    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content = f"Item: {item_id} stock updated to: {item_entry.stock}",
        is_json=False
    )


def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if item_entry is None:
        return create_error_message(f"Item: {item_id} not found")
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        return create_error_message(
            error=f"Item: {item_id} stock cannot get reduced below zero!"
        )
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError as e :
        return create_error_message(str(e))

    return create_response_message(
        content=f"Item: {item_id} stock updated to: {item_entry.stock}",
        is_json=False
    )


def process_message(message_type, content):
    """
    Based on message type it delegates the call to a specific function.

    :param message_type: The message type
    :param content: The actual message content
    :return: Processed response
    """
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

    return create_error_message(error=f"Unknown message type: {message_type}")

if __name__ == "__main__":
    asyncio.run(consume_events(process_message))
