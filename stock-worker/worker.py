import os
import uuid

import redis
import asyncio

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis
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
        db.set(key, int(item_stock)) # TODO: Use WATCH
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content=f"Item: {item_id} stock updated to: {item_stock}",
        is_json=False
    )

# TODO: make this faster? should be all or nothing
def subtract_bulk(items_amounts: dict[str, int]):
    rollback_actions = []
    try:
        for item_id, amount in items_amounts.items():
            remove_stock(item_id=item_id, amount=amount)
            rollback_actions.append((item_id, amount))
        return create_response_message(content={"msg": "Bulk subtract successful"}, is_json=True)

    except Exception as e:
        for item_id, amount in rollback_actions:
            add_stock(item_id=item_id, amount=amount)
        return create_response_message(content={"msg": f"Error: {str(e)}"}, is_json=True)


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
    elif message_type == MsgType.SUBTRACT_BULK:
        return subtract_bulk(items_amounts=content["items_amounts"])

    return create_error_message(error=f"Unknown message type: {message_type}")

if __name__ == "__main__":
    asyncio.run(consume_events(process_message))
