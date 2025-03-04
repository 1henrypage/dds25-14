import asyncio
import logging
import os
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis, get_from_db
from model import OrderValue

GATEWAY_URL = os.environ['GATEWAY_URL']


db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

def get_order_from_db(order_id: str) ->  OrderValue | None:
    return get_from_db(
        db=db,
        key=order_id,
        value_type=OrderValue
    )

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



def add_item(order_id: str, item_id: str, quantity: int):
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

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        requests.post(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

def checkout(order_id: str):
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if order_entry is None:
        return create_error_message(f"Order: {order_id} not found")
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = requests.post(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            return create_error_message(
                error = f'Out of stock on item_id: {item_id}'
            )
        removed_items.append((item_id, quantity))
    user_reply = requests.post(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        return create_error_message(
            error = "User out of credit"
        )
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError as e:
        return create_error_message(
            error = str(e)
        )

    return create_response_message(
        content = "Checkout successful",
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
        return create_order(user_id=content['user_id'])
    elif message_type == MsgType.BATCH_INIT:
        return batch_init_users(n=content['n'], n_items=content['n_items'], n_users=content['n_users'], item_price=content['item_price'])
    elif message_type == MsgType.FIND:
        return find_order(order_id=content['order_id'])
    elif message_type == MsgType.ADD:
        return add_item(order_id=content['order_id'], item_id=content['item_id'], quantity=content['quantity'])
    elif message_type == MsgType.CHECKOUT:
        return checkout(content['order_id'])

    return create_error_message(
        error = str(f"Unknown message type: {message_type}")
    )


if __name__ == "__main__":
    asyncio.run(consume_events(process_message=process_message))
