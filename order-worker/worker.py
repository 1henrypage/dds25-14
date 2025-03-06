import asyncio
import logging
import os
import random
import uuid
from collections import defaultdict, Counter

import redis
import requests
from flask import request

from msgspec import msgpack

from common.ForgetfulClient import ForgetfulClient
from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis
from model import OrderValue
from orchestrator import SagaOrchestrator, SagaStep, Outcome

GATEWAY_URL = os.environ['GATEWAY_URL']

forgetful_client = ForgetfulClient(rabbitmq_url=os.environ['RABBITMQ_URL'], routing_key=os.environ['ROUTE_KEY'])
db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

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
    order_value = OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
    try:
        db.set(key, msgpack.encode(order_value))
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
        db.set(order_id, msgpack.encode(order_entry)) # TODO: use WATCH
    except redis.exceptions.RedisError as e:
        return create_error_message(
            error = str(e)
        )

    return create_response_message(
        content=f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        is_json=False
    )


def payment_step(order_entry):
    asyncio.create_task(forgetful_client.call(msg={"user_id": order_entry.user_id, "amount": order_entry.total_cost}, msg_type=MsgType.SUBTRACT))
    # user_reply = requests.post(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    # if user_reply.status_code != 200:
    #     return create_error_message(error = "User out of credit")
    # return create_response_message(content = "Payment successful", is_json=False)

def rollback_payment(order_entry: OrderValue):
    asyncio.create_task(forgetful_client.call(msg={"user_id": order_entry.user_id, "amount": order_entry.total_cost}, msg_type=MsgType.ADD))
    # user_reply = requests.post(f"{GATEWAY_URL}/payment/add_funds/{order_entry.user_id}/{order_entry.total_cost}")
    # if user_reply.status_code != 200:
    #     return create_error_message(error="Error when rolling back payment")
    # return create_response_message(content="Payment rollback successful", is_json=False)

def payment_step_db(order_entry, order_id):
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(content = "Payment db successful", is_json=False)

def rollback_payment_db(order_entry, order_id):
    order_entry.paid = False
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(content = "Payment rollback db successful", is_json=False)

def subtract_stock_step(order_entry: OrderValue):
    asyncio.create_task(forgetful_client.call(msg=dict(Counter(dict(order_entry.items))), msg_type=MsgType.SUBTRACT_BULK))
    # stock_reply = requests.post(f"{GATEWAY_URL}/stock/subtract-bulk", json=Counter(dict(order_entry.items)))
    # if stock_reply.status_code != 200:
    #     return create_error_message(f'Out of stock on item_id')  # TODO get item id that failed'
    # return create_response_message(content="Stock subtract successful", is_json=False)

def rollback_stock(order_entry: OrderValue):
    asyncio.create_task(forgetful_client.call(msg=dict(Counter(dict(order_entry.items))), msg_type=MsgType.ADD_BULK))
    # stock_reply = requests.post(f"{GATEWAY_URL}/stock/add-bulk", json=Counter(dict(order_entry.items)))
    # if stock_reply.status_code != 200:
    #     return create_error_message(error=f'Failed to rollback stock: {stock_reply}')
    # return create_response_message(content="Stock rollback successful", is_json=False)

def checkout(order_id: str, request_id):
    """Orchestrates the checkout process using the Saga pattern."""
    try:
        order_entry: OrderValue = get_order_from_db(order_id)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    if order_entry is None:
        return create_error_message(f"Order: {order_id} not found")

    saga_steps = [
        SagaStep(name="Payment",
                 action=lambda: payment_step(order_entry),
                 compensating_action=rollback_payment(order_entry)),
        SagaStep(name="Payment-DB",
                 action=lambda: payment_step_db(order_entry, order_id),
                 compensating_action=lambda: rollback_payment_db(order_entry, order_id)),
        SagaStep(name="Stock",
                 action=lambda: subtract_stock_step(order_entry),
                 compensating_action=lambda: rollback_stock(order_entry))
    ]

    saga = SagaOrchestrator(request_id, saga_steps, redis_client=db, forgetful_client=forgetful_client)
    saga.execute()

    # if result == Outcome.SUCCESS:
    #     return create_response_message(content="Checkout successful", is_json=False)

def checkout_reply(saga, content):
    result, res = saga.handle_response(content)
    if result == Outcome.FAILURE:
        return create_error_message(error=res)
    if result == Outcome.WAITING:

    return create_response_message(content="Checkout successful", is_json=False)

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
        checkout(content['order_id'])
    elif message_type == MsgType.CHECKOUT_REPLY:
        # TODO: make orchestrator handle this with handle_response (technically it should be in the request which saga this is)
        result, res = content['saga'].handle_response(content)
        if result == Outcome.FAILURE:
            return create_error_message(error=res)
        elif result == Outcome.SUCCESS:
            return create_response_message(content="Checkout successful", is_json=False)
    else:
        return create_error_message(
            error = str(f"Unknown message type: {message_type}")
        )

async def main():
    await forgetful_client.connect()
    await consume_events(process_message=process_message)
    await forgetful_client.close()

if __name__ == "__main__":
    asyncio.run(main())
