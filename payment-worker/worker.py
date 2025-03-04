import asyncio
import os
import uuid

import redis

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis

db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

def create_user():
    key: str = str(uuid.uuid4())
    try:
        db.set(key, 0)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(
        content={'user_id':key},
        is_json=True
    )

def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, int] = {f"{i}": starting_money for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    return create_response_message(
        content={"msg": "Batch init for users successful"},
        is_json=True
    )

def find_user(user_id: str):
    try:
        credit = db.get(user_id)
        if credit is None:
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    credit = int(credit)

    return create_response_message(
        content={
            "user_id": user_id,
            "credit": credit
        },
        is_json=True
    )

def add_credit(user_id: str, amount: int):
    try:
        if not db.exists(user_id):
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    # update credit, serialize and update database
    try:
        new_balance = db.incrby(user_id,amount=amount)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content=f"User: {user_id} credit updated to: {new_balance}",
        is_json=False
    )


def remove_credit(user_id: str, amount: int):
    try:
        credit = db.get(user_id)
        if credit is None:
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    credit = int(credit)

    # update credit, serialize and update database
    credit -= int(amount)

    if credit < 0:
        return create_error_message(
            error=f"User: {user_id} credit cannot get reduced below zero!"
        )
    try:
        db.set(user_id, credit)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content=f"User: {user_id} credit updated to: {credit}",
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
        return create_user()
    elif message_type == MsgType.BATCH_INIT:
        return batch_init_users(
            n=content['n'],
            starting_money=content['starting_money']
        )
    elif message_type == MsgType.FIND:
        return find_user(user_id=content['user_id'])
    elif message_type == MsgType.ADD:
        return add_credit(user_id=content['user_id'], amount=content['amount'])
    elif message_type == MsgType.SUBTRACT:
        return remove_credit(user_id=content['user_id'], amount=content['amount'])

    return create_error_message(
        error=f"Unknown message type: {message_type}"
    )



if __name__ == "__main__":
    asyncio.run(consume_events(process_message))
