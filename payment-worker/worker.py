import asyncio
import os
import uuid

import redis

from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis, get_from_db

db: redis.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

def create_user():
    key = str(uuid.uuid4())
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
        user_entry = db.get(user_id)
        if user_entry is None:
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    user_entry = int(user_entry)

    return create_response_message(
        content={
            "user_id": user_id,
            "credit": user_entry
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
        user_entry= db.get(user_id)
        if user_entry is None:
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    user_entry = int(user_entry)

    # update credit, serialize and update database
    user_entry -= int(amount)

    if user_entry < 0:
        return create_error_message(
            error=f"User: {user_id} credit cannot get reduced below zero!"
        )
    try:
        db.set(user_id, user_entry)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content=f"User: {user_id} credit updated to: {user_entry}",
        is_json=False
    )


def process_message(message_type, content):
    if message_type == 'create_user':
        return create_user()
    elif message_type == 'batch_init':
        return batch_init_users(
            n=content['n'],
            starting_money=content['starting_money']
        )
    elif message_type == 'find_user':
        return find_user(user_id=content['user_id'])
    elif message_type == 'add_funds':
        return add_credit(user_id=content['user_id'], amount=content['amount'])
    elif message_type == 'pay':
        return remove_credit(user_id=content['user_id'], amount=content['amount'])

    return create_error_message(
        error=f"Unknown message type: {message_type}"
    )



if __name__ == "__main__":
    asyncio.run(consume_events(process_message))
