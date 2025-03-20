import asyncio
import os
import uuid

import redis
from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis, release_locks, attempt_acquire_locks

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
        with db.pipeline() as pipe:
            for key, value in kv_pairs.items():
                pipe.set(key, value)
            pipe.execute()
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

def check_and_validate_user(user_dict: dict[str, int]) :
    """Validates if there is enough credit for all users in the dictionary."""
    for user_id, amount in user_dict.items():
        current_credit = db.get(user_id)
        if current_credit is None or int(current_credit) < amount:
            return create_error_message(f"Not enough credit for user: {user_id}")
    # No errors, user credit is sufficient

def update_credit_in_db(user_dict: dict[str, int], operation_func):
    """Updates credit in the database via pipeline for the specified operation."""
    try:
        with db.pipeline() as pipe:
            for user_id, amount in user_dict.items():
                operation_func(pipe, user_id, amount)
            pipe.execute()
    except redis.exceptions.RedisError as e:
        return create_error_message(f"Redis error: {e}")

def remove_credit(user_id: str, amount: int):
    try:
        credit = db.get(user_id)
        if credit is None:
            return create_error_message(f"User: {user_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    # Attempt to acquire locks
    if not (acquired_locks := attempt_acquire_locks(db, [user_id])):
        return create_error_message("Failed to acquire necessary locks after multiple retries")
    try:
        user_dict = {user_id: int(amount)}
        # Fetch current user credit and check availability
        if validation_error := check_and_validate_user(user_dict):
            return validation_error
        # If sufficient credit is available, update in a pipeline
        if updating_error := update_credit_in_db(user_dict, lambda p, k, a: p.decrby(k, a)):
            return updating_error
        return create_response_message(
            content=f"User: {user_id} credit updated to: {int(db.get(user_id))}",
            is_json=False
        )
    finally:
        release_locks(db, [user_id])


async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return create_user()
    elif message_type == MsgType.BATCH_INIT:
        return batch_init_users(
            n=content['n'],
            starting_money=content['starting_money']
        )
    elif message_type == MsgType.FIND:
        return find_user(user_id=content['user_id'])
    elif message_type in (MsgType.ADD, MsgType.SAGA_PAYMENT_REVERSE):
        return add_credit(user_id=content['user_id'], amount=content["total_cost"])
    elif message_type == MsgType.SUBTRACT:
        return remove_credit(user_id=content['user_id'], amount=content["total_cost"])
    elif message_type == MsgType.SAGA_INIT:
        return remove_credit(user_id=content['user_id'], amount=content['total_cost'])
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        return None # Ignore

    return create_error_message(
        error=f"Unknown message type: {message_type}"
    )

def get_message_response_type(message: AbstractIncomingMessage) -> str:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_PAYMENT_RESPONSE

if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=lambda message: None
    ))
