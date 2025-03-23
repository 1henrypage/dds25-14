import asyncio
import os
import uuid

import redis
from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.request_utils import create_response_message, create_error_message
from common.redis_utils import configure_redis
from common.idempotency_utils import is_duplicate_message, cache_response

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


async def process_message(message: AbstractIncomingMessage):
    # Check for idempotency based on the correlation ID
    correlation_id = message.correlation_id
    message_type = message.type
    
    # Skip processing if this is a duplicate message
    is_duplicate, cached_response = is_duplicate_message(db, correlation_id, message_type)
    if is_duplicate:
        if cached_response:
            return cached_response
        else:
            return create_response_message(
                content={"status": "skipped", "reason": "duplicate_message"},
                is_json=True
            )
    
    content = msgpack.decode(message.body)

    # Process the message based on its type
    if message_type == MsgType.CREATE:
        response = create_user()
    elif message_type == MsgType.BATCH_INIT:
        response = batch_init_users(
            n=content['n'],
            starting_money=content['starting_money']
        )
    elif message_type == MsgType.FIND:
        response = find_user(user_id=content['user_id'])
    elif message_type in (MsgType.ADD, MsgType.SAGA_PAYMENT_REVERSE):
        response = add_credit(user_id=content['user_id'], amount=content["total_cost"])
    elif message_type == MsgType.SUBTRACT:
        response = remove_credit(user_id=content['user_id'], amount=content["total_cost"])
    elif message_type == MsgType.SAGA_INIT:
        response = remove_credit(user_id=content['user_id'], amount=content['total_cost'])
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        response = None  # Ignore
    else:
        response = create_error_message(error=f"Unknown message type: {message_type}")
    
    # Cache the response if we have a correlation ID and response
    if correlation_id and response:
        cache_response(db, correlation_id, message_type, response)
    
    return response

def get_message_response_type(message: AbstractIncomingMessage) -> str:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_PAYMENT_RESPONSE

if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=lambda message: None
    ))
