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
import logging
from common.idempotency_utils import cache_response_with_db,is_duplicate_message,cache_response_with_pipe

db: redis.asyncio.cluster.RedisCluster = configure_redis()

async def create_user():
    key: str = str(uuid.uuid4())
    try:
        await db.set(key, 0)
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    return create_response_message(
        content={'user_id':key},
        is_json=True
    )

async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, int] = {f"{i}": starting_money for i in range(n)}
    try:
        async with db.pipeline() as pipe:
            for key, value in kv_pairs.items():
                pipe.set(key, value)
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    return create_response_message(
        content={"msg": "Batch init for users successful"},
        is_json=True
    )

async def find_user(user_id: str):
    try:
        credit = await db.get(user_id)
        if credit is None:
            return create_error_message(f"User: {user_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    credit = int(credit)

    return create_response_message(
        content={
            "user_id": user_id,
            "credit": credit
        },
        is_json=True
    )

async def add_credit(user_id: str, amount: int, correlation_id: str, message_type: str):
    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response
    try:
        if not await db.exists(user_id):
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(f"User: {user_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    # update credit, serialize and update database
    try:
        async with db.pipeline() as pipe:
            pipe.incrby(user_id,amount=amount)
            cache_response_with_pipe(pipe,correlation_id,message_type,True)
            result = await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        await cache_response_with_db(db,correlation_id,message_type,False)
        return create_error_message(str(e))

    return create_response_message(
        content=f"User: {user_id} credit updated to:{result[0]}",
        is_json=False
    )


async def remove_credit(user_id: str, amount: int, correlation_id: str, message_type: str):
    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response
    # attempt to get lock
    if not await attempt_acquire_locks(db, [user_id]):
        return create_error_message("Failed to acquire necessary lock after multiple retries")

    try:
        credit = await db.get(user_id)
        if credit is None:
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(f"User: {user_id} not found")

        credit = int(credit)
        amount = int(amount)

        if credit < amount:
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(
                error=f"User: {user_id} credit cannot get reduced below zero!"
            )
        async with db.pipeline() as pipe:
            pipe.decrby(user_id, amount=amount)
            cache_response_with_pipe(pipe,correlation_id,message_type,True)
            result = await pipe.execute()
        return create_response_message(
                content=f"User: {user_id} credit updated to: {result[0]}",
                is_json=False
            )
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        await cache_response_with_db(db,correlation_id,message_type,False)
        return create_error_message(str(e))
    finally:
        await release_locks(db, [user_id])


async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return await create_user()
    elif message_type == MsgType.BATCH_INIT:
        return await batch_init_users(
            n=content['n'],
            starting_money=content['starting_money']
        )
    elif message_type == MsgType.FIND:
        return await find_user(user_id=content['user_id'])
    elif message_type in (MsgType.ADD, MsgType.SAGA_PAYMENT_REVERSE):
        return await add_credit(user_id=content['user_id'], amount=content["total_cost"], correlation_id=message.correlation_id, message_type=message_type)
    elif message_type == MsgType.SUBTRACT:
        return await remove_credit(user_id=content['user_id'], amount=content["total_cost"], correlation_id=message.correlation_id, message_type=message_type)
    elif message_type == MsgType.SAGA_INIT:
        return await remove_credit(user_id=content['user_id'], amount=content['total_cost'], correlation_id=message.correlation_id, message_type=message_type)
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        logging.error("THIS SHOULDN'T EVER HAPPEN BIG PROBLEMS IF IT DOES!")
        return None # Ignore

    return create_error_message(
        error=f"Unknown message type: {message_type}"
    )

def get_message_response_type(message: AbstractIncomingMessage) -> MsgType:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_PAYMENT_RESPONSE

async def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    return None

if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=get_custom_reply_to
    ))
