import os
import uuid

import redis
import asyncio
import logging

from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, release_locks, acquire_locks, attempt_acquire_locks
from common.request_utils import create_error_message, create_response_message
from common.idempotency_utils import is_duplicate_message, cache_response_with_db, cache_response_with_pipe

db: redis.asyncio.cluster.RedisCluster = configure_redis()


async def create_item(price: int):
    key = str(uuid.uuid4())
    try:
        await db.hset(key, mapping={"stock": 0, "price": int(price)})
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    return create_response_message(
        content={'item_id':key},
        is_json=True
    )


async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)


    try:
        async with db.pipeline() as pipe:
            for i in range(n):
                pipe.hset(f"{i}", mapping={"stock": starting_stock, "price": item_price})
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    return create_response_message(
        content={"msg": "Batch init for stock successful"},
        is_json=True
    )

async def find_item(item_id: str):
    try:
        item_data = await db.hgetall(item_id)
        if not item_data:
            return create_error_message(f"Item: {item_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    return create_response_message(
        content={
            "stock": int(item_data.get(b"stock", b"-1").decode("utf-8")),
            "price": int(item_data.get(b"price", b"-1").decode("utf-8"))
        },
        is_json=True
    )

async def add_stock(item_id: str, amount: int,correlation_id: str, message_type: str):
    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response
    try:
        if not await db.exists(item_id):
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(f"Item: {item_id} not found")
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    # update stock, serialize and update database
    try:
        async with db.pipeline() as pipe:
            pipe.hincrby(item_id, "stock", amount)
            cache_response_with_pipe(pipe,correlation_id,message_type,True)
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    return create_response_message(
        content = f"Item: {item_id} stock updated",
        is_json=False
    )

async def remove_stock(item_id: str, amount: int,correlation_id: str, message_type: str):
    cached_response = await is_duplicate_message(db,correlation_id,message_type)
    if cached_response is not None:
        return cached_response
    # attempt to get lock
    if not await attempt_acquire_locks(db, [item_id]):
        return create_error_message("Failed to acquire necessary lock after multiple retries")

    try:
        item_stock = await db.hget(item_id, "stock")
        if item_stock is None:
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(f"Item: {item_id} not found")
        item_stock = int(item_stock)
        amount = int(amount)

        # update stock, serialize and update database
        if item_stock < amount:
            await cache_response_with_db(db,correlation_id,message_type,False)
            return create_error_message(
                error=f"Item: {item_id} stock cannot get reduced below zero!"
            )
        try:
            async with db.pipeline() as pipe:
                new_stock = pipe.hincrby(item_id, "stock", -amount)
                cache_response_with_pipe(pipe,correlation_id,message_type,True)
                await pipe.execute()
        except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
            return create_error_message(str(e))
        
        return create_response_message(
            content=f"Item: {item_id} stock updated to: {new_stock}",
            is_json=False
        )
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        await cache_response_with_db(db,correlation_id,message_type,False)
        return create_error_message(str(e))
    finally:
        await release_locks(db, [item_id])


async def check_and_validate_stock(item_dict: dict[str, int]):
    """Validates if there is enough stock for all items in the dictionary."""
    # Start a pipeline to fetch all stocks in one go
    # Queue all the hget commands for each item
    async with db.pipeline() as pipe:
        for item_id in item_dict:
            pipe.hget(item_id, "stock")

        results = await pipe.execute()

    # Now process each result
    for index, (item_id, amount) in enumerate(item_dict.items()):
        current_stock = results[index]
        if current_stock is None or int(current_stock) < amount:
            return create_error_message(f"Not enough stock for item: {item_id}")


async def subtract_bulk(item_dict: dict[str, int]):
    """Attempts to decrement stock safely while locking only relevant keys."""
    # Attempt to acquire locks
    if not await attempt_acquire_locks(db, item_dict.keys()):
        return create_error_message("Failed to acquire necessary locks after multiple retries")
    try:
        # Fetch current stock levels and check availability
        if validation_error := await check_and_validate_stock(item_dict):
            return validation_error

        # If sufficient stock is available, update in a pipeline
        async with db.pipeline() as pipe:
            for item_id, dec_amount in item_dict.items():
                pipe.hincrby(item_id, "stock", -dec_amount)
            await pipe.execute()
        return create_response_message(content="All items' stock successfully updated for the saga.", is_json=False)
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))
    finally:
        await release_locks(db, item_dict.keys())

async def add_bulk(item_dict: dict[str, int]):
    # Use a pipeline to send multiple INCRBY commands in a batch
    try:
        async with db.pipeline() as pipe:
            for item_id, inc_amount in item_dict.items():
                pipe.hincrby(item_id, "stock", inc_amount)
            await pipe.execute()
    except (redis.exceptions.RedisError, redis.exceptions.RedisClusterException) as e:
        return create_error_message(str(e))

    return create_response_message(
        content="Stock successfully restored for the saga reversal.",
        is_json=False
    )

async def process_message(message: AbstractIncomingMessage):
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type == MsgType.CREATE:
        return await create_item(price=content["price"])
    elif message_type == MsgType.BATCH_INIT:
        return await batch_init_users(n=content["n"], starting_stock=content["starting_stock"], item_price=content["item_price"])
    elif message_type == MsgType.FIND or message_type == MsgType.FIND_PRIORITY:
        return await find_item(item_id=content["item_id"])
    elif message_type == MsgType.ADD:
        return await add_stock(item_id=content["item_id"], amount=content["total_cost"],correlation_id=message.correlation_id,message_type=message_type)
    elif message_type == MsgType.SUBTRACT:
        return await remove_stock(item_id=content["item_id"], amount=content["total_cost"],correlation_id=message.correlation_id,message_type=message_type)
    elif message_type == MsgType.SAGA_INIT:
        return await subtract_bulk(item_dict=dict(content["items"]))
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        return await add_bulk(item_dict=dict(content["items"]))
    elif message_type == MsgType.SAGA_PAYMENT_REVERSE:
        logging.error("THIS SHOULDN'T EVER HAPPEEN, BIG PROBLEMS IF IT DOES")
        return None # Ignore

    return create_error_message(error=f"Unknown message type: {message_type}")


def get_message_response_type(message: AbstractIncomingMessage) -> MsgType:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_STOCK_RESPONSE

async def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    return None


if __name__ == "__main__":
    asyncio.run(consume_events(
        process_message=process_message,
        get_message_response_type=get_message_response_type,
        get_custom_reply_to=get_custom_reply_to
    ))
