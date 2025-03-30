import os
import uuid
import redis
import asyncio

from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, release_locks, attempt_acquire_locks
from common.request_utils import create_error_message, create_response_message
from common.idempotency_utils import is_duplicate_message, make_response_key, cache_response

db: redis.asyncio.cluster.RedisCluster = configure_redis(host=os.environ['MASTER_1'], port=int(os.environ['REDIS_PORT']))

async def recover_pending_operations():
    pending_keys = await db.keys("pending:*")
    for pending_key in pending_keys:
        try:
            _, correlation_id, message_type = pending_key.decode().split(":")
            item_key = await db.get(pending_key)
            if not item_key:
                continue

            item_id = item_key.decode().strip("{}")
            response_key = make_response_key(item_id, correlation_id, message_type)

            if await db.exists(response_key):
                await db.delete(pending_key)
                continue

            print(f"[RECOVERY] Message {correlation_id} of type {message_type} with item {item_id} may need replay.")
        except Exception as e:
            print(f"[RECOVERY ERROR] While handling {pending_key}: {e}")

async def create_item(price: int, correlation_id: str, message_type: str):
    item_id = str(uuid.uuid4())
    item_key = f"{{{item_id}}}"
    response_key = make_response_key(item_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        await db.set(pending_key, item_key)
        response = create_response_message(content={'item_id': item_id}, is_json=True)
        encoded_response = msgpack.encode(response)

        await db.hset(item_key, mapping={"stock": 0, "price": int(price)})
        await db.set(response_key, encoded_response)
        await db.delete(pending_key)

        return response
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        for i in range(int(n)):
            await db.hset(f"{{{i}}}", mapping={"stock": int(starting_stock), "price": int(item_price)})
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(content={"msg": "Batch init for stock successful"}, is_json=True)

async def find_item(item_id: str):
    item_key = f"{{{item_id}}}"
    try:
        item_data = await db.hgetall(item_key)
        if not item_data:
            return create_error_message(f"Item: {item_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(
        content={
            "stock": int(item_data.get(b"stock", b"-1").decode("utf-8")),
            "price": int(item_data.get(b"price", b"-1").decode("utf-8"))
        },
        is_json=True
    )

async def add_stock(item_id: str, amount: int, correlation_id: str, message_type: str):
    item_key = f"{{{item_id}}}"
    try:
        if not await db.exists(item_key):
            return create_error_message(f"Item: {item_id} not found")

        try:
            async with db.pipeline() as pipe:
                # Set pending key
                pipe.set(f"pending:{correlation_id}:{message_type}", item_key)
                # Update stock
                pipe.hincrby(item_key, "stock", amount)
                # Execute all commands
                results = await pipe.execute()

            new_stock = results[-1]  # Last result is from hincrby
            response = create_response_message(content=f"Item: {item_id} stock updated to: {new_stock}", is_json=False)
            
            try:
                await cache_response(db, item_id, correlation_id, message_type, response)
                await db.delete(f"pending:{correlation_id}:{message_type}")
            except RuntimeError:
                # Ignore event loop closure during cleanup
                pass
                
            return response
        except redis.exceptions.RedisError as e:
            return create_error_message(str(e))
    except Exception as e:
        # Handle any other exceptions, including RuntimeError
        return create_error_message(str(e))

async def remove_stock(item_id: str, amount: int):
    item_key = f"{{{item_id}}}"
    try:
        item_stock = await db.hget(item_key, "stock")
        if item_stock is None:
            return create_error_message(f"Item: {item_id} not found")

        if not await attempt_acquire_locks(db, [item_id]):
            return create_error_message("Failed to acquire necessary lock after multiple retries")

        item_stock = int(item_stock)
        amount = int(amount)

        if item_stock < amount:
            return create_error_message(error=f"Item: {item_id} stock cannot get reduced below zero!")

        new_stock = await db.hincrby(item_key, "stock", -amount)
        return create_response_message(content=f"Item: {item_id} stock updated to: {new_stock}", is_json=False)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    finally:
        await release_locks(db, [item_id])

async def check_and_validate_stock(item_dict: dict[str, int]):
    for item_id, amount in item_dict.items():
        stock = await db.hget(f"{{{item_id}}}", "stock")
        if stock is None or int(stock) < amount:
            return create_error_message(f"Not enough stock for item: {item_id}")

async def subtract_bulk(item_dict: dict[str, int], correlation_id: str, message_type: str):
    cached = await is_duplicate_message(db, "bulk", correlation_id, message_type)
    if cached:
        return cached

    if not await attempt_acquire_locks(db, item_dict.keys()):
        return create_error_message("Failed to acquire necessary locks after multiple retries")

    try:
        if validation_error := await check_and_validate_stock(item_dict):
            return validation_error

        await db.set(f"pending:{correlation_id}:{message_type}", "bulk")

        for item_id, dec_amount in item_dict.items():
            await db.hincrby(f"{{{item_id}}}", "stock", -dec_amount)

        response = create_response_message("All items' stock successfully updated for the saga.", is_json=False)
        await cache_response(db, "bulk", correlation_id, message_type, response)
        await db.delete(f"pending:{correlation_id}:{message_type}")
        return response
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    finally:
        await release_locks(db, item_dict.keys())

async def add_bulk(item_dict: dict[str, int], correlation_id: str, message_type: str):
    cached = await is_duplicate_message(db, "bulk", correlation_id, message_type)
    if cached:
        return cached

    try:
        await db.set(f"pending:{correlation_id}:{message_type}", "bulk")

        for item_id, inc_amount in item_dict.items():
            await db.hincrby(f"{{{item_id}}}", "stock", inc_amount)

        response = create_response_message("Stock successfully restored for the saga reversal.", is_json=False)
        await cache_response(db, "bulk", correlation_id, message_type, response)
        await db.delete(f"pending:{correlation_id}:{message_type}")
        return response
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def process_message(message: AbstractIncomingMessage):
    correlation_id = message.correlation_id
    message_type = message.type
    content = msgpack.decode(message.body)

    if message_type in (MsgType.SAGA_INIT, MsgType.SAGA_STOCK_REVERSE):
        cache_key = "bulk"
    else:
        cache_key = content.get("item_id")

    cached_response = await is_duplicate_message(db, cache_key, correlation_id, message_type)
    if cached_response:
        return cached_response

    if message_type == MsgType.CREATE:
        response = await create_item(price=content["price"], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.BATCH_INIT:
        response = await batch_init_users(n=content["n"], starting_stock=content["starting_stock"], item_price=content["item_price"])
    elif message_type in (MsgType.FIND, MsgType.FIND_PRIORITY):
        response = await find_item(item_id=content["item_id"])
    elif message_type == MsgType.ADD:
        response = await add_stock(item_id=content["item_id"], amount=content["total_cost"], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.SUBTRACT:
        response = await remove_stock(item_id=content["item_id"], amount=content["total_cost"])
    elif message_type == MsgType.SAGA_INIT:
        response = await subtract_bulk(item_dict=dict(content["items"]), correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        response = await add_bulk(item_dict=dict(content["items"]), correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.SAGA_PAYMENT_REVERSE:
        response = None
    else:
        response = create_error_message(error=f"Unknown message type: {message_type}")

    return response

def get_message_response_type(message: AbstractIncomingMessage) -> MsgType:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_STOCK_RESPONSE

async def get_custom_reply_to(message: AbstractIncomingMessage) -> str:
    return None

async def main():
    try:
        # First recover any pending operations
        await recover_pending_operations()
        
        # Then start consuming events
        await consume_events(
            process_message=process_message,
            get_message_response_type=get_message_response_type,
            get_custom_reply_to=get_custom_reply_to
        )
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Ensure we close the Redis connection
        try:
            await db.close()
        except Exception as e:
            print(f"Error closing Redis connection: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
    except Exception as e:
        print(f"Fatal error: {e}")
