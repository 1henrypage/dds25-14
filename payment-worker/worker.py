import os
import uuid
import asyncio
import redis

from aio_pika.abc import AbstractIncomingMessage
from msgspec import msgpack

from common.msg_types import MsgType
from common.queue_utils import consume_events
from common.redis_utils import configure_redis, release_locks, attempt_acquire_locks
from common.request_utils import create_error_message, create_response_message
from common.idempotency_utils import is_duplicate_message, make_response_key

db: redis.asyncio.cluster.RedisCluster = configure_redis(
    host=os.environ['MASTER_1'],
    port=int(os.environ['REDIS_PORT'])
)

async def recover_pending_operations():
    pending_keys = await db.keys("pending:*")
    for pending_key in pending_keys:
        try:
            _, correlation_id, message_type = pending_key.decode().split(":")
            user_key = await db.get(pending_key)
            if not user_key:
                continue

            user_id = user_key.decode().strip("{}")
            response_key = make_response_key(user_id, correlation_id, message_type)

            if await db.exists(response_key):
                await db.delete(pending_key)
                continue

            print(f"[RECOVERY] Message {correlation_id} of type {message_type} with user {user_id} may need replay.")
        except Exception as e:
            print(f"[RECOVERY ERROR] {pending_key}: {e}")

async def create_user(correlation_id: str, message_type: str):
    user_id = str(uuid.uuid4())
    user_key = f"{{{user_id}}}"
    response_key = make_response_key(user_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        await db.set(pending_key, user_key)
        await db.set(user_key, 0)
        response = create_response_message(content={'user_id': user_id}, is_json=True)
        await db.set(response_key, msgpack.encode(response))
        await db.delete(pending_key)
        return response
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def batch_init_users(n: str, starting_money: int, correlation_id: str, message_type: str):
    try:
        async with db.pipeline() as pipe:
            for i in range(int(n)):
                pipe.set(f"{{{i}}}", starting_money)
            await pipe.execute()
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

    return create_response_message(content={"msg": "Batch init for users successful"}, is_json=True)

async def find_user(user_id: str):
    try:
        credit = await db.get(f"{{{user_id}}}")
        if credit is None:
            return create_error_message(f"User: {user_id} not found")
        return create_response_message(content={"user_id": user_id, "credit": int(credit)}, is_json=True)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def add_credit(user_id: str, amount: int, correlation_id: str, message_type: str):
    user_key = f"{{{user_id}}}"
    response_key = make_response_key(user_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        # Check if we've already processed this request
        if await db.exists(response_key):
            return msgpack.decode(await db.get(response_key))

        # Use pipeline for atomic operations
        async with db.pipeline() as pipe:
            # Get current balance
            pipe.get(user_key)
            # Set pending key
            pipe.set(pending_key, user_key)
            # Update balance atomically
            pipe.incrby(user_key, amount)
            # Execute all commands
            current_balance, _, new_balance = await pipe.execute()

            if current_balance is None:
                return create_error_message(f"User: {user_id} not found")

            # Create and store response
            response = create_response_message(
                content=f"User: {user_id} credit updated to: {new_balance}",
                is_json=False
            )
            await db.set(response_key, msgpack.encode(response))
            await db.delete(pending_key)
            return response

    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    except Exception as e:
        return create_error_message(str(e))

async def remove_credit(user_id: str, amount: int, correlation_id: str, message_type: str):
    user_key = f"{{{user_id}}}"
    response_key = make_response_key(user_id, correlation_id, message_type)
    pending_key = f"pending:{correlation_id}:{message_type}"

    try:
        credit = await db.get(user_key)
        if credit is None:
            return create_error_message(f"User: {user_id} not found")

        if not await attempt_acquire_locks(db, [user_id]):
            return create_error_message("Failed to acquire necessary lock after multiple retries")

        if int(credit) < amount:
            return create_error_message(error=f"User: {user_id} credit cannot get reduced below zero!")

        await db.set(pending_key, user_key)
        new_balance = await db.decrby(user_key, amount)

        response = create_response_message(content=f"User: {user_id} credit updated to: {new_balance}", is_json=False)
        await db.set(response_key, msgpack.encode(response))
        await db.delete(pending_key)
        return response
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))
    finally:
        await release_locks(db, [user_id])

async def process_message(message: AbstractIncomingMessage):
    correlation_id = message.correlation_id
    message_type = message.type
    content = msgpack.decode(message.body)

    user_id = content.get("user_id", "create")  # fallback for CREATE

    cached = await is_duplicate_message(db, user_id, correlation_id, message_type)
    if cached:
        return cached

    if message_type == MsgType.CREATE:
        return await create_user(correlation_id, message_type)
    elif message_type == MsgType.BATCH_INIT:
        return await batch_init_users(n=content["n"], starting_money=content["starting_money"], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.FIND:
        return await find_user(user_id=content["user_id"])
    elif message_type in (MsgType.ADD, MsgType.SAGA_PAYMENT_REVERSE):
        return await add_credit(user_id=content["user_id"], amount=content["total_cost"], correlation_id=correlation_id, message_type=message_type)
    elif message_type in (MsgType.SUBTRACT, MsgType.SAGA_INIT):
        return await remove_credit(user_id=content["user_id"], amount=content["total_cost"], correlation_id=correlation_id, message_type=message_type)
    elif message_type == MsgType.SAGA_STOCK_REVERSE:
        return None
    else:
        return create_error_message(error=f"Unknown message type: {message_type}")

def get_message_response_type(message: AbstractIncomingMessage) -> MsgType:
    if message.type == MsgType.SAGA_INIT:
        return MsgType.SAGA_PAYMENT_RESPONSE

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
