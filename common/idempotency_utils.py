import redis
from msgspec import msgpack

IDEMPOTENCY_EXPIRY = 60  # seconds

def make_response_key(user_id: str, correlation_id: str, message_type: str) -> str:
    """
    Build a Redis-safe idempotency key using hash tags for cluster slot alignment.
    """
    return f"response:{{{user_id}}}:{correlation_id}:{message_type}"

async def is_duplicate_message(
    db: redis.asyncio.cluster.RedisCluster,
    user_id: str,
    correlation_id: str,
    message_type: str
) -> dict:
    """
    Check if a message has been processed before using the full idempotency key.
    
    :param db: Redis connection
    :param user_id: The user ID used for hashing
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :return: cached_response or None
    """
    if not correlation_id:
        return None

    idempotency_key = make_response_key(user_id, correlation_id, message_type)
    cached_value = await db.get(idempotency_key)

    if cached_value:
        try:
            return msgpack.decode(cached_value)
        except Exception as e:
            raise ValueError(f"Failed to decode cached response for {idempotency_key}: {str(e)}")

    return None

async def cache_response(
    db: redis.asyncio.cluster.RedisCluster,
    user_id: str,
    correlation_id: str,
    message_type: str,
    response: dict,
    pipeline=None
):
    """
    Cache the response for a given correlation ID and message type using a user-based Redis key.
    
    :param db: Redis connection
    :param user_id: The user ID to ensure key cluster affinity
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :param response: The response to cache
    :param pipeline: Optional Redis pipeline for transaction support
    """
    if not correlation_id:
        return

    idempotency_key = make_response_key(user_id, correlation_id, message_type)

    try:
        encoded = msgpack.encode(response)
        if pipeline:
            await pipeline.set(idempotency_key, encoded, ex=IDEMPOTENCY_EXPIRY)
        else:
            await db.set(idempotency_key, encoded, ex=IDEMPOTENCY_EXPIRY)
    except Exception as e:
        print(f"Failed to cache response for {idempotency_key}: {str(e)}")
