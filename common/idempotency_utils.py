import redis
from msgspec import msgpack
from common.request_utils import create_response_message, create_error_message

IDEMPOTENCY_EXPIRY = 60  # seconds

async def is_duplicate_message(db: redis.asyncio.cluster.RedisCluster, correlation_id: str, message_type: str) -> dict:
    """
    Check if a message has been processed before using correlation_id
    
    :param db: Redis connection
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :return: cached_response
    """
    if not correlation_id:
        return None
        
    idempotency_key = f"{correlation_id}:{message_type}"
    
    cached_value = await db.get(idempotency_key)
    
    if cached_value is not None:
        cached_str = cached_value.decode('utf-8') if isinstance(cached_value, bytes) else str(cached_value)
        
        if cached_str == "ERROR":
            return create_error_message(f"Duplicate error message")
        if cached_str == "SUCCESS":
            return create_response_message(content=f"Duplicate response", is_json=False)
    
    # Message hasn't been processed before
    return None

async def cache_response_with_db(db: redis.asyncio.cluster.RedisCluster, correlation_id: str, message_type: str, response: bool):
    """
    Cache the response using a Redis database connection
    
    :param db: Redis database connection
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :param response: Response value to cache
    """
    if not correlation_id:
        return
        
    idempotency_key = f"{correlation_id}:{message_type}"
    
    # Use consistent string values instead of booleans
    cache_value = "SUCCESS" if response else "ERROR"
    
    try:
        await db.set(idempotency_key, cache_value, ex=IDEMPOTENCY_EXPIRY)
    except Exception as e:
        # If caching fails, log but continue - it's not critical
        print(f"Failed to cache response for {correlation_id}: {str(e)}")

def cache_response_with_pipe(pipe: redis.asyncio.cluster.ClusterPipeline, correlation_id: str, message_type: str, response: bool):
    """
    Cache the response using a Redis pipeline
    
    :param pipe: Redis pipeline to use
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :param response: Response value to cache
    """
    if not correlation_id:
        return
        
    idempotency_key = f"{correlation_id}:{message_type}"
    
    # Use consistent string values instead of booleans
    cache_value = "SUCCESS" if response else "ERROR"
    
    try:
        pipe.set(idempotency_key, cache_value, ex=IDEMPOTENCY_EXPIRY)
    except Exception as e:
        # If caching fails, log but continue - it's not critical
        print(f"Failed to cache response for {correlation_id}: {str(e)}")