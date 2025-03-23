import redis
from msgspec import msgpack

IDEMPOTENCY_EXPIRY = 60  # seconds

def is_duplicate_message(db: redis.RedisCluster, correlation_id: str, message_type: str) -> tuple[bool, dict]:
    """
    Check if a message has been processed before using correlation_id
    
    :param db: Redis connection
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :return: Tuple of (is_duplicate, cached_response)
    """
    if not correlation_id:
        return False, None
        
    idempotency_key = f"{correlation_id}:{message_type}"
    
    cached_value = db.get(idempotency_key)
    
    if cached_value:
        try:
            return True, msgpack.decode(cached_value)
        except:
            # If we can't decode the response, there was an issue with the cached value
            return True, None
    
    # Message hasn't been processed before
    return False, None

def cache_response(db: redis.RedisCluster, correlation_id: str, message_type: str, response: dict):
    """
    Cache the response for a given correlation ID and message type
    
    :param db: Redis connection
    :param correlation_id: The unique correlation ID for the message
    :param message_type: The type of message
    :param response: The response to cache
    """
    if not correlation_id:
        return
        
    idempotency_key = f"{correlation_id}:{message_type}"
    
    try:
        db.set(idempotency_key, msgpack.encode(response), ex=IDEMPOTENCY_EXPIRY)
    except Exception as e:
        # If caching fails, log but continue - it's not critical
        print(f"Failed to cache response for {correlation_id}: {str(e)}")
