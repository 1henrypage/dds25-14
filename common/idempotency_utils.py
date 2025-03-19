import redis
import time
from typing import Optional, Any, Tuple

class IdempotencyManager:
    def __init__(self, db: redis.RedisCluster, ttl_sec: int = 60):
        """
        Initialize with redis connection
        :param redis: redis db instance
        :param ttl_sec: idempotency keys ttl (default: 1 hour)
        
        """
        self.redis = db
        self.expiry_time = ttl_sec

    def check_and_set_request(self, idempotency_key: str, response: Optional[bytes] = None) -> Tuple[bool, Optional[bytes]]:
        """
        checks if a request has been processed before and stores its response if not

        :param idempotency_key: unique key for the request (correlation id)
        :param response: optional response to store if this is a new request
        :return: tuple of (is_new_request, stored_response)
        """
        response_key = f"idempotent:response:{idempotency_key}"

        stored_response = self.redis.get(response_key)

        if stored_response is not None:
            return (False, stored_response)
        
        if response is not None:
            try:
                with self.redis.pipeline() as pipeline:
                    pipeline.set(response_key, response)
                    pipeline.expire(response_key, self.expiry_time)
                    pipeline.execute()
            except redis.RedisClusterException:
                # If pipeline fails, try individual commands
                self.redis.set(response_key, response)
                self.redis.expire(response_key, self.expiry_time)

        return (True, None)

    def store_response(self, idempotency_key: str, response: bytes) -> None:
        """
        store a response for a previously checked request

        :param idempotency_key: correlation id
        :param response: response data to store
        """
        response_key = f"idempotent:response:{idempotency_key}"
        try:
            with self.redis.pipeline() as pipeline:
                pipeline.set(response_key, response)
                pipeline.expire(response_key, self.expiry_time)
                pipeline.execute()
        except redis.RedisClusterException:
            # If pipeline fails, try individual commands
            self.redis.set(response_key, response)
            self.redis.expire(response_key, self.expiry_time)