import redis
import asyncio
import time
from typing import Optional, Any, Tuple

class IdempotencyManager:
    def __init__(self,redis: redis.RedisCluster, ttl_sec: int = 3600):
        """
        Initialize with redis connection
        :param redis: redis db instance
        :param ttl_sec: idempotency keys ttl (default: 1 hour)
        
        """
        self.redis = redis
        self.expiry_time = ttl_sec

    async def check_and_set_request(self,idempotency_key: str, response: Optional[bytes] = None) -> Tuple[bool, Optional[bytes]]:
        """
        checks if a request has been processed before and stores its response if not

        :param idempotency_key: unique key for the request (correlation id)
        :param response: optional response to store if this is a new request
        :return: tuple of (is_new_request, stored_response)
        """
        response_key = f"idempotent:response:{idempotency_key}"

        stored_response = await self.redis.get(response_key)

        if stored_response is not None:
            return (False, stored_response)
        
        if response is not None:
            try:
                pipeline = await self.redis.pipeline()
                await pipeline.set(response_key, response)
                await pipeline.expire(response_key, self.expiry_time)
                await pipeline.execute()
            except redis.RedisClusterException:
                # If pipeline fails, try individual commands
                await self.redis.set(response_key, response)
                await self.redis.expire(response_key, self.expiry_time)

        return (True, None)

    async def store_response(self,idempotency_key: str, response: bytes) -> None:
        """
        store a response for a previously checked request

        :param idempotency_key: correlation id
        :param response: response data to store
        """
        response_key = f"idempotent:response:{idempotency_key}"
        try:
            pipeline = await self.redis.pipeline()
            await pipeline.set(response_key, response)
            await pipeline.expire(response_key, self.expiry_time)
            await pipeline.execute()
        except redis.RedisClusterException:
            # If pipeline fails, try individual commands
            await self.redis.set(response_key, response)
            await self.redis.expire(response_key, self.expiry_time)

