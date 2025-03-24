import time

import redis
import atexit
import signal
import asyncio
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

LOCK_EXPIRY=3 # 3 seconds lock expiry in case of error
MAX_RETRIES = 5
MAX_RETRIES_BEFORE_ERROR = 3 # Retry 3 times, then raise error
MAX_WAIT_TIME = 0.512
MIN_WAIT_TIME = 0.008
RETRY_DELAY = 0.1

async def close_redis(db: redis.asyncio.RedisCluster):
    await db.close()

def configure_redis(host: str, port: int = 6379) -> redis.asyncio.RedisCluster:
    """
    Connect to database and register a callback handler which terminates the database upon app termination.

    :param host: The host of any redis node within the cluster.
    :param port: The port number of the associated node
    :return: An initialised cluster client.
    """
    host = str(host)
    port = int(port)
    db = redis.asyncio.cluster.RedisCluster(
        host=host,
        port=port,
        decode_responses=False,
        retry=Retry(ExponentialBackoff(cap=MAX_WAIT_TIME, base=MIN_WAIT_TIME), MAX_RETRIES),
        retry_on_error=[ConnectionError, TimeoutError, ConnectionResetError],
        cluster_error_retry_attempts=MAX_RETRIES_BEFORE_ERROR,
        require_full_coverage=True
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(close_redis(db)))
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(close_redis(db)))

    return db

async def attempt_acquire_locks(db, keys):
    """Attempts to acquire locks on stock keys with retry logic."""
    for _ in range(MAX_RETRIES):
        acquired_locks = await acquire_locks(db, keys)
        if acquired_locks:
            return acquired_locks
        await asyncio.sleep(RETRY_DELAY)  # Wait before retrying

async def acquire_locks(db, keys) -> list[str] | None:
    """Try to acquire locks for all relevant stock keys."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting
    # Try to acquire all locks
    async with db.pipeline() as pipe:
        for lock in lock_keys:
            pipe.set(lock, "1", nx=True, ex=LOCK_EXPIRY)

        lock_results = await pipe.execute()

    if all(lock_results):
        return lock_keys
    # If any lock fails, release all acquired locks and return None
    await release_locks(db, keys)
    return None

async def release_locks(db, keys):
    """Release the locks."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting

    async with db.pipeline() as pipe:
        for lock in lock_keys:
            pipe.delete(lock)

        await pipe.execute()
