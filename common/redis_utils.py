import time

import redis
import atexit

LOCK_EXPIRY=3 # 3 seconds lock expiry in case of error
MAX_RETRIES = 3
RETRY_DELAY = 0.1 # TODO make this exponentiaal backofff

def configure_redis(host: str, port: int = 6379) -> redis.RedisCluster:
    """
    Connect to database and register a callback handler which terminates the database upon app termination.

    :param host: The host of any redis node within the cluster.
    :param port: The port number of the associated node
    :return: An initialised cluster client.
    """
    host = str(host)
    port = int(port)
    db = redis.RedisCluster(
        host=host,
        port=port,
        decode_responses=False,
        require_full_coverage=True
    )
    atexit.register(lambda: db.close())
    return db

def attempt_acquire_locks(db, keys):
    """Attempts to acquire locks on stock keys with retry logic."""
    for _ in range(MAX_RETRIES):
        acquired_locks = acquire_locks(db, keys)
        if acquired_locks:
            return acquired_locks
        time.sleep(RETRY_DELAY)  # Wait before retrying

def acquire_locks(db, keys: list[str]) -> list[str] | None:
    """Try to acquire locks for all relevant stock keys."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting
    # Try to acquire all locks
    with db.pipeline() as pipe:
        for lock in lock_keys:
            pipe.set(lock, "1", nx=True, ex=LOCK_EXPIRY)

        lock_results = pipe.execute()

    if all(lock_results):
        return lock_keys
    # If any lock fails, release all acquired locks and return None
    release_locks(db, keys)
    return None

def release_locks(db, keys: list[str]):
    """Release the locks."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting

    with db.pipeline() as pipe:
        for lock in lock_keys:
            pipe.delete(lock)

        pipe.execute()
