
import redis
import atexit
from msgspec import msgpack

LOCK_EXPIRY=3 # 3 seconds lock expiry in case of error


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

def acquire_locks(db, keys):
    """Try to acquire locks for all relevant stock keys."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting
    acquired_locks = []

    for lock in lock_keys:
        if db.set(lock, "1", nx=True, ex=LOCK_EXPIRY):
            acquired_locks.append(lock)
        else:
            # If any lock fails, release all acquired locks
            release_locks(db, keys)  # Pass original keys
            return None

    return acquired_locks

def release_locks(db, keys):
    """Release the locks."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting
    if lock_keys:
        db.delete(*lock_keys)



