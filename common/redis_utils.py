
import redis
import atexit
from typing import Type, TypeVar, Optional
from msgspec import msgpack

T = TypeVar('T')

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

def get_from_db(db: redis.RedisCluster, key: str, value_type: Type[T]) -> Optional[T]:
    """
    Gets a value from a redis cluster by key. DOES NO FAILURE HANDLING! ALL FAILURES HAVE TO BE HANDLED OUTSIDE!
    """
    entry: bytes = db.get(key)
    entry: Optional[T] = msgpack.decode(entry, type=value_type) if entry else None
    return entry

