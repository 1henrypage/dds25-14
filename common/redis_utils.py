
import redis
import atexit
from typing import Type, TypeVar, Optional
from flask import abort, Response
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
    try:
        entry: bytes = db.get(key)
    except redis.exceptions.RedisError as e:
        return abort(400, str(e))

    entry: Optional[T] = msgpack.decode(entry, type=value_type) if entry else None
    if entry is None:
        abort(400, f"{value_type.__name__}: {key} not found!")

    return entry
