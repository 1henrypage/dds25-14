
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
    """
    Gets a value from a redis cluster by key. Does some failure handling
    400 -> if redis fails for some reason
    400 -> if key is not found

    :param db: The cluster to retrieve from
    :param key: The key to retrieve from
    :param value_type: The type of the domain object. Can be OrderValue | StockValue | UserValue
    :return: The value as a python object. Otherwise client error
    """
    try:
        entry: bytes = db.get(key)
    except redis.exceptions.RedisError as e:
        return abort(400, str(e))

    entry: Optional[T] = msgpack.decode(entry, type=value_type) if entry else None
    if entry is None:
        abort(400, f"{value_type.__name__}: {key} not found!")

    return entry

def set_in_db(db: redis.RedisCluster, key: str, value: bytes) -> None:
    """
    Sets a value from a redis cluster by key. Does some failure handling
    400 -> if redis fails for some reason

    :param db: The cluster to set the key in
    :param key: The key to set
    :param value: The value to set along with the associated key
    :return: Nothing if successful, 400 if it failed for some reason
    """

    try:
        db.set(key, value)
    except redis.exceptions.RedisError as e:
        return abort(400, str(e))
    return None

