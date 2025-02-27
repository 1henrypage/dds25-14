
import os
import redis

def connect_to_redis_cluster(host: str, port: int = 6379) -> redis.RedisCluster:
    """

    :param host: The host of any redis node within the cluster
    :param port: The port number of the associated node
    :return: An initialised cluster client
    """
    host = str(host)
    port = int(port)
    return redis.RedisCluster(
        host=str(os.environ['MASTER_1']),
        port=int(os.environ['REDIS_PORT']),
        require_full_coverage=True
    )

