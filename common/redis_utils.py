import time

import redis
import atexit
import signal
import asyncio
import os
import socket
import random

from redis.asyncio.cluster import ClusterNode
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

# LOCK VALUES
LOCK_EXPIRY=3 # 3 seconds lock expiry in case of error
MAX_RETRIES = 3
LOCK_RETRY_DELAY = 0.1

# CLUSTER VALUES
MAX_RETRIES_BEFORE_ERROR = 3# Retry 3 times, then raise error

def configure_redis() -> redis.asyncio.RedisCluster:
    """
    Connect to database and register a callback handler which terminates the database upon app termination.

    :return: An initialised cluster client.
    """
    port = int(os.environ['REDIS_PORT'])
    nodes = [
        ClusterNode(os.environ['MASTER_1'], port=port),
        ClusterNode(os.environ['MASTER_2'], port=port),
        ClusterNode(os.environ['MASTER_3'], port=port),
        ClusterNode(os.environ['REPLICA_1'], port=port),
        ClusterNode(os.environ['REPLICA_2'], port=port),
        ClusterNode(os.environ['REPLICA_3'], port=port),
    ]

    # Shuffle only the first three nodes (masters)
    random.shuffle(nodes[:3])

    # The shuffled first three nodes combined with the unchanged replicas
    nodes = nodes[:3] + nodes[3:]

    exceptions_to_retry = [
        redis.exceptions.ClusterDownError,
        redis.exceptions.ConnectionError,
        redis.exceptions.BusyLoadingError,
        redis.exceptions.TimeoutError,
        ConnectionResetError,
        OSError,
        TimeoutError,
        socket.timeout
    ]

    db = redis.asyncio.cluster.RedisCluster(
        startup_nodes=nodes,
        decode_responses=False,
        retry=Retry(ExponentialBackoff(), 6),
        retry_on_error=exceptions_to_retry,
        cluster_error_retry_attempts=MAX_RETRIES_BEFORE_ERROR,
        require_full_coverage=True
    )


    return db

async def attempt_acquire_locks(db, keys):
    """Attempts to acquire locks on stock keys with retry logic."""
    for _ in range(MAX_RETRIES):
        acquired_locks = await acquire_locks(db, keys)
        if acquired_locks:
            return acquired_locks
        await asyncio.sleep(LOCK_RETRY_DELAY)  # Wait before retrying

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
    await release_locks(db, [key for idx, key in enumerate(keys) if lock_results[idx]])
    return None

async def release_locks(db, keys):
    """Release the locks."""
    lock_keys = [f"{key}-lock" for key in keys]  # Ensure consistent lock key formatting

    async with db.pipeline() as pipe:
        for lock in lock_keys:
            pipe.delete(lock)

        await pipe.execute()
