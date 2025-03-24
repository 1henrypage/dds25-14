import redis
import os

from common.msg_types import MsgType
from common.redis_utils import release_locks, attempt_acquire_locks
from common.request_utils import create_error_message, create_response_message
from model import OrderValue

# ====================
# Saga Processing Logic
# ====================

async def process_saga(db, order_worker_client, correlation_id: str):
    """Processes the saga completion based on stored states."""
    saga_key = f"saga-{correlation_id}"
    order_data = await db.hgetall(saga_key)

    if not order_data or b"payment" not in order_data or b"stock" not in order_data or b"processed" in order_data:
        return None  # Saga is not yet complete or has been completed already

    payment_status = int(order_data[b"payment"])
    stock_status = int(order_data[b"stock"])
    order_id = order_data[b"order_id"].decode("utf-8")

    return await handle_saga_completion(db, order_worker_client, order_id, payment_status, stock_status, correlation_id)


async def get_order_from_db(db, order_id: str) -> OrderValue | None:
    """
    Gets an order from DB via id. Is NONE, if it doesn't exist

    :param db: database
    :param order_id: The order ID
    :return: The order as a `OrderValue` object, none if it doesn't exist
    """
    async with db.pipeline() as pipe:
        pipe.hgetall(order_id)
        pipe.lrange(f"{order_id}:items", 0, -1)
        order_data, items_raw = await pipe.execute()

    if not order_data:
        return None

    return OrderValue(
        paid=int(order_data.get(b"paid")) > 0,
        user_id=order_data.get(b"user_id").decode("utf-8"),
        total_cost=int(order_data.get(b"total_cost")),
        items=[(items_raw[i].decode("utf-8"), int(items_raw[i + 1])) for i in range(0, len(items_raw), 2)] if items_raw else []
    )

async def handle_saga_completion(db, order_worker_client, order_id: str, payment_status: int, stock_status: int, correlation_id: str):
    """Handles the different completion scenarios of the saga."""
    try:
        order_entry = await get_order_from_db(db, order_id)
        if order_entry is None:
            return create_error_message(f"Order for saga completion {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(error=str(e))

    result = None

    if payment_status == 1 and stock_status == 1: # Both payment and stock successful, complete order
        result = await finalize_order(db, order_id)
    elif payment_status == 1: # Payment successful but stock failed, reverse payment
        msg = {"user_id": order_entry.user_id, "total_cost": order_entry.total_cost}
        result = await reverse_service(order_worker_client, msg, correlation_id, MsgType.SAGA_PAYMENT_REVERSE, "Payment", "Stock")
    elif stock_status == 1: # Stock successful but payment failed, reverse stock
        msg = {"items": order_entry.items}
        result = await reverse_service(order_worker_client, msg, correlation_id, MsgType.SAGA_STOCK_REVERSE, "Stock", "Payment")

    await db.hsetnx(f"saga-{correlation_id}", "processed", "1")

    return create_error_message("Both payment and stock services failed in the SAGA.") if result is None else result

# =========================
# Finalization and Reversal
# =========================

async def finalize_order(db, order_id):
    """Finalizes the order if both stock and payment succeed."""
    try:
        await db.hincrby(order_id, "paid", 1)
        return create_response_message("Checkout successful!", is_json=False)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def reverse_service(order_worker_client, msg, correlation_id, msg_type, service_name_good, service_name_bad):
    """Handles the response when a service fails, either payment or stock."""
    if msg_type == MsgType.SAGA_PAYMENT_REVERSE:
        await order_worker_client.call_with_route_no_reply(
            msg=msg,
            msg_type=msg_type,
            routing_key=os.environ['PAYMENT_ROUTE'],
            correlation_id=correlation_id,
            reply_to=None
        )
    elif msg_type == MsgType.SAGA_STOCK_REVERSE:
        await order_worker_client.call_with_route_no_reply(
            msg=msg,
            msg_type=msg_type,
            routing_key=os.environ['STOCK_ROUTE'],
            correlation_id=correlation_id,
            reply_to=None
        )
    else:
        raise RuntimeError("This shouldn't happen!")

    return create_error_message(f"{service_name_bad} service failed in the SAGA, {service_name_good} reversed.")

# =====================
# Saga Completion Check
# =====================

async def check_saga_completion(db, order_worker_client, correlation_id):
    """Check if both payment and stock responses are available atomically using a single lock with retries."""
    lock_key = f"saga-{correlation_id}" # Lock key for the saga correlation ID
    if await attempt_acquire_locks(db, [lock_key]):
        try:
            # Now that we have acquired the lock, check the status in the hash
            return await process_saga(db, order_worker_client, correlation_id)
        finally:
            # Release the lock after processing
            await release_locks(db, [lock_key])
    # If lock could not be acquired after max_retries, return None
    return None