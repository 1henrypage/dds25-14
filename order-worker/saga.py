from collections import defaultdict

import redis
from msgspec import msgpack

from common.msg_types import MsgType
from common.redis_utils import release_locks, attempt_acquire_locks
from common.request_utils import create_error_message, create_response_message
from model import OrderValue

SAGA_TIMEOUT = 30

# ====================
# Saga Processing Logic
# ====================

async def process_saga(db, order_worker_client, correlation_id: str):
    """Processes the saga completion based on stored states."""
    saga_key = f"saga-{correlation_id}"
    order_data = db.hgetall(saga_key)

    if not order_data or b"payment" not in order_data or b"stock" not in order_data:
        return None  # Saga is not yet complete

    payment_status = int(order_data[b"payment"])
    stock_status = int(order_data[b"stock"])
    order_id = order_data[b"order_id"].decode("utf-8")

    return await handle_saga_completion(db, order_worker_client, order_id, payment_status, stock_status, correlation_id)

async def handle_saga_completion(db, order_worker_client, order_id: str, payment_status: int, stock_status: int, correlation_id: str):
    """Handles the different completion scenarios of the saga."""
    try:
        entry: bytes = db.get(order_id)
        order_entry = msgpack.decode(entry, type=OrderValue) if entry else None
        if order_entry is None:
            return create_error_message(f"Order for saga completion {order_id} not found")
    except redis.exceptions.RedisError as e:
        return create_error_message(error=str(e))

    if payment_status == 1 and stock_status == 1: # Both payment and stock successful, complete order
        return await finalize_order(db, order_id, order_entry)
    elif payment_status == 1: # Payment successful but stock failed, reverse payment
        return await reverse_service(order_worker_client, order_entry, correlation_id, MsgType.SAGA_PAYMENT_REVERSE, "Payment")
    elif stock_status == 1: # Stock successful but payment failed, reverse stock
        return await reverse_service(order_worker_client, order_entry, correlation_id, MsgType.SAGA_STOCK_REVERSE, "Stock")

    return create_error_message("Both payment and stock services failed in the SAGA.")

# =========================
# Finalization and Reversal
# =========================

async def finalize_order(db, order_id, order_entry):
    """Finalizes the order if both stock and payment succeed."""
    try:
        order_entry.paid = True
        db.set(order_id, msgpack.encode(order_entry))
        return create_response_message("Checkout successful!", is_json=False)
    except redis.exceptions.RedisError as e:
        return create_error_message(str(e))

async def reverse_service(order_worker_client, order_entry, correlation_id, msg_type, service_name):
    """Handles the response when a service fails, either payment or stock."""
    await order_worker_client.order_fanout_call(
        msg=order_entry,
        msg_type=msg_type,
        correlation_id=correlation_id,
        reply_to=None,
    )
    return create_error_message(f"{service_name} service failed in the SAGA, {service_name} reversed.")

# =====================
# Saga Completion Check
# =====================

async def check_saga_completion(db, order_worker_client, correlation_id):
    """Check if both payment and stock responses are available atomically using a single lock with retries."""
    lock_key = f"saga-{correlation_id}" # Lock key for the saga correlation ID
    if attempt_acquire_locks(db, [lock_key]):
        try:
            # Now that we have acquired the lock, check the status in the hash
            return await process_saga(db, order_worker_client, correlation_id)
        finally:
            # Release the lock after processing
            release_locks(db, [lock_key])
    # If lock could not be acquired after max_retries, return None
    return None