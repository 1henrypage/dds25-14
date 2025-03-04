import asyncio
from quart import Quart, request, jsonify, redirect, abort, Response
import aio_pika
import os
import logging

from common.msg_types import MsgType
from common.request_utils import process_encoded_response_body
from common.queue_utils import RpcClient

app = Quart("order-publisher")

rpc_client: RpcClient = None

@app.before_serving
async def before_serving():
    global rpc_client
    rpc_client = await RpcClient(routing_key=os.environ['ROUTE_KEY'],rabbitmq_url=os.environ['RABBITMQ_URL']).connect()

@app.after_serving
async def after_serving():
    await rpc_client.disconnect()

@app.post('/create/<user_id>')
async def create_order(user_id: str):
    response = await rpc_client.call(msg={"user_id": user_id}, msg_type=MsgType.CREATE)
    return process_encoded_response_body(
        response=response,
    )

@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    response = await rpc_client.call(
        msg={"n": int(n), "n_items": int(n_items), "n_users": int(n_users), "item_price": int(item_price)},
        msg_type=MsgType.BATCH_INIT
    )
    return process_encoded_response_body(
        response=response,
    )

@app.get('/find/<order_id>')
async def find_order(order_id: str):
    response = await rpc_client.call(msg={"order_id": order_id}, msg_type=MsgType.FIND)
    return process_encoded_response_body(
        response=response,
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    response = await rpc_client.call(
        msg={"order_id": order_id, "item_id": item_id, "quantity": int(quantity)},
        msg_type=MsgType.ADD
    )
    return process_encoded_response_body(
        response=response,
    )


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    response = await rpc_client.call(msg={"order_id": order_id}, msg_type=MsgType.CHECKOUT)
    return process_encoded_response_body(
        response=response,
    )


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
