import asyncio
from quart import Quart, request, jsonify, redirect, abort, Response
import aio_pika
import sys
import os
import logging

from common.msg_types import MsgType
from common.request_utils import process_encoded_response_body
from common.queue_utils import RpcClient

app = Quart("payment-publisher")

rpc_client: RpcClient = None

allow_flag = True


@app.before_serving
async def before_serving():
    global rpc_client
    rpc_client = await RpcClient(routing_key=os.environ['ROUTE_KEY'],rabbitmq_url=os.environ['RABBITMQ_URL']).connect()

@app.after_serving
async def after_serving():
    await rpc_client.disconnect()


@app.post('/create_user')
async def create_user():
    response = await rpc_client.call(msg={}, msg_type=MsgType.CREATE)
    return process_encoded_response_body(
        response=response
    )


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    response = await rpc_client.call(
        msg={"n": int(n), "starting_money": int(starting_money)},
        msg_type=MsgType.BATCH_INIT
    )
    return process_encoded_response_body(
        response=response,
    )


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    response = await rpc_client.call(msg={"user_id": user_id}, msg_type=MsgType.FIND)
    return process_encoded_response_body(
        response=response,
    )

@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    response = await rpc_client.call(
        msg={"user_id": user_id, "total_cost": int(amount)},
        msg_type=MsgType.ADD
    )
    return process_encoded_response_body(
        response=response,
    )

@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    response = await rpc_client.call(
        msg={"user_id": user_id, "total_cost": int(amount)},
        msg_type=MsgType.SUBTRACT
    )
    return process_encoded_response_body(
        response=response,
    )



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
