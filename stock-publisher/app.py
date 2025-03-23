from quart import Quart, request, jsonify, redirect, abort, Response
import os
import logging

from common.msg_types import MsgType
from common.request_utils import process_encoded_response_body
from common.queue_utils import RpcClient

app = Quart("stock-publisher")

rpc_client: RpcClient = None

@app.before_serving
async def before_serving():
    global rpc_client
    rpc_client = await RpcClient(routing_key=os.environ['ROUTE_KEY'],rabbitmq_url=os.environ['RABBITMQ_URL']).connect()

@app.after_serving
async def after_serving():
    await rpc_client.disconnect()

@app.post('/item/create/<price>')
async def create_item(price: int):
    response = await rpc_client.call(msg={"price": price}, msg_type=MsgType.CREATE)
    return process_encoded_response_body(
        response=response,
    )


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    response = await rpc_client.call(
        msg={"n": n, "starting_stock": starting_stock, "item_price": item_price},
        msg_type=MsgType.BATCH_INIT
    )
    return process_encoded_response_body(response=response)


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    response = await rpc_client.call(msg={"item_id": item_id}, msg_type=MsgType.FIND)
    return process_encoded_response_body(response=response)

@app.get('/find/<item_id>/priority')
async def find_item_priority(item_id: str):
    response = await rpc_client.call(msg={"item_id": item_id}, msg_type=MsgType.FIND_PRIORITY)
    return process_encoded_response_body(response=response)


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    response = await rpc_client.call(msg={"item_id": item_id, "total_cost": amount}, msg_type=MsgType.ADD)
    return process_encoded_response_body(response=response)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    response = await rpc_client.call(msg={"item_id": item_id, "total_cost": amount}, msg_type=MsgType.SUBTRACT)
    return process_encoded_response_body(response=response)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
