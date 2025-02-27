import asyncio
from quart import Quart, request, jsonify
import aio_pika
import os
import logging

app = Quart("order-publisher")

RABBITMQ_URL = os.environ['RABBITMQ_URL']
RABBITMQ_PORT = int(os.environ['RABBITMQ_PORT'])
OUTBOUND_QUEUE = os.environ['OUTBOUND_QUEUE']
INBOUND_QUEUE = os.environ['INBOUND_QUEUE']

@app.before_serving
async def startup():
    app.rabbit_connection = await aio_pika.connect_robust(url=RABBITMQ_URL)
    app.rabbit_channel = await app.rabbit_connection.channel()
    await app.rabbit_channel.set_qos(prefetch_count=1)










if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)





