import logging

import requests
from quart import Response, abort, jsonify
from msgspec import msgpack


def process_encoded_response_body(response):
    response_body = msgpack.decode(response)

    status_code = int(response_body['status'])


    # Check if the status code is a 4xx error
    if 400 <= status_code < 500:
        logging.error("YO3")
        # Handle the client error by aborting with the appropriate status and message
        abort(status_code, response_body['error'])


    if response_body['is_json']:
        return jsonify(response_body.get("content", "{}"))
    else:
        logging.error("YO6")
        return Response(response_body.get("content", ""), status=status_code)

def create_response_message(content, is_json, status: int = 200):
    return {
        "content": content,
        "status": status,
        "is_json": is_json
    }

def create_error_message(error, status: int = 400):
    return {
        "error": error,
        "status": status,
    }

