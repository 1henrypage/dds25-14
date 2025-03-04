import logging
from typing import Any

import requests
from quart import Response, abort, jsonify
from msgspec import msgpack


def process_encoded_response_body(response):
    """
    Unencodes a response using msgpack and translates responses into Quart lifecycle events (success response, abort)

    :param response: The encoded respones to process
    :return: Quart responses
    """
    response_body = msgpack.decode(response)

    status_code = int(response_body['status'])

    # Check if the status code is a 4xx error
    if 400 <= status_code < 500:
        # Handle the client error by aborting with the appropriate status and message
        return abort(status_code, response_body['error'])

    if response_body['is_json']:
        return jsonify(response_body.get("content", "{}"))
    else:
        return Response(response_body.get("content", ""), status=status_code)

def create_response_message(content: Any, is_json: bool, status: int = 200):
    """
    Wrapper function to wrap content along with its status and whether the final clientside response
    should be rendered as json

    :param content: The actual content coming from the worker
    :param is_json: Whether the client-side should be rendered as json
    :param status: The HTTP status code
    :return: A wrapped KV object with all of this information
    """
    return {
        "content": content,
        "status": status,
        "is_json": is_json
    }

def create_error_message(error: Any, status: int = 400):
    """
    Wrapper function to wrap an error

    :param error: The error message
    :param status: Should be 400 <= x < 500. I can't think of any other cases where it's different
    :return: Wrapped error message
    """
    return {
        "error": error,
        "status": status,
    }

