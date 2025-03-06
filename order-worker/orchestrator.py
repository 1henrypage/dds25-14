from enum import Enum

import redis
import asyncio

from common.request_utils import create_error_message

class SagaStep:
    """Defines a step in the saga."""
    def __init__(self, name: str, action: callable, compensating_action: callable):
        self.name = name
        self.action = action
        self.compensating_action = compensating_action

class Outcome(str, Enum):
    SUCCESS = 'Success'
    FAILURE = 'Failure'
    WAITING = 'Waiting'

class Status(str, Enum):
    PENDING = 'P'
    SUCCESS = 'S'
    REJECTED = 'R'

class SagaOrchestrator:
    """Orchestrates a series of saga steps."""
    def __init__(self, steps: list[SagaStep], request_id, redis_client, forgetful_client):
        self.steps = steps
        self.request_id = request_id
        self.redis_client = redis_client
        self.forgetful_client = forgetful_client

    def execute(self):
        """Executes the saga steps and handles failures/compensations."""
        for step in self.steps:
            try:
                self.redis_client.set(f'{self.request_id}-{step.name}', Status.PENDING)
            except redis.exceptions.RedisError as e:
                return create_error_message(str(e))
            asyncio.create_task(step.action())

    async def handle_response(self, response_msg):
        """Handles the response from a worker."""
        status = response_msg['status']
        if status != 200:
            await self._compensate()
            return Outcome.FAILURE, response_msg

        name = response_msg['step_name']

        # Check if all steps are completed
        for step in self.steps:
            if step.name == name:
                try:
                    self.redis_client.set(f'{self.request_id}-{name}', Status.SUCCESS)
                except redis.exceptions.RedisError as e:
                    return create_error_message(str(e))
            else:
                redis_key = f'{self.request_id}-{step.name}'
                if self.redis_client.get(redis_key) != Status.SUCCESS:
                    return Outcome.WAITING, response_msg
        return Outcome.SUCCESS, response_msg

    async def _compensate(self):
        """Compensates executed steps."""
        for step in self.steps:
            await step.compensating_action() # TODO handle retries?