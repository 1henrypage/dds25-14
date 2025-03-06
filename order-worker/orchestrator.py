from enum import Enum


class SagaStep:
    """Defines a step in the saga."""
    def __init__(self, name: str, action: callable, compensating_action: callable):
        self.name = name
        self.action = action
        self.compensating_action = compensating_action

class Outcome(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'

class SagaOrchestrator:
    """Orchestrates a series of saga steps."""
    def __init__(self, steps: list[SagaStep]):
        self.steps = steps
        self.completed_steps = []

    def execute(self):
        """Executes the saga steps and handles failures/compensations."""
        for step in self.steps:
            result, res = self._execute_step(step)
            if result == Outcome.FAILURE:
                self._compensate()
                return Outcome.FAILURE, res
        return Outcome.SUCCESS, None

    def _execute_step(self, step: SagaStep):
        """Executes an individual step of the saga."""
        res = step.action()
        if res['status'] == 200:
            self.completed_steps.append(step)
            return Outcome.SUCCESS, None
        return Outcome.FAILURE, res

    def _compensate(self):
        """Compensates previously executed steps."""
        for step in self.completed_steps:
            step.compensating_action() # TODO handle retries?
