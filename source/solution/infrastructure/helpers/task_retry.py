"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from typing import Any, Dict, List, Optional

from aws_cdk import Duration
from aws_cdk import aws_stepfunctions as sfn


class TaskRetry:
    """
    Helper class for adding retry functionality to StepFunction steps
    Usage:
    - Create a TaskRetry object with your desired retry parameters
    - For states which are subclasses of sfn.TaskStateBase, call task_retry.apply_to_step([step1, step2, step3])
       - For all LambdaInvoke states, set retry_on_service_exceptions=False to avoid including the default Retry functionality
    - For custom states, include the custom_state_params under the `Retry` property
    - The retry should be included as part of the task before constructing the state machine (including chaining tasks with .next())
    """

    def __init__(
        self,
        errors: Optional[List[str]] = None,
        interval: Duration = Duration.seconds(1),
        max_attempts: int = 3,
        backoff_rate: float = 2,
    ):
        if not errors:
            errors = ["States.ALL"]
        self.errors = errors
        self.interval = interval
        self.max_attempts = max_attempts
        self.backoff_rate = backoff_rate

    def custom_state_params(self) -> List[Dict[str, Any]]:
        return [
            {
                "ErrorEquals": self.errors,
                "IntervalSeconds": self.interval.to_seconds(),
                "MaxAttempts": self.max_attempts,
                "BackoffRate": self.backoff_rate,
            }
        ]

    def apply_to_steps(self, steps: List[sfn.TaskStateBase]) -> None:
        for step in steps:
            step.add_retry(**vars(self))
