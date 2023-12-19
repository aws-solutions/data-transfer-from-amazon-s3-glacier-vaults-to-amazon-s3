"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import builtins
from typing import Any, Dict, List, Optional, Sequence

from aws_cdk import aws_stepfunctions as sfn


class TaskCatch:
    """
    Helper class for adding catch functionality to StepFunction steps
    Usage:
    - Create a CatchRetry object with your desired retry parameters
    - For states which are subclasses of sfn.TaskStateBase, call task_retry.apply_to_step([step1, step2, step3])
    - For custom states, include the custom_state_params under the `Catch` property
    - The catch should be included as part of the task before constructing the state machine (including chaining tasks with .next())
    """

    def __init__(
        self,
        handler: sfn.IChainable,
        errors: Optional[Sequence[builtins.str]] = None,
        result_path: Optional[builtins.str] = None,
    ):
        if not errors:
            errors = ["States.ALL"]
        self.errors = errors
        self.handler = handler
        self.result_path = result_path

    def custom_state_params(self) -> List[Dict[str, Any]]:
        params = {"ErrorEquals": self.errors, "Next": self.handler.id}
        if self.result_path:
            params["ResultPath"] = self.result_path
        return [params]

    def apply_to_steps(self, steps: List[sfn.TaskStateBase]) -> None:
        for step in steps:
            step.add_catch(**vars(self))
