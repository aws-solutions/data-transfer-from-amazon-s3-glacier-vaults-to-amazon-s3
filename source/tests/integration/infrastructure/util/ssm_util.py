"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import time
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
else:
    SSMClient = object


def poll_execution_status(execution_id: str) -> str:
    ssm_client: SSMClient = boto3.client("ssm")
    start_time = time.time()
    while (time.time() - start_time) < 90:
        automation_response = ssm_client.get_automation_execution(
            AutomationExecutionId=execution_id
        )
        if automation_response["AutomationExecution"]["AutomationExecutionStatus"] in [
            "Success",
            "Failed",
        ]:
            return automation_response["AutomationExecution"][
                "AutomationExecutionStatus"
            ]
        time.sleep(10)
    else:
        return "Test TimedOut"
