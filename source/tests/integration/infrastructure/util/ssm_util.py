"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import time
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
else:
    SSMClient = object


def ssm_client() -> SSMClient:
    if "AWS_PROFILE" in os.environ and "AWS_REGION" in os.environ:
        session = boto3.Session(
            profile_name=os.environ["AWS_PROFILE"],
            region_name=os.environ["AWS_REGION"],
        )
    else:
        session = boto3.Session()

    client: SSMClient = session.client("ssm")
    return client


def poll_execution_status(execution_id: str) -> str:
    client: SSMClient = ssm_client()
    start_time = time.time()
    while (time.time() - start_time) < 90:
        automation_response = client.get_automation_execution(
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
