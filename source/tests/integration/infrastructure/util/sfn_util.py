"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import time
from typing import TYPE_CHECKING, Any, Dict, List

import boto3

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object
    S3Client = object


def get_state_machine_output(executionArn: str, timeout: int) -> str:
    client: SFNClient = boto3.client("stepfunctions")
    start_time = time.time()
    sf_output: str = "TIMEOUT EXCEEDED"
    while (time.time() - start_time) < timeout:
        time.sleep(1)
        sf_describe_response = client.describe_execution(executionArn=executionArn)
        status = sf_describe_response["status"]
        if status == "RUNNING":
            continue
        elif status == "SUCCEEDED":
            sf_output = sf_describe_response["output"]
            break
        else:
            # for status: FAILED, TIMED_OUT or ABORTED
            raise Exception(f"Execution {status}")

    return sf_output


def wait_till_state_machine_finish(executionArn: str, timeout: int) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        time.sleep(1)
        sf_describe_response = client.describe_execution(executionArn=executionArn)
        status = sf_describe_response["status"]
        if status == "RUNNING":
            continue
        break


def nested_distributed_map_navigator(
    execution_arn: str, state_name_to_state_type: Dict[str, Any]
) -> Any:
    client: SFNClient = boto3.client("stepfunctions")
    for index, (name, state_type) in enumerate(state_name_to_state_type.items()):
        sf_history_output = client.get_execution_history(
            executionArn=execution_arn, maxResults=1000
        )
        event = [
            event
            for event in sf_history_output["events"]
            if state_type in event["type"]
            and event["stateExitedEventDetails"]["name"] == name
        ][0]
        if not event:
            raise AssertionError(f"{name}: map failed to run successfully.")

        event_output = json.loads(event["stateExitedEventDetails"]["output"])
        if index == len(state_name_to_state_type) - 1:
            return event_output

        retrieve_archive_distributed_map_executions = client.list_executions(
            mapRunArn=event_output["map_result"]["MapRunArn"]
        )
        execution_arn = retrieve_archive_distributed_map_executions["executions"][0][
            "executionArn"
        ]

    return dict()


def stop_execution_running_sfn(state_machine_arns: List[str]) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    for machine_arn in state_machine_arns:
        response = client.list_executions(
            stateMachineArn=machine_arn, statusFilter="RUNNING"
        )
        for execution in response["executions"]:
            client.stop_execution(executionArn=execution["executionArn"])
