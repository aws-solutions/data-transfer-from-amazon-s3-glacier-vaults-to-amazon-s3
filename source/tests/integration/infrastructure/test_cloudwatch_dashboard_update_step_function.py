"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import os
import time
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from tests.integration.infrastructure.util import ddb_util, sfn_util

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.model.metric_record import MetricRecord
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object


WORKFLOW_RUN = "workflow_run_cloudwatch_dashboard_update"


@pytest.fixture(scope="module")
def default_input() -> str:
    return json.dumps(
        dict(
            workflow_run=WORKFLOW_RUN,
        )
    )


@pytest.fixture(autouse=True, scope="module")
def setup() -> Any:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.METRIC_TABLE_NAME])
    metric_record = MetricRecord(
        pk=WORKFLOW_RUN,
        count_total=100,
        size_total=10000,
        count_requested=50,
        size_requested=5000,
        count_staged=50,
        size_staged=5000,
        count_downloaded=20,
        size_downloaded=2000,
        count_skipped=30,
        size_skipped=300,
        count_failed=0,
        size_failed=0,
    )

    ddb_accessor.insert_item(metric_record.marshal())
    yield
    ddb_util.delete_all_table_items(os.environ[OutputKeys.METRIC_TABLE_NAME], "pk")


@pytest.fixture(scope="module")
def sfn_execution_arn(default_input: str, sfn_client: SFNClient) -> Any:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_STATE_MACHINE_ARN
        ],
        input=default_input,
    )

    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=60)
    return response["executionArn"]


@pytest.fixture(scope="module")
def sf_history_output(sfn_client: SFNClient, sfn_execution_arn: str) -> Any:
    return sfn_client.get_execution_history(
        executionArn=sfn_execution_arn, maxResults=1000
    )


def test_state_machine_start_execution(
    default_input: str, sfn_client: SFNClient
) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_STATE_MACHINE_ARN
        ]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None


def test_get_metric_from_table_state(sf_history_output: Any) -> None:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.METRIC_TABLE_NAME])
    ddb_item = ddb_accessor.get_item(
        MetricRecord(pk=WORKFLOW_RUN).key,
    )
    events = [
        event
        for event in sf_history_output["events"]
        if "TaskStateExited" in event["type"]
    ]
    for event in events:
        if event["stateExitedEventDetails"]["name"] == "GetMetricsFromMetricTable":
            ddb_state_output = json.loads(event["stateExitedEventDetails"]["output"])[
                "get_metrics"
            ]["Item"]
            assert ddb_item == ddb_state_output
            break
    else:
        pytest.fail("GetMetricsFromMetricTable state not executed in state machine")


def test_put_metric_data_to_cw_state(sf_history_output: Any) -> None:
    events = [
        event
        for event in sf_history_output["events"]
        if "TaskStateExited" in event["type"]
    ]
    for event in events:
        if event["stateExitedEventDetails"]["name"] == "PutMetricsToCloudwatch":
            break
    else:
        pytest.fail("PutMetricsToCloudwatch state not executed in state machine")
