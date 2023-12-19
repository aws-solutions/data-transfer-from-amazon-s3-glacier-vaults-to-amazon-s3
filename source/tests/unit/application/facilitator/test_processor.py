"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING, Any, Tuple
from unittest import mock

import pytest

from solution.application.facilitator import processor
from solution.application.model.facilitator import AsyncRecord, JobCompletionEvent
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = object


@pytest.fixture
def async_facilitator_table(
    dynamodb_client: DynamoDBClient,
) -> Tuple[DynamoDBClient, str]:
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    try:
        dynamodb_client.describe_table(TableName=table_name)
        dynamodb_client.delete_table(TableName=table_name)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        pass
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "job_id", "AttributeType": "S"},
        ],
        TableName=os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME],
        KeySchema=[
            {"AttributeName": "job_id", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return (dynamodb_client, table_name)


@pytest.fixture
def job_id() -> str:
    return "testing"


@pytest.fixture
def completion_date() -> str:
    return "2012-06-12T22:20:40.790Z"


@pytest.fixture
def workflow_run() -> str:
    return "workflow_run_test"


@pytest.fixture(params=[True, False], ids=["complete", "incomplete"])
def completed(request: pytest.FixtureRequest) -> bool:
    return bool(getattr(request, "param"))


@pytest.fixture(
    params=[
        value
        for key, value in JobCompletionEvent.StatusCode.__dict__.items()
        if not key.startswith("_")
    ]
)
def status_code(request: pytest.FixtureRequest) -> str:
    return str(getattr(request, "param"))


@pytest.fixture
def job_completion_event(
    job_id: str,
    completed: bool,
    status_code: str,
    workflow_run: str,
    completion_date: str,
) -> str:
    return json.dumps(
        {
            "JobId": job_id,
            "Completed": completed,
            "workflow_run": workflow_run,
            "StatusCode": status_code
            if completed
            else JobCompletionEvent.StatusCode.IN_PROGRESS,
        }
        | ({"CompletionDate": completion_date} if completed else {})
    )


@pytest.fixture
def task_tracking_record(
    job_id: str,
    completion_date: str,
    job_completion_event: str,
    workflow_run: str,
    completed: bool,
) -> dict[str, Any]:
    return {
        "job_id": {"S": job_id},
        "task_token": {"S": "task_token"},
        "workflow_run": {"S": workflow_run},
    } | (
        {
            "job_result": {"S": job_completion_event},
            "finish_timestamp": {"S": completion_date},
        }
        if completed
        else {}
    )


@pytest.fixture
def existing_record(
    request: pytest.FixtureRequest,
    async_facilitator_table: Tuple[DynamoDBClient, str],
    job_id: str,
) -> None:
    create_record = bool(getattr(request, "param"))
    if create_record:
        async_facilitator_table[0].put_item(
            TableName=async_facilitator_table[1], Item={"job_id": {"S": job_id}}
        )


@pytest.mark.parametrize(
    "existing_record", [True, False], ids=["exists", "absent"], indirect=True
)
def test_handle_job_notification(
    existing_record: None,
    job_completion_event: str,
    completed: bool,
    async_facilitator_table: Tuple[DynamoDBClient, str],
    job_id: str,
    completion_date: str,
) -> None:
    before = (
        async_facilitator_table[0]
        .get_item(TableName=async_facilitator_table[1], Key={"job_id": {"S": job_id}})
        .get("Item", {})
    )

    processor.handle_job_notification(job_completion_event)

    after = (
        async_facilitator_table[0]
        .get_item(TableName=async_facilitator_table[1], Key={"job_id": {"S": job_id}})
        .get("Item", {})
    )

    if completed:
        assert after | before == after
        assert after["finish_timestamp"]["S"] == completion_date
        assert after["job_result"]["S"] == job_completion_event
    else:
        assert before == after
        assert "finish_timestamp" not in after
        assert "job_result" not in after


def test_handle_record_changed(
    monkeypatch: pytest.MonkeyPatch,
    job_completion_event: str,
    task_tracking_record: dict[str, Any],
    status_code: str,
    completed: bool,
) -> None:
    client = mock.Mock()
    monkeypatch.setattr(processor, "get_sfn_client", lambda: client)

    processor.handle_record_changed(task_tracking_record)

    event = JobCompletionEvent.parse(
        json.loads(job_completion_event), raw_message=job_completion_event
    )
    record = AsyncRecord.parse(task_tracking_record)

    if not completed:
        assert len(client.method_calls) == 0
    elif status_code == JobCompletionEvent.StatusCode.SUCCEEDED:
        assert len(client.method_calls) == 1
        client.send_task_success.assert_called_once_with(
            taskToken=record.task_token, output=job_completion_event
        )
    else:
        assert len(client.method_calls) == 1
        client.send_task_failure.assert_called_once_with(
            taskToken=record.task_token,
            error=event.status_code,
            cause=job_completion_event,
        )
