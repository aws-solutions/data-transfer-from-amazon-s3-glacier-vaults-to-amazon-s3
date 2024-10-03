"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef

from solution.application.metrics.status_controller import StatusMetricController
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.metric_record import MetricRecord
from solution.application.util.exceptions import MaximumRetryLimitExceeded
from solution.infrastructure.output_keys import OutputKeys

WORKFLOW_RUN_1 = "workflow_run_orchestrator_1"
WORKFLOW_RUN_2 = "workflow_run_orchestrator_2"
ARCHIVE_ID = "test_archive_id"
ARCHIVE_SIZE = 10
ARCHIVE_CHANGED_COUNT = 5


def mock_image(workflow_run: str, status: Optional[str]) -> dict[str, Any]:
    return {
        "pk": {"S": f"{workflow_run}|{ARCHIVE_ID}"},
        "sk": {"S": "meta"},
        "job_id": {"S": "test_job_id"},
        "start_time": {"S": "2023-12-04T22:49:58.903585"},
        "retrieval_type": {"S": "archive-retrieval"},
        "vault_name": {"S": "test_small_vault_archive_range"},
        "archive_id": {"S": ARCHIVE_ID},
        "s3_storage_class": {"S": "GLACIER"},
        "retrieve_status": {"S": f"{workflow_run}/{status}"},
        "file_name": {"S": "test3.txt"},
        "description": {"S": "test3.txt"},
        "size": {"N": str(ARCHIVE_SIZE)},
    }


@pytest.fixture(scope="module")
def mock_records() -> List[dict[str, Any]]:
    return [
        {
            "eventName": event_name,
            "eventSource": "aws:dynamodb",
            "dynamodb": {
                "NewImage": mock_image(workflow_run, to_status),
                "OldImage": mock_image(workflow_run, from_status)
                if event_name == "MODIFY"
                else None,
            },
        }
        for workflow_run, event_name, from_status, to_status in (
            (WORKFLOW_RUN_1, "INSERT", None, GlacierTransferModel.StatusCode.REQUESTED),
            (
                WORKFLOW_RUN_1,
                "MODIFY",
                GlacierTransferModel.StatusCode.REQUESTED,
                GlacierTransferModel.StatusCode.STAGED,
            ),
            (
                WORKFLOW_RUN_1,
                "MODIFY",
                GlacierTransferModel.StatusCode.STAGED,
                GlacierTransferModel.StatusCode.DOWNLOADED,
            ),
            (WORKFLOW_RUN_2, "INSERT", None, GlacierTransferModel.StatusCode.REQUESTED),
            (
                WORKFLOW_RUN_2,
                "MODIFY",
                GlacierTransferModel.StatusCode.STAGED,
                GlacierTransferModel.StatusCode.DOWNLOADED,
            ),
        )
        for _ in range(ARCHIVE_CHANGED_COUNT)
    ]


def test_handle_archive_status_changed(
    dynamodb_client: DynamoDBClient,
    mock_records: List[dict[str, Any]],
    metric_table_mock: CreateTableOutputTypeDef,
) -> None:
    initial_downloaded_count = 10
    initial_downloaded_size = 90
    initial_staged_count = 5
    initial_staged_size = 45

    item_1: Dict[str, Any] = {
        "pk": WORKFLOW_RUN_1,
        "count_downloaded": initial_downloaded_count,
        "size_downloaded": initial_downloaded_size,
        "count_staged": initial_staged_count,
        "size_staged": initial_staged_size,
    }

    metric_record_1: MetricRecord = MetricRecord(**item_1)

    dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=metric_record_1.marshal(),
    )

    item_2: Dict[str, Any] = {
        "pk": WORKFLOW_RUN_2,
        "count_downloaded": 0,
        "size_downloaded": 0,
        "count_staged": 0,
        "size_staged": 0,
    }

    metric_record_2: MetricRecord = MetricRecord(**item_2)

    dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=metric_record_2.marshal(),
    )

    controller = StatusMetricController(records=mock_records)

    controller.handle_archive_status_changed()

    ddb_metric_1 = MetricRecord.parse(
        dynamodb_client.get_item(
            TableName=os.environ[OutputKeys.METRIC_TABLE_NAME], Key=metric_record_1.key
        )["Item"]
    )

    ddb_metric_2 = MetricRecord.parse(
        dynamodb_client.get_item(
            TableName=os.environ[OutputKeys.METRIC_TABLE_NAME], Key=metric_record_2.key
        )["Item"]
    )

    assert (
        ddb_metric_1.count_downloaded
        == initial_downloaded_count + ARCHIVE_CHANGED_COUNT
    )
    assert (
        ddb_metric_1.size_downloaded
        == initial_downloaded_size + ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE
    )
    assert ddb_metric_1.count_staged == initial_staged_count + ARCHIVE_CHANGED_COUNT
    assert (
        ddb_metric_1.size_staged
        == initial_staged_size + ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE
    )

    assert ddb_metric_2.size_requested == ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE

    assert ddb_metric_2.size_downloaded == ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE


@patch("boto3.client")
def test_handle_archive_status_changed_retry(
    boto3_client_mock: MagicMock,
    dynamodb_client: DynamoDBClient,
    mock_records: List[dict[str, Any]],
    metric_table_mock: CreateTableOutputTypeDef,
) -> None:
    controller = StatusMetricController(records=mock_records)
    boto3_client_mock.return_value.transact_write_items.side_effect = Exception(
        "TransactionConflict exception"
    )

    with pytest.raises(MaximumRetryLimitExceeded) as exc:
        controller.handle_archive_status_changed()
    assert (
        str(exc.value)
        == "Maximum retry limit 10 exceeded. Exception: TransactionConflict exception"
    )


def test_token_length(mock_records: List[dict[str, Any]]) -> None:
    controller = StatusMetricController(records=mock_records)
    token = controller._generate_client_request_token(mock_records)

    # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken
    # TransactWriteItems ClientRequestToken max length constraint: 36
    assert len(token) <= 36


def test_increase_archive_status_metric_counter() -> None:
    controller = StatusMetricController(records=[])

    new_image_1 = mock_image(WORKFLOW_RUN_1, GlacierTransferModel.StatusCode.REQUESTED)

    old_image_2 = mock_image(WORKFLOW_RUN_1, GlacierTransferModel.StatusCode.REQUESTED)
    new_image_2 = mock_image(WORKFLOW_RUN_1, GlacierTransferModel.StatusCode.STAGED)

    controller.increase_archive_status_metric_counter(new_image_1)
    controller.increase_archive_status_metric_counter(new_image_2, old_image_2)

    print(controller.workflow_run_metrics[WORKFLOW_RUN_1])

    assert controller.workflow_run_metrics[WORKFLOW_RUN_1]["requested_count"] == 1
    assert (
        controller.workflow_run_metrics[WORKFLOW_RUN_1]["requested_size"]
        == ARCHIVE_SIZE
    )
    assert controller.workflow_run_metrics[WORKFLOW_RUN_1]["staged_count"] == 1
    assert (
        controller.workflow_run_metrics[WORKFLOW_RUN_1]["staged_size"] == ARCHIVE_SIZE
    )
