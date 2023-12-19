"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
from typing import Any, Dict, List

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef

from solution.application.metrics.status_controller import StatusMetricController
from solution.application.model.metric_record import MetricRecord
from solution.infrastructure.output_keys import OutputKeys

WORKFLOW_RUN = "workflow_run_orchestrator"
ARCHIVE_ID = "test_archive_id"
ARCHIVE_SIZE = 10
ARCHIVE_CHANGED_COUNT = 5


def mock_image(status: str) -> dict[str, Any]:
    return {
        "pk": {"S": f"{WORKFLOW_RUN}|{ARCHIVE_ID}"},
        "sk": {"S": "meta"},
        "job_id": {"S": "test_job_id"},
        "start_time": {"S": "2023-12-04T22:49:58.903585"},
        "retrieval_type": {"S": "archive-retrieval"},
        "vault_name": {"S": "test_small_vault_archive_range"},
        "archive_id": {"S": ARCHIVE_ID},
        "s3_storage_class": {"S": "GLACIER"},
        "retrieve_status": {"S": f"{WORKFLOW_RUN}/{status}"},
        "file_name": {"S": "test3.txt"},
        "description": {"S": "test3.txt"},
        "size": {"N": str(ARCHIVE_SIZE)},
    }


@pytest.fixture(scope="module")
def mock_records() -> List[dict[str, Any]]:
    return [
        {
            "eventName": "MODIFY",
            "eventSource": "aws:dynamodb",
            "dynamodb": {
                "NewImage": mock_image(to_status),
                "OldImage": mock_image(from_status),
            },
        }
        for from_status, to_status in (
            ("requested", "staged"),
            ("staged", "downloaded"),
        )
        for _ in range(ARCHIVE_CHANGED_COUNT)
    ]


def test_handle_archive_status_changed(
    dynamodb_client: DynamoDBClient,
    mock_records: List[dict[str, Any]],
    metric_table_mock: CreateTableOutputTypeDef,
) -> None:
    initiale_downloaded_count = 10
    initiale_downloaded_size = 90
    initiale_staged_count = 5
    initiale_staged_size = 45

    item: Dict[str, Any] = {
        "pk": WORKFLOW_RUN,
        "count_downloaded": initiale_downloaded_count,
        "size_downloaded": initiale_downloaded_size,
        "count_staged": initiale_staged_count,
        "size_staged": initiale_staged_size,
    }

    metric_record: MetricRecord = MetricRecord(**item)

    dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME], Item=metric_record.marshal()
    )

    controller = StatusMetricController(records=mock_records)

    controller.handle_archive_status_changed()

    ddb_metric = MetricRecord.parse(
        dynamodb_client.get_item(
            TableName=os.environ[OutputKeys.METRIC_TABLE_NAME], Key=metric_record.key
        )["Item"]
    )

    assert (
        ddb_metric.count_downloaded == initiale_downloaded_count + ARCHIVE_CHANGED_COUNT
    )
    assert (
        ddb_metric.size_downloaded
        == initiale_downloaded_size + ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE
    )
    assert ddb_metric.count_staged == initiale_staged_count + ARCHIVE_CHANGED_COUNT
    assert (
        ddb_metric.size_staged
        == initiale_staged_size + ARCHIVE_CHANGED_COUNT * ARCHIVE_SIZE
    )
