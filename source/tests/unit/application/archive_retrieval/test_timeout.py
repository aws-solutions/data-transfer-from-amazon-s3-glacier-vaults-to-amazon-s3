"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import datetime
import os
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
from unittest import mock

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef

from solution.application.archive_retrieval.timeout import calculate_timeout
from solution.application.model.partition_metric_record import PartitionMetricRecord
from solution.infrastructure.output_keys import OutputKeys


@pytest.fixture
def start_time() -> datetime.datetime:
    return datetime.datetime(year=2022, month=1, day=1, hour=12, minute=0)


WORKFLOW_RUN = "test_workflow_run"


@pytest.fixture
def glacier_retrieval_item() -> Dict[str, Any]:
    return {
        "pk": {"S": WORKFLOW_RUN},
        "sk": {"S": "meta"},
        "daily_quota": {"N": "1"},
    }


def partition_record(
    start_time: datetime.datetime, workflow_name: str
) -> PartitionMetricRecord:
    return PartitionMetricRecord(
        pk=f"{workflow_name}|PARTITION",
        archives_count=3,
        archives_size=12,
        start_time=start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )


def test_calculate_timeout_no_start_time(
    metric_table_mock: CreateTableOutputTypeDef,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    ddb_client, _ = glacier_retrieval_table_mock
    partition_record = PartitionMetricRecord(
        pk=f"{WORKFLOW_RUN}|PARTITION", archives_count=3, archives_size=12
    )
    ddb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=partition_record.marshal(),
    )
    assert calculate_timeout(WORKFLOW_RUN) == 0


def test_calculate_timeout_with_no_metadata(
    metric_table_mock: CreateTableOutputTypeDef,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    start_time: datetime.datetime,
) -> None:
    ddb_client, _ = glacier_retrieval_table_mock
    ddb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=partition_record(start_time, WORKFLOW_RUN).marshal(),
    )
    assert calculate_timeout(WORKFLOW_RUN) == 0


def test_calculate_negative_timeout(
    metric_table_mock: CreateTableOutputTypeDef,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    start_time: datetime.datetime,
    glacier_retrieval_item: Dict[str, Any],
) -> None:
    ddb_client, glacier_retrieval_table_name = glacier_retrieval_table_mock
    ddb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=partition_record(start_time, WORKFLOW_RUN).marshal(),
    )
    ddb_client.put_item(
        TableName=glacier_retrieval_table_name,
        Item=glacier_retrieval_item,
    )

    with mock.patch("solution.application.archive_retrieval.timeout.datetime") as dt:
        end_time = datetime.datetime(year=2023, month=1, day=1, hour=12, minute=1)
        dt.now.return_value = end_time
        dt.strptime.return_value = start_time
        assert calculate_timeout(WORKFLOW_RUN) == 0


def test_calculate_timeout_success(
    metric_table_mock: CreateTableOutputTypeDef,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    start_time: datetime.datetime,
    glacier_retrieval_item: Dict[str, Any],
) -> None:
    ddb_client, glacier_retrieval_table_name = glacier_retrieval_table_mock
    end_time = datetime.datetime(year=2022, month=1, day=1, hour=12, minute=0)

    ddb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item=partition_record(start_time, WORKFLOW_RUN).marshal(),
    )
    ddb_client.put_item(
        TableName=glacier_retrieval_table_name,
        Item=glacier_retrieval_item,
    )

    with mock.patch("solution.application.archive_retrieval.timeout.datetime") as dt:
        dt.now.return_value = end_time
        dt.strptime.return_value = start_time
        assert calculate_timeout(WORKFLOW_RUN) != 0
