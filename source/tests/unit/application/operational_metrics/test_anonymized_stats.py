"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING, Any, Dict
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from solution.application.model.metric_record import MetricRecord
from solution.application.model.workflow_metadata_model import WorkflowMetadataRecord
from solution.application.operational_metrics.anonymized_stats import (
    StatsType,
    query_metric,
    send_job_stats,
    send_stats,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
else:
    DynamoDBClient = object
    CreateTableOutputTypeDef = object


def mock_anonymized_data() -> Dict[str, Any]:
    return {
        "Solution": "TestSolutionID",
        "UUID": ANY,
        "TimeStamp": ANY,
        "Data": {
            "Region": "us-east-1",
            "Version": "test_version",
            "StorageClass": "test_storage_class",
            "RetrievalTier": "test_retrieval_tier",
            "StartTime": "test_start_time",
            "StorageClass": "Glacier",
            "RetrievalTier": "Bulk",
            "DailyQuota": 10000,
            "ProvidedInventory": "NO",
            "TransferType": "LAUNCH",
            "CrossRegionTransfer": "False",
            "NamingOverrideFile": "NO",
            "VaultSize": 100,
            "ArchiveCount": 10,
        },
    }


@pytest.fixture
def anonymized_submission_data() -> Dict[str, Any]:
    return mock_anonymized_data()


@pytest.fixture
def anonymized_completion_data() -> Dict[str, Any]:
    data = mock_anonymized_data()
    data["Data"]["FinishTime"] = ANY
    data["Data"]["DownloadedSize"] = 90
    data["Data"]["DownloadedCount"] = 9
    return data


def test_query_metric(
    metric_table_mock: CreateTableOutputTypeDef,
    dynamodb_client: DynamoDBClient,
    anonymized_submission_data: Dict[str, Any],
) -> None:
    workflow_run = "sample_workflow"
    metric_record = MetricRecord(
        pk=workflow_run,
        size_total=anonymized_submission_data["Data"]["VaultSize"],
        count_total=anonymized_submission_data["Data"]["ArchiveCount"],
    )
    dynamodb_client.put_item(
        TableName=os.environ["MetricTableName"],
        Item=metric_record.marshal(),
    )
    record = query_metric(workflow_run)
    assert record is not None
    assert record.marshal() == metric_record.marshal()


@pytest.mark.parametrize(
    "stats_type",
    [
        (StatsType.SUBMISSION),
        (StatsType.COMPLETION),
    ],
)
def test_send_job_stats(
    metric_table_mock: CreateTableOutputTypeDef,
    glacier_retrieval_table_mock: CreateTableOutputTypeDef,
    dynamodb_client: DynamoDBClient,
    anonymized_submission_data: Dict[str, Any],
    anonymized_completion_data: Dict[str, Any],
    stats_type: str,
) -> None:
    workflow_run = "sample_workflow"
    dynamodb_client.put_item(
        TableName=os.environ["MetricTableName"],
        Item=MetricRecord(
            pk=workflow_run,
            size_total=anonymized_submission_data["Data"]["VaultSize"],
            count_total=anonymized_submission_data["Data"]["ArchiveCount"],
            size_downloaded=anonymized_completion_data["Data"]["DownloadedSize"],
            count_downloaded=anonymized_completion_data["Data"]["DownloadedCount"],
        ).marshal(),
    )
    dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Item=WorkflowMetadataRecord(
            workflow_run=workflow_run,
            vault_name="test_vault_name",
            start_time=anonymized_submission_data["Data"]["StartTime"],
            daily_quota=anonymized_submission_data["Data"]["DailyQuota"],
            storage_class=anonymized_submission_data["Data"]["StorageClass"],
            retrieval_tier=anonymized_submission_data["Data"]["RetrievalTier"],
            provided_inventory=anonymized_submission_data["Data"]["ProvidedInventory"],
            transfer_type=anonymized_submission_data["Data"]["TransferType"],
            naming_override_file="",
            cross_region_transfer=anonymized_submission_data["Data"][
                "CrossRegionTransfer"
            ],
        ).marshal(),
    )

    # Replace 'send_stats' with a mock that captures the input data
    with patch(
        "solution.application.operational_metrics.anonymized_stats.send_stats"
    ) as mock_send_stats:
        with patch(
            "solution.application.operational_metrics.anonymized_stats.os.environ",
            {
                "SEND_ANONYMIZED_STATISTICS": "Yes",
                "ACCOUNT_ID": "Test",
                "SOLUTION_ID": "TestSolutionID",
                "REGION": anonymized_submission_data["Data"]["Region"],
                "VERSION": anonymized_submission_data["Data"]["Version"],
                OutputKeys.METRIC_TABLE_NAME: os.environ[OutputKeys.METRIC_TABLE_NAME],
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: os.environ[
                    OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME
                ],
            },
        ):
            send_job_stats(stats_type, workflow_run)

        anonymized_data = (
            anonymized_completion_data
            if stats_type == StatsType.COMPLETION
            else anonymized_submission_data
        )
        mock_send_stats.assert_called_with(anonymized_data)


def test_send_stats_with_toggle_on(anonymized_submission_data: Dict[str, Any]) -> None:
    # Replace 'os.environ' to control the behavior of 'send_stats'
    with patch(
        "solution.application.operational_metrics.anonymized_stats.os.environ",
        {"SEND_ANONYMIZED_STATISTICS": "Yes"},
    ):
        # Replace 'urllib.request.urlopen' with a mock that captures the request
        with patch("urllib.request.urlopen") as mock_request:
            anonymized_submission_data["UUID"] = "TestUUID"
            anonymized_submission_data["TimeStamp"] = "TestTimestamp"
            expected_data = json.dumps(anonymized_submission_data).encode("utf-8")
            send_stats(anonymized_submission_data)

            # Verify that 'urllib.request.urlopen' is called with the expected arguments
            mock_request.assert_called()
            args, _ = mock_request.call_args
            assert args[0].data == expected_data


def test_send_stats_with_toggle_off(anonymized_submission_data: Dict[str, Any]) -> None:
    # Replace 'os.environ' to control the behavior of 'send_stats'
    with patch(
        "solution.application.operational_metrics.anonymized_stats.os.environ",
        {"SEND_ANONYMIZED_STATISTICS": "No"},
    ):
        # Replace 'urllib.request.urlopen' with a mock that captures the request
        with patch("urllib.request.urlopen") as mock_request:
            mock_response = Mock()
            mock_request.return_value = mock_response
            send_stats(anonymized_submission_data)

            # Verify that 'urllib.request.urlopen' is not called
            mock_request.assert_not_called()
