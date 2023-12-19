"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest

from solution.infrastructure.glue_helper.scripts.metric_collection_script import (
    update_metric_table,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
else:
    DynamoDBClient = object
    CreateTableOutputTypeDef = object


@pytest.fixture
def mock_dfc() -> Mock:
    mock_row_valid = Mock()
    mock_row_valid.TotalArchivesNumber = 100
    mock_row_valid.TotalArchivesSize = 2000

    # Mock for archives <= 5TB DataFrame
    mock_df_valid = Mock()
    mock_df_valid.collect.return_value = [mock_row_valid]
    mock_df_valid.toDF.return_value = mock_df_valid

    mock_row_skipped = Mock()
    mock_row_skipped.TotalArchivesNumber = 1
    mock_row_skipped.TotalArchivesSize = 20

    # Mock for archives > 5TB DataFrame
    mock_df_skipped = Mock()
    mock_df_skipped.collect.return_value = [mock_row_skipped]
    mock_df_skipped.toDF.return_value = mock_df_skipped

    # Mock for dfc.values()
    mock_values = Mock()
    mock_values.values.return_value = [mock_df_valid, mock_df_skipped]
    return mock_values


def test_update_metric_table_new_run(
    metric_table_mock: CreateTableOutputTypeDef,
    mock_dfc: Any,
    dynamodb_client: DynamoDBClient,
) -> None:
    update_metric_table("test_pk", "MetricTable", "LAUNCH", mock_dfc)
    response = dynamodb_client.get_item(
        TableName="MetricTable",
        Key={
            "pk": {"S": "test_pk"},
            "count_total": {"N": "100"},
            "size_total": {"N": "2000"},
        },
    )
    assert response["Item"]["count_total"]["N"] == "100"
    assert response["Item"]["size_total"]["N"] == "2000"
    assert response["Item"]["count_skipped"]["N"] == "1"
    assert response["Item"]["size_skipped"]["N"] == "20"


def test_update_metric_table_resume(
    metric_table_mock: CreateTableOutputTypeDef,
    mock_dfc: Any,
    dynamodb_client: DynamoDBClient,
) -> None:
    dynamodb_client.put_item(
        TableName="MetricTable",
        Item={
            "pk": {"S": "test_pk"},
            "count_total": {"N": "100"},
            "size_total": {"N": "2000"},
            "count_staged": {"N": "80"},
            "size_staged": {"N": "1600"},
            "count_requested": {"N": "80"},
            "size_requested": {"N": "1600"},
            "count_downloaded": {"N": "50"},
            "size_downloaded": {"N": "1000"},
        },
    )

    update_metric_table("test_pk", "MetricTable", "RESUME", mock_dfc)
    response = dynamodb_client.get_item(
        TableName="MetricTable",
        Key={
            "pk": {"S": "test_pk"},
        },
    )
    response["Item"]["count_requested"]["N"] == "50"
    response["Item"]["size_requested"]["N"] == "1000"
    response["Item"]["count_staged"]["N"] == "50"
    response["Item"]["size_staged"]["N"] == "1000"
