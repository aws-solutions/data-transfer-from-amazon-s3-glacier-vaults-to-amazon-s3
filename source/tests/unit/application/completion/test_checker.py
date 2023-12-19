"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING, Any, Dict, Optional

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef, PutItemOutputTypeDef

from solution.application.completion.checker import is_workflow_completed
from solution.application.model.metric_record import MetricRecord
from solution.infrastructure.output_keys import OutputKeys


@pytest.mark.parametrize(
    "workflow_run, count_downloaded, result",
    [
        ("test_with_no_download", None, False),
        ("test_with_all_downloaded", 2, True),
        ("test_with_larger_count_downloaded", 3, True),
        ("test_with_not_all_downloaded", 1, False),
    ],
)
def test_is_workflow_completed(
    workflow_run: str,
    count_downloaded: Optional[int],
    result: bool,
    metric_table_mock: CreateTableOutputTypeDef,
    dynamodb_client: DynamoDBClient,
) -> None:
    base_item: Dict[str, Any] = {"count_total": 2}
    base_item["pk"] = workflow_run
    if count_downloaded:
        base_item["count_downloaded"] = count_downloaded

    metric_table_put_item(metric_table_mock, base_item, dynamodb_client)
    assert is_workflow_completed(workflow_run, dynamodb_client) == result


def metric_table_put_item(
    metric_table_mock: CreateTableOutputTypeDef,
    item: Dict[str, Any],
    dynamodb_client: DynamoDBClient,
) -> PutItemOutputTypeDef:
    metric_record: MetricRecord = MetricRecord(**item)

    return dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME], Item=metric_record.marshal()
    )
