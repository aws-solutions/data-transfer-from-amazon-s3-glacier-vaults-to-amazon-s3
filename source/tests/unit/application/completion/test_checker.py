"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import Any, Dict, Optional
from unittest.mock import Mock, patch

import pytest
from botocore.config import Config
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef, PutItemOutputTypeDef

from solution.application import __boto_config__
from solution.application.completion.checker import (
    check_workflow_completion,
    is_workflow_completed,
)
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
    base_item: Dict[str, Any] = {"count_total": 2, "pk": workflow_run}
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


@patch(
    "solution.application.completion.checker.is_workflow_completed", return_value=None
)
def test_user_agent_on_dynamodb_client(
    _is_workflow_completed_mock: Mock,
    solution_user_agent: str,
) -> None:
    with patch("boto3.client") as mock_client:
        check_workflow_completion("test_workflow_run")

        _config = mock_client.call_args[1]["config"]
        assert type(_config) is Config

        _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
        assert _config_user_agent_extra == __boto_config__.__getattribute__(
            "user_agent_extra"
        )
        assert _config_user_agent_extra == solution_user_agent
