"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
from typing import TYPE_CHECKING, Dict, Tuple

import pytest
from botocore.config import Config

from solution.application import __boto_config__
from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = object


@pytest.fixture()
def dynamodb_accessor_mock() -> DynamoDBAccessor:
    return DynamoDBAccessor(os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME])


@pytest.fixture()
def ddb_table_item() -> Dict[str, Dict[str, str]]:
    return {
        "job_id": {"S": "123"},
        "task_token": {"S": "xadsd"},
        "start_timestamp": {"S": "11:11:11"},
    }


def test_insert_item(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, Dict[str, str]],
) -> None:
    dynamodb_accessor_mock.insert_item(ddb_table_item)
    response = common_dynamodb_table_mock[0].get_item(
        TableName=common_dynamodb_table_mock[1], Key=ddb_table_item
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_item(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, Dict[str, str]],
) -> None:
    common_dynamodb_table_mock[0].put_item(
        TableName=common_dynamodb_table_mock[1], Item=ddb_table_item
    )
    assert dynamodb_accessor_mock.get_item(ddb_table_item) == ddb_table_item


def test_get_item_not_found(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
) -> None:
    assert (
        dynamodb_accessor_mock.get_item(
            {
                "job_id": {"S": "4123"},
                "task_token": {"S": "xadsd"},
                "start_timestamp": {"S": "12:11:11"},
            }
        )
        is None
    )


def test_update_item(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, Dict[str, str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    common_dynamodb_table_mock[0].put_item(
        TableName=common_dynamodb_table_mock[1], Item=ddb_table_item
    )
    update_expression = "set forth_column = :somthing"
    expression_attribute_values = {":somthing": {"S": "Jane Doe"}}
    with caplog.at_level(logging.INFO):
        dynamodb_accessor_mock.update_item(
            ddb_table_item, update_expression, expression_attribute_values
        )
        assert "Successfully updated the database" in caplog.text


def test_delete_item(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, Dict[str, str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    common_dynamodb_table_mock[0].put_item(
        TableName=common_dynamodb_table_mock[1], Item=ddb_table_item
    )
    with caplog.at_level(logging.INFO):
        dynamodb_accessor_mock.delete_item(ddb_table_item)
        assert "Successfully deleted item from the database" in caplog.text


def test_query_items(
    common_dynamodb_table_mock: Tuple[DynamoDBClient, str],
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, Dict[str, str]],
) -> None:
    common_dynamodb_table_mock[0].put_item(
        TableName=common_dynamodb_table_mock[1], Item=ddb_table_item
    )
    expected_items = [ddb_table_item]
    result = dynamodb_accessor_mock.query_items("job_id = :key", {":key": {"S": "123"}})
    assert result == expected_items


def test_user_agent_on_dynamodb_client(
    solution_user_agent: str,
    dynamodb_client: DynamoDBClient,
) -> None:
    ddb_accessor = DynamoDBAccessor("")

    _config = ddb_accessor.dynamodb.meta.config
    assert type(_config) is Config

    _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
    assert _config_user_agent_extra == __boto_config__.__getattribute__(
        "user_agent_extra"
    )
    assert _config_user_agent_extra == solution_user_agent
