"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import time
import uuid
from typing import TYPE_CHECKING

import boto3

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBClient = object
    Table = object


def test_table_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    key = {"pk": {"S": "pk-testing"}, "sk": {"S": "sk-testing"}}
    value = str(uuid.uuid4())

    client.put_item(
        TableName=table_name, Item={**key, **{"testing_value": {"S": value}}}
    )
    assert (
        value
        == client.get_item(TableName=table_name, Key=key)["Item"]["testing_value"]["S"]
    )
    client.delete_item(TableName=table_name, Key=key)


def test_gsi_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    index_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    key = {"pk": {"S": "pk-testing-gsi"}, "sk": {"S": "sk-testing-gsi"}}
    value = str(uuid.uuid4())

    client.put_item(
        TableName=table_name,
        Item={
            **key,
            **{
                "job_id": {"S": "job_id-testing-gsi"},
                "testing_value": {"S": value},
            },
        },
    )
    # Add 10 sec delay to allow the data to propagate to the index (GSI replica is eventually consistent)
    time.sleep(10)

    query_response = client.query(
        TableName=table_name,
        IndexName=index_name,
        KeyConditionExpression="job_id = :ji",
        ExpressionAttributeValues={":ji": {"S": "job_id-testing-gsi"}},
    )
    assert 1 == query_response["Count"]
    assert value == query_response["Items"][0]["testing_value"]["S"]
    client.delete_item(TableName=table_name, Key=key)
