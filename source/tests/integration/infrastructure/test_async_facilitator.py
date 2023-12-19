"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
import uuid
from typing import TYPE_CHECKING, Any, Dict

import boto3
import pytest

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_sns import SNSClient
else:
    DynamoDBClient = object
    SNSClient = object
    Table = object


def test_table_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    key = {"job_id": {"S": "testing"}}
    value = str(uuid.uuid4())

    client.put_item(
        TableName=table_name, Item={**key, **{"testing_value": {"S": value}}}
    )
    assert (
        value
        == client.get_item(TableName=table_name, Key=key)["Item"]["testing_value"]["S"]
    )
    client.delete_item(TableName=table_name, Key=key)


def test_topic_publish() -> None:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    client: SNSClient = boto3.client("sns")

    response = client.publish(
        Message=json.dumps(
            {
                "Action": "InventoryRetrieval",
                "JobId": "JOBID",
                "StatusCode": "InProgress",
                "Completed": False,
            }
        ),
        TopicArn=topic_arn,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]


def test_lambda_invoked() -> None:
    lambda_name = os.environ[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME]
    client = boto3.client("lambda")
    response = client.invoke(FunctionName=lambda_name)
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]


def test_lambda_invoked_by_sns() -> None:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    sns_client: SNSClient = boto3.client("sns")
    response = sns_client.publish(
        Message=json.dumps(
            {
                "Action": "InventoryRetrieval",
                "JobId": "JOBID",
                "StatusCode": "InProgress",
                "Completed": False,
            }
        ),
        TopicArn=topic_arn,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    notification_sqs_arn = os.environ[OutputKeys.NOTIFICATIONS_SQS_ARN]

    endpoint = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)[
        "Subscriptions"
    ][0]["Endpoint"]
    assert endpoint == notification_sqs_arn
