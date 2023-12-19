"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from solution.application.download_window import extension
from solution.application.model.glacier_transfer_model import GlacierTransferModel

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
else:
    DynamoDBClient = object
    Table = object
    CreateTableOutputTypeDef = object


@pytest.fixture
def glacier_retrieval_table(
    dynamodb_client: DynamoDBClient,
) -> CreateTableOutputTypeDef:
    return dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "retrieve_status", "AttributeType": "S"},
            {"AttributeName": "archive_creation_date", "AttributeType": "S"},
        ],
        TableName=os.environ["GlacierRetrievalTableName"],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "staged_archives_index",
                "KeySchema": [
                    {"AttributeName": "retrieve_status", "KeyType": "HASH"},
                    {"AttributeName": "archive_creation_date", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )


def test_query_archives_needing_extension(
    glacier_retrieval_table: Table, dynamodb_client: DynamoDBClient
) -> None:
    workflow_run = "test_workflow_run"

    retrieve_status = f"{workflow_run}/{GlacierTransferModel.StatusCode.STAGED}"

    for index, hours in enumerate([0, 24 - 6, 24 - 3]):
        download_window = (datetime.now() - timedelta(hours=hours)).isoformat()
        dynamodb_client.put_item(
            TableName=os.environ["GlacierRetrievalTableName"],
            Item={
                "pk": {"S": f"{workflow_run}:test_archive_id_{index}"},
                "sk": {"S": "meta"},
                "retrieve_status": {"S": retrieve_status},
                "download_window": {"S": download_window},
                "archive_creation_date": {"S": "2023-05-09T15:52:27.757Z"},
            },
        )

    archives = extension.query_archives_needing_extension(dynamodb_client, workflow_run)
    assert 1 == len(archives)
    assert "test_workflow_run:test_archive_id_2" == archives[0]["pk"]["S"]
