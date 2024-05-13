"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Tuple
from unittest.mock import MagicMock, patch

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_glacier.client import GlacierClient

from solution.application import __boto_config__
from solution.application.archive_retrieval.initiator import (
    extend_retrieval,
    glacier_initiate_job,
    initiate_request,
    initiate_retrieval,
)
from solution.application.model import events
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

VAULT_NAME = "test_vault_name"


@pytest.fixture
def default_item() -> Iterator[events.InitiateArchiveRetrievalItem]:
    items = generate_initiate_archive_retrieval_items(count=1)
    yield items[0]


@pytest.fixture(scope="module")
def setup_clients(
    glacier_client: GlacierClient, dynamodb_client: DynamoDBClient
) -> Tuple[GlacierClient, DynamoDBClient]:
    glacier_client.create_vault(vaultName=VAULT_NAME)

    dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "retrieve_status", "AttributeType": "S"},
            {"AttributeName": "archive_creation_date", "AttributeType": "S"},
        ],
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
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
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "job_id", "AttributeType": "S"},
        ],
        TableName=os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME],
        KeySchema=[
            {"AttributeName": "job_id", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    return (glacier_client, dynamodb_client)


if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import (
        CreateTableOutputTypeDef,
        GetItemOutputTypeDef,
        PutItemOutputTypeDef,
    )
    from mypy_boto3_glacier.client import GlacierClient
else:
    DynamoDBClient = object
    GetItemOutputTypeDef = object
    PutItemOutputTypeDef = object
    GlacierClient = object
    CreateTableOutputTypeDef = object


def test_initiate_retrieval_batch(
    setup_clients: Tuple[GlacierClient, DynamoDBClient],
    metric_table_mock: CreateTableOutputTypeDef,
) -> None:
    glacier_client, dynamodb_client = setup_clients
    sns_topic = "test_topic"
    account_id = "test_account"
    workflow_run = "test-workflow_run"
    count = 5
    items: List[
        events.InitiateArchiveRetrievalItem
    ] = generate_initiate_archive_retrieval_items(count=count)
    initiate_retrieval(account_id, sns_topic, items, glacier_client)

    for i in range(count):
        archive_id = f"test_archive_id_{i}"
        response = glacier_retrieval_get_item(workflow_run, archive_id, dynamodb_client)

        assert response.get("Item") is not None
        assert response["Item"]["retrieve_status"]["S"].endswith(
            f"/{GlacierTransferModel.StatusCode.REQUESTED}"
        )

        job_id = response["Item"]["job_id"]["S"]
        glacier_response = glacier_client.describe_job(
            vaultName=VAULT_NAME, jobId=job_id
        )
        assert archive_id == glacier_response["ArchiveId"]


def test_initiate_request(
    setup_clients: Tuple[GlacierClient, DynamoDBClient],
    default_item: events.InitiateArchiveRetrievalItem,
    metric_table_mock: CreateTableOutputTypeDef,
) -> None:
    glacier_client, dynamodb_client = setup_clients
    sns_topic = "test_topic"
    account_id = "test_account"
    workflow_run = "test-workflow_run"
    archive_id = "test_archive_id_0"

    initiate_request(
        default_item, dynamodb_client, glacier_client, sns_topic, account_id
    )

    glacier_retrieval_response = glacier_retrieval_get_item(
        workflow_run, archive_id, dynamodb_client
    )

    assert glacier_retrieval_response.get("Item") is not None
    assert glacier_retrieval_response["Item"]["retrieve_status"]["S"].endswith(
        f"/{GlacierTransferModel.StatusCode.REQUESTED}"
    )

    job_id = glacier_retrieval_response["Item"]["job_id"]["S"]
    glacier_response = glacier_client.describe_job(vaultName=VAULT_NAME, jobId=job_id)
    assert archive_id == glacier_response["ArchiveId"]


def test_initiate_request_already_downloaded(
    setup_clients: Tuple[GlacierClient, DynamoDBClient],
    default_item: events.InitiateArchiveRetrievalItem,
) -> None:
    glacier_client, dynamodb_client = setup_clients

    sns_topic = "test_topic"
    account_id = "test_account"
    workflow_run = "test-workflow_run"
    archive_id = "test_archive_id_0"

    glacier_retrieval_put_item(
        workflow_run,
        archive_id,
        "downloaded",
        dynamodb_client,
    )

    initiate_request(
        default_item, dynamodb_client, glacier_client, sns_topic, account_id
    )

    response = glacier_retrieval_get_item(workflow_run, archive_id, dynamodb_client)

    assert response.get("Item") is not None
    assert response["Item"]["retrieve_status"]["S"].endswith(
        f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
    )

    job_id = response["Item"]["job_id"]["S"]

    with pytest.raises(Exception):
        glacier_client.describe_job(vaultName=VAULT_NAME, jobId=job_id)


def test_glacier_initiate_job(glacier_client: GlacierClient) -> None:
    glacier_client.create_vault(vaultName=VAULT_NAME)
    job_id = glacier_initiate_job(
        glacier_client,
        VAULT_NAME,
        "test_sns_topic",
        "test_archive_id",
        "test_tier",
        "test_account_id",
    )
    assert job_id is not None


@patch("boto3.client")
def test_glacier_initiate_job_failed(boto3_client_mock: MagicMock) -> None:
    boto3_client_mock.initiate_job.side_effect = ClientError(
        error_response={"Error": {"Code": "ResourceNotFoundException"}},
        operation_name="InitiateJob",
    )
    job_id = glacier_initiate_job(
        boto3_client_mock,
        VAULT_NAME,
        "test_sns_topic",
        "test_archive_id",
        "test_tier",
        "test_account_id",
    )
    assert job_id == None


def generate_initiate_archive_retrieval_items(
    count: int,
) -> List[events.InitiateArchiveRetrievalItem]:
    items: List[events.InitiateArchiveRetrievalItem] = []
    for i in range(count):
        items.append(
            {
                "item": {
                    "CreationDate": "2023-05-16T20:55:11Z",
                    "Size": "12",
                    "ArchiveDescription": f"test_archive_description_{i}",
                    "ArchiveId": f"test_archive_id_{i}",
                    "SHA256TreeHash": f"test_tree_hash_{i}",
                    "Filename": f"filename_{i}",
                },
                "workflow_run": "test-workflow_run",
                "tier": "Bulk",
                "vault_name": VAULT_NAME,
                "s3_storage_class": "GLACIER",
            }
        )
    return items


def glacier_retrieval_get_item(
    workflow_run: str, archive_id: str, dynamodb_client: DynamoDBClient
) -> GetItemOutputTypeDef:
    return dynamodb_client.get_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Key=GlacierTransferMetadataRead(
            workflow_run=workflow_run, glacier_object_id=archive_id
        ).key,
    )


def glacier_retrieval_put_item(
    workflow_run: str, archive_id: str, status: str, dynamodb_client: DynamoDBClient
) -> PutItemOutputTypeDef:
    return dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Item={
            **GlacierTransferMetadataRead(
                workflow_run=workflow_run, glacier_object_id=archive_id
            ).key,
            "job_id": {"S": "test_job_id"},
            "retrieve_status": {"S": f"{workflow_run}/{status}"},
        },
    )


@pytest.mark.parametrize("functions", [extend_retrieval, initiate_retrieval])
def test_user_agent_on_dynamodb_client(
    solution_user_agent: str,
    glacier_client: GlacierClient,
    functions: Callable[[str, str, List[Any], GlacierClient], None],
) -> None:
    with patch("boto3.client") as mock_client:
        functions("", "", [], glacier_client)
        _config = mock_client.call_args[1]["config"]
        assert type(_config) is Config

        _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
        assert _config_user_agent_extra == __boto_config__.__getattribute__(
            "user_agent_extra"
        )
        assert _config_user_agent_extra == solution_user_agent
