"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Generator, Iterator, Tuple

import aws_cdk as core
import aws_cdk.assertions as assertions
import boto3
import cdk_nag
import pytest
from botocore.config import Config
from moto import mock_dynamodb, mock_glacier, mock_s3, mock_sqs  # type: ignore
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
from mypy_boto3_glacier import GlacierClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from solution.infrastructure.aspects.app_registry import AppRegistry
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.stack import SolutionStack


@pytest.fixture(scope="module", autouse=True)
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCOUNT_ID"] = "testing"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME] = "FacilitatorTable"
    os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME] = "GlacierRetrievalTable"
    os.environ[
        OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME
    ] = "GlacierRetrievalJobIndexName"
    os.environ[OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME] = "staged_archives_index"
    os.environ[OutputKeys.METRIC_TABLE_NAME] = "MetricTable"
    os.environ[OutputKeys.VALIDATION_SQS_URL] = "ValidationSQS"
    os.environ["LOGGING_LEVEL"] = str(logging.INFO)


@pytest.fixture(scope="module")
def solution_user_agent() -> str:
    return "AwsSolution/SO0293/v1.1.0"


@pytest.fixture(scope="module")
def sqs_client(aws_credentials: None) -> Iterator[SQSClient]:
    with mock_sqs():
        connection: SQSClient = boto3.client("sqs", region_name="us-east-1")
        yield connection


@pytest.fixture(scope="module")
def s3_client(aws_credentials: None) -> Iterator[S3Client]:
    with mock_s3():
        connection: S3Client = boto3.client("s3", region_name="us-east-1")
        yield connection


@pytest.fixture(scope="module")
def glacier_client(aws_credentials: None) -> Iterator[GlacierClient]:
    with mock_glacier():
        connection: GlacierClient = boto3.client("glacier", region_name="us-east-1")
        yield connection


@pytest.fixture(scope="module")
def dynamodb_client(
    aws_credentials: None,
) -> Generator[DynamoDBClient, None, None]:
    with mock_dynamodb():
        ddb_client = boto3.client("dynamodb", "us-east-1")
        yield ddb_client


@pytest.fixture(scope="module")
def common_dynamodb_table_mock(
    dynamodb_client: DynamoDBClient,
) -> Tuple[DynamoDBClient, str]:
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "job_id", "AttributeType": "S"},
            {"AttributeName": "task_token", "AttributeType": "S"},
            {"AttributeName": "start_timestamp", "AttributeType": "S"},
        ],
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "job_id", "KeyType": "HASH"},
            {"AttributeName": "task_token", "KeyType": "RANGE"},
            {"AttributeName": "start_timestamp", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return (dynamodb_client, table_name)


@pytest.fixture(scope="module")
def metric_table_mock(
    dynamodb_client: DynamoDBClient,
) -> CreateTableOutputTypeDef:
    return dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


@pytest.fixture(scope="module")
def glacier_retrieval_table_mock(
    dynamodb_client: DynamoDBClient,
) -> Tuple[DynamoDBClient, str]:
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "retrieve_status", "AttributeType": "S"},
            {"AttributeName": "archive_creation_date", "AttributeType": "S"},
            {"AttributeName": "job_id", "AttributeType": "S"},
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
            },
            {
                "IndexName": os.environ[OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME],
                "KeySchema": [
                    {"AttributeName": "job_id", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )
    return (dynamodb_client, table_name)


@pytest.fixture
def app() -> core.App:
    app = core.App(
        context={
            "@aws-cdk/aws-iam:minimizePolicies": True,
            "SOLUTION_ID": "SO0140",
            "SOLUTION_VERSION": "%%VERSION%%",
            "BUCKET_NAME": "FAKEBUCKETNAME",
            "SOLUTION_NAME": "%%SOLUTION_NAME%%",
            "APP_REGISTRY_NAME": "FAKEAPPREGISTRYNAME",
            "APPLICATION_TYPE": "AWS-Solutions",
        }
    )
    return app


@pytest.fixture
def stack(app: core.App) -> SolutionStack:
    stack = SolutionStack(
        app, "data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3"
    )
    core.Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True)
    )
    core.Aspects.of(app).add(AppRegistry(stack, "AppRegistryAspect"))
    return stack


@pytest.fixture
def template(stack: SolutionStack) -> assertions.Template:
    return assertions.Template.from_stack(stack)


@pytest.fixture
def glacier_job_result() -> Dict[str, Any]:
    return {
        "Action": "InventoryRetrieval",
        "ArchiveId": None,
        "ArchiveSHA256TreeHash": None,
        "ArchiveSizeInBytes": None,
        "Completed": True,
        "CompletionDate": "2023-03-03T21:42:40.684Z",
        "CreationDate": "2023-03-03T17:53:45.420Z",
        "InventoryRetrievalParameters": {
            "EndDate": None,
            "Format": "CSV",
            "Limit": None,
            "Marker": None,
            "StartDate": None,
        },
        "InventorySizeInBytes": 1024,
        "JobDescription": "This is a test",
        "JobId": "KXt2zItqLEKWXWyHk__7sVM8PfNIrdrsdtTLMPsyzXMnIriEK4lzltZgN7erM6_-VLXwOioQapa8EOgKfqTpqeGWuGpk",
        "RetrievalByteRange": None,
        "SHA256TreeHash": None,
        "SNSTopic": "ARN",
        "StatusCode": "Succeeded",
        "StatusMessage": "Succeeded",
        "Tier": None,
        "VaultARN": "ARN",
    }
