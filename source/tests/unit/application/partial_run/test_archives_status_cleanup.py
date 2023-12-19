"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import pytest

from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.partial_run.archives_status_cleanup import (
    cleanup_archives_status,
    collect_non_downloaded_archives,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import QueryOutputTypeDef
else:
    DynamoDBClient = object
    QueryOutputTypeDef = object

WORKFLOW_RUN = "test_workflow_run"


@pytest.fixture(autouse=True, scope="module")
def setup(glacier_retrieval_table_mock: Tuple[DynamoDBClient, str]) -> None:
    pass


def test_query_archives_needing_status_cleanup_empty(
    dynamodb_client: DynamoDBClient,
) -> None:
    empty_glacier_retrieval_table(dynamodb_client)
    archives = collect_non_downloaded_archives(dynamodb_client, WORKFLOW_RUN)
    assert 0 == len(archives)


def test_query_archives_needing_status_cleanup(dynamodb_client: DynamoDBClient) -> None:
    populate_glacier_retrieval_table(
        GlacierTransferModel.StatusCode.STAGED, dynamodb_client
    )
    populate_glacier_retrieval_table(
        GlacierTransferModel.StatusCode.REQUESTED, dynamodb_client
    )
    populate_glacier_retrieval_table(
        GlacierTransferModel.StatusCode.DOWNLOADED, dynamodb_client
    )

    archives = collect_non_downloaded_archives(dynamodb_client, WORKFLOW_RUN)
    assert 10 == len(archives)
    for archive in archives:
        assert (
            archive["retrieve_status"]["S"]
            == f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.STAGED}"
        ) or (
            archive["retrieve_status"]["S"]
            == f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.REQUESTED}"
        )


def test_cleanup_archives_status_empty(dynamodb_client: DynamoDBClient) -> None:
    archives: List[Dict[str, Any]] = []
    cleanup_archives_status(archives)

    response = query_stopped_status(dynamodb_client)

    assert len(response["Items"]) == 0


def test_cleanup_archives_status(dynamodb_client: DynamoDBClient) -> None:
    archives = []
    staged_archives_count = 5
    for i in range(staged_archives_count):
        archive = mock_archive_metadata(f"{WORKFLOW_RUN}:test_archive_id_{i}")
        archives.append({"item": archive.marshal()})

    cleanup_archives_status(archives)

    response = query_stopped_status(dynamodb_client)

    assert len(response["Items"]) == staged_archives_count


def mock_archive_metadata(archive_id: str) -> GlacierTransferMetadata:
    return GlacierTransferMetadata(
        workflow_run=WORKFLOW_RUN,
        glacier_object_id=archive_id,
        archive_id=archive_id,
        job_id="test_job_id",
        start_time="test_start_time",
        vault_name="test_vault_name",
        retrieval_type="archive-retrieval",
        size=150,
        description="test_archive_description",
        file_name="test_file_name",
        retrieve_status=f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.STAGED}",
        download_window="2023-05-09T15:52:27.757Z",
        sha256_tree_hash="test_tree_hash",
        archive_creation_date=f"2023-01-01T01:01:01.001Z",
        s3_storage_class="GLACIER",
    )


def empty_glacier_retrieval_table(dynamodb_client: DynamoDBClient) -> None:
    response = dynamodb_client.scan(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    )

    for item in response["Items"]:
        key = {key: item[key] for key in item}
        dynamodb_client.delete_item(
            TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], Key=key
        )


def query_stopped_status(dynamodb_client: DynamoDBClient) -> QueryOutputTypeDef:
    return dynamodb_client.query(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        IndexName=os.environ[OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME],
        KeyConditionExpression="retrieve_status = :status",
        ExpressionAttributeValues={
            ":status": {
                "S": f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.STAGED}/{GlacierTransferModel.StatusCode.STOPPED}"
            }
        },
    )


def populate_glacier_retrieval_table(
    status: str, dynamodb_client: DynamoDBClient
) -> None:
    for i in range(0, 5):
        dynamodb_client.put_item(
            TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
            Item={
                "pk": {"S": f"{WORKFLOW_RUN}:test_archive_id_{status}_{i}"},
                "sk": {"S": "meta"},
                "retrieve_status": {"S": f"{WORKFLOW_RUN}/{status}"},
                "archive_creation_date": {"S": "2023-05-09T15:52:27.757Z"},
            },
        )
