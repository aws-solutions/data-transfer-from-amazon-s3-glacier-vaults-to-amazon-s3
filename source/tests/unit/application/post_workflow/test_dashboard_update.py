"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef, QueryOutputTypeDef
from mypy_boto3_s3.client import S3Client

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.metric_record import MetricRecord
from solution.application.post_workflow.dashboard_update import handle_failed_archives
from solution.infrastructure.output_keys import OutputKeys

WORKFLOW_RUN = "test_workflow_run"


@pytest.fixture(autouse=True, scope="module")
def mock_inventory_bucket(s3_client: S3Client) -> None:
    os.environ[
        OutputKeys.INVENTORY_BUCKET_NAME
    ] = "test_mock_upload_provided_file_bucket"
    s3_client.create_bucket(Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME])


def populate_glacier_retrieval_table(
    status: str, glacier_table_accessor: DynamoDBAccessor
) -> None:
    for i in range(0, 5):
        glacier_table_accessor.insert_item(
            item={
                "pk": {"S": f"{WORKFLOW_RUN}|test_archive_id_{status}_{i}"},
                "sk": {"S": "meta"},
                "job_id": {"S": "test_staged_job_id"},
                "start_time": {"S": "Today"},
                "vault_name": {"S": "vault_name"},
                "retrieval_type": {"S": "archive-retrieval"},
                "file_name": {"S": "test_file_name"},
                "s3_storage_class": {"S": "test_s3_storage_class"},
                "retrieve_status": {"S": f"{WORKFLOW_RUN}/{status}"},
                "size": {"N": "10"},
                "archive_id": {"S": f"test_archive_id_{status}_{i}"},
                "description": {"S": "test_description"},
                "archive_creation_date": {"S": "2023-05-09T15:52:27.757Z"},
                "sha256_tree_hash": {"S": "afdsfsdft43593453"},
            },
        )


def test_handle_failed_archives(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    metric_table_mock: CreateTableOutputTypeDef,
    s3_client: S3Client,
) -> None:
    glacier_table_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    metric_table_accessor = DynamoDBAccessor(os.environ[OutputKeys.METRIC_TABLE_NAME])
    content = b"ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\ntest_archive_id_staged_0,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_staged_1,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_staged_2,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_staged_3,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_staged_4,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_requested_0,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_requested_1,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_requested_2,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_requested_3,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\ntest_archive_id_requested_4,test_description,2023-05-09T15:52:27.757Z,10,afdsfsdft43593453\n"
    for status in ["STAGED", "REQUESTED", "DOWNLOADED"]:
        populate_glacier_retrieval_table(
            getattr(GlacierTransferModel.StatusCode, status), glacier_table_accessor
        )

    metric_record = MetricRecord(
        pk=WORKFLOW_RUN,
        size_total=150,
        count_total=15,
        size_staged=50,
        count_staged=5,
        size_downloaded=50,
        count_downloaded=5,
        size_requested=50,
        count_requested=5,
    )
    metric_table_accessor.insert_item(item=metric_record.marshal())
    handle_failed_archives(WORKFLOW_RUN, os.environ[OutputKeys.INVENTORY_BUCKET_NAME])
    response = metric_table_accessor.get_item(metric_record.key)
    obj = s3_client.get_object(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Key=f"{WORKFLOW_RUN}/failed_archives/failed_archives.csv",
    )
    assert content == obj["Body"].read()
    assert response and response["size_failed"]["N"] == "100"
    assert response and response["count_failed"]["N"] == "10"
