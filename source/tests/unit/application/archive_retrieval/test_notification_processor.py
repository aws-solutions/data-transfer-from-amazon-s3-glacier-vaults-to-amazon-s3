"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import os
from typing import List, Tuple
from unittest.mock import Mock, patch

import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_s3 import S3Client

from solution.application.archive_retrieval.notification_processor import (
    create_multipart_upload,
    get_glacier_transfer_metadata,
    handle_archive_job_notification,
    send_chunk_events,
    update_glacier_transfer_metadata,
)
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

VAULT_NAME = "test_vault_name"
WORKFLOW_RUN = "test_notification_processor"
ARCHIVE_ID = "test_archive_id"
JOB_ID = "test_job_id"


@pytest.fixture(scope="module")
def mock_metadata() -> GlacierTransferMetadata:
    return GlacierTransferMetadata(
        workflow_run=WORKFLOW_RUN,
        job_id=JOB_ID,
        staged_job_id=JOB_ID,
        start_time="",
        vault_name=VAULT_NAME,
        retrieval_type=GlacierJobType.ARCHIVE_RETRIEVAL,
        description="",
        retrieve_status=f"workflow1/{GlacierTransferModel.StatusCode.REQUESTED}",
        glacier_object_id=ARCHIVE_ID,
        size=1,
        file_name=ARCHIVE_ID,
        s3_storage_class="GLACIER",
    )


def test_handle_archive_job_notification_happy_path(
    dynamodb_client: DynamoDBClient, mock_metadata: GlacierTransferMetadata
) -> None:
    # Mocking necessary dependencies and utilities
    with patch(
        "solution.application.archive_retrieval.notification_processor.get_glacier_transfer_metadata"
    ) as mock_get_glacier_transfer_metadata, patch(
        "solution.application.archive_retrieval.notification_processor.calculate_chunk_size"
    ) as mock_calculate_chunk_size, patch(
        "solution.application.archive_retrieval.notification_processor.generate_chunk_array"
    ) as mock_generate_chunk_array, patch(
        "solution.application.archive_retrieval.notification_processor.create_multipart_upload"
    ) as mock_create_multipart_upload, patch(
        "solution.application.archive_retrieval.notification_processor.update_glacier_transfer_metadata"
    ) as mock_update_glacier_transfer_metadata, patch(
        "solution.application.archive_retrieval.notification_processor.send_chunk_events"
    ) as mock_send_chunk_events:
        # Arrange
        mock_get_glacier_transfer_metadata.return_value = mock_metadata

        notification_str = json.dumps(
            {
                "ArchiveId": "test_archive_id",
                "JobId": "test_job_id",
                "Completed": True,
                "StatusCode": "Succeeded",
                "CompletionDate": "test_completion_date",
            }
        )

        # Act
        handle_archive_job_notification(notification_str)

        # Assert that each expected function call was made
        mock_get_glacier_transfer_metadata.assert_called_once()
        mock_calculate_chunk_size.assert_called_once()
        mock_generate_chunk_array.assert_called_once()
        mock_create_multipart_upload.assert_called_once()
        mock_update_glacier_transfer_metadata.assert_called_once()
        mock_send_chunk_events.assert_called_once()


def test_get_glacier_transfer_metadata(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_metadata: GlacierTransferMetadata,
) -> None:
    # Arrange
    dynamodb_client, _ = glacier_retrieval_table_mock
    job_id = "test_job_id"
    dynamodb_client.put_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Item=mock_metadata.marshal(),
    )
    # Act/Assert
    metadata = get_glacier_transfer_metadata(dynamodb_client, job_id)
    assert metadata is not None
    assert metadata.marshal() == mock_metadata.marshal()


def test_get_glacier_transfer_metadata_none(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str]
) -> None:
    # Arrange
    dynamodb_client, _ = glacier_retrieval_table_mock
    job_id = "test_job_id_none"

    # Act
    response = get_glacier_transfer_metadata(dynamodb_client, job_id)

    # Assert that no metadata exists
    assert response is None


def test_create_multipart_upload(s3_client: S3Client) -> None:
    # Arrange
    upload_key = "test_key"
    bucket_name = "test_bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    os.environ[OutputKeys.OUTPUT_BUCKET_NAME] = bucket_name

    # Act
    upload_id = create_multipart_upload(upload_key, "GLACIER")

    # Assert
    assert upload_id is not None

    # Assert the multipart upload is listed on our mock S3Client
    uploads = s3_client.list_multipart_uploads(Bucket=bucket_name)
    assert uploads["Uploads"][0]["Key"] == upload_key


@pytest.mark.parametrize(
    "job_id, workflow_run, vault_name, archive_id, upload_id, object_key, chunks",
    [
        (
            "test_job_id",
            "test_workflow_run",
            "test_vault_name",
            "test_archive_id",
            "test_upload_id",
            "test_object_key",
            ["0-100", "101-200", "201-300"],
        ),
        (
            "test_job_id2",
            "test_workflow_run2",
            "test_vault_name2",
            "test_archive_id2",
            "test_upload_id2",
            "test_object_key2",
            ["1-1", "1-0", "2"],
        ),
    ],
)
def test_send_chunk_events(
    job_id: str,
    workflow_run: str,
    vault_name: str,
    archive_id: str,
    upload_id: str,
    object_key: str,
    chunks: List[str],
) -> None:
    # Mocking external dependencies
    os.environ[OutputKeys.CHUNKS_SQS_URL] = "mock_sqs_url"
    os.environ[OutputKeys.OUTPUT_BUCKET_NAME] = "mock_bucket_name"

    # Create a mock for boto3.client("sqs")
    sqs_mock = Mock()

    # Patching the boto SQS client creation
    with patch(
        "solution.application.archive_retrieval.notification_processor.boto3.client",
        return_value=sqs_mock,
    ):
        # Call the function with sample arguments
        send_chunk_events(
            chunks=chunks,
            job_id=job_id,
            workflow_run=workflow_run,
            vault_name=vault_name,
            archive_id=archive_id,
            upload_id=upload_id,
            object_key=object_key,
        )

    # Assertions
    for index, chunk in enumerate(chunks):
        expected_message_body = {
            "JobId": job_id,
            "VaultName": vault_name,
            "ByteRange": chunk,  # Assuming the chunks are processed sequentially
            "S3DestinationBucket": "mock_bucket_name",
            "S3DestinationKey": object_key,
            "GlacierObjectId": archive_id,
            "UploadId": upload_id,
            "PartNumber": index + 1,
            "WorkflowRun": workflow_run,
        }

        # Ensure that send_message was called with the expected arguments
        sqs_mock.send_message.assert_any_call(
            QueueUrl="mock_sqs_url", MessageBody=json.dumps(expected_message_body)
        )
    assert sqs_mock.send_message.call_count == len(chunks)


@pytest.mark.parametrize(
    "chunks_count, upload_id, destination_key, retrieve_status, completion_date, job_id",
    [
        (
            "100",
            "test_upload",
            "test_destination_key",
            "test_retrieve_status",
            "test_completion_date",
            "test_job_id",
        ),
        (
            "-100",
            "test_upload2",
            "test_destination_key2",
            "test_retrieve_status2",
            "test_completion_date2",
            "test_job_id2",
        ),
    ],
)
def test_update_glacier_transfer_metadata(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    chunks_count: str,
    upload_id: str,
    destination_key: str,
    retrieve_status: str,
    completion_date: str,
    job_id: str,
) -> None:
    # Arrange
    dynamodb_client, _ = glacier_retrieval_table_mock

    # Act
    update_glacier_transfer_metadata(
        dynamodb_client,
        WORKFLOW_RUN,
        ARCHIVE_ID,
        chunks_count,
        upload_id,
        destination_key,
        retrieve_status,
        completion_date,
        job_id,
    )

    # Assert
    # Get the updated Metadata from the Glacier Retrieval Table
    ddb_metadata = GlacierTransferMetadata.parse(
        dynamodb_client.get_item(
            TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
            Key=GlacierTransferMetadataRead(
                workflow_run=WORKFLOW_RUN, glacier_object_id=ARCHIVE_ID
            ).key,
        )["Item"]
    )

    # Assert the Metadata was updated accordingly
    assert ddb_metadata.chunks_count == int(chunks_count)
    assert ddb_metadata.upload_id == upload_id
    assert ddb_metadata.s3_destination_key == destination_key
    assert ddb_metadata.retrieve_status == retrieve_status
    assert ddb_metadata.download_window == completion_date
    assert ddb_metadata.staged_job_id == job_id
