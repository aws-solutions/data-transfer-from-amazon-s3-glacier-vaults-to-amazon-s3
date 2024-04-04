"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from base64 import b64encode
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterator, Tuple
from unittest.mock import MagicMock, Mock, patch

import boto3
import pytest
from botocore.config import Config
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_glacier import GlacierClient
from mypy_boto3_sqs import SQSClient

from solution.application import __boto_config__
from solution.application.glacier_s3_transfer.facilitator import GlacierToS3Facilitator
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.model import responses
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.glacier_transfer_part_model import (
    GlacierTransferPart,
    GlacierTransferPartRead,
)
from solution.application.util.exceptions import (
    ExpiredDownloadWindow,
    GlacierValidationMismatch,
)
from solution.infrastructure.output_keys import OutputKeys


@pytest.fixture
def validation_sqs_mock(sqs_client: SQSClient) -> Iterator[SQSClient]:
    sqs_client.create_queue(QueueName=os.environ[OutputKeys.VALIDATION_SQS_URL])

    yield sqs_client
    sqs_client.delete_queue(QueueUrl=os.environ[OutputKeys.VALIDATION_SQS_URL])


@pytest.fixture
def mock_item() -> GlacierTransferMetadata:
    return GlacierTransferMetadata(
        workflow_run="workflow1",
        glacier_object_id="archive1",
        job_id="job1",
        start_time="1",
        vault_name="test_vault_name",
        retrieval_type="archive-retrieval",
        size=150,
        description="archive_description",
        file_name="test_file_name",
        retrieve_status=f"workflow1/{GlacierTransferModel.StatusCode.REQUESTED}",
        download_window=(datetime.now() + timedelta(hours=1)).isoformat(),
        sha256_tree_hash="test_hash",
        s3_storage_class="Glacier",
    )


@patch("solution.application.glacier_s3_transfer.facilitator.TreeHash")
@patch("solution.application.glacier_s3_transfer.facilitator.GlacierDownload")
@patch("solution.application.glacier_s3_transfer.facilitator.S3Upload")
def test_transfer_happy_path(
    upload_mock: MagicMock,
    download_mock: MagicMock,
    tree_hash_mock: MagicMock,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1], Item=mock_item.marshal()
    )
    download_instance = download_mock.return_value
    data = b"chunk"
    download_instance.read.return_value = data
    download_instance.checksum.return_value = (
        "deadbeef"  # An example checksum which only has hex characters
    )

    tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

    upload_mock.return_value.upload_part = _mock_upload
    facilitator = _create_facilitator()
    test_now = datetime(1856, 7, 10, 12, 34, 56)

    result = facilitator.transfer()

    expected_result = _mock_upload(data, 1)
    expected_result["TreeChecksum"] = b64encode(b"\xde\xad\xbe\xef").decode("ascii")
    assert result == expected_result
    glacier_retrieval_ddb_item = glacier_retrieval_table_mock[0].get_item(
        TableName=glacier_retrieval_table_mock[1],
        Key=GlacierTransferPartRead(
            workflow_run="workflow1", glacier_object_id="archive1", part_number=1
        ).key,
    )
    glacier_transfer_part = GlacierTransferPart.parse(
        glacier_retrieval_ddb_item["Item"]
    )
    assert glacier_transfer_part.checksum_sha_256 == "checksum1"
    assert glacier_transfer_part.e_tag == "etag1"
    assert glacier_transfer_part.tree_checksum == expected_result["TreeChecksum"]


@patch("solution.application.glacier_s3_transfer.facilitator.TreeHash")
@patch("solution.application.glacier_s3_transfer.facilitator.GlacierDownload")
@patch("solution.application.glacier_s3_transfer.facilitator.S3Upload")
def test_transfer_glacier_checksum_mismatch(
    upload_mock: MagicMock,
    download_mock: MagicMock,
    tree_hash_mock: MagicMock,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1], Item=mock_item.marshal()
    )
    download_instance = download_mock.return_value
    download_instance.read.return_value = b"chunk"
    download_instance.checksum.return_value = (
        "deadbeef1"  # An example checksum which only has hex characters
    )

    tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

    facilitator = _create_facilitator()
    upload_mock.return_value.upload_part = _mock_upload
    with pytest.raises(GlacierValidationMismatch):
        facilitator.transfer()


@patch("solution.application.glacier_s3_transfer.facilitator.TreeHash")
@patch("solution.application.glacier_s3_transfer.facilitator.GlacierDownload")
@patch("solution.application.glacier_s3_transfer.facilitator.S3Upload")
def test_transfer_glacier_checksum_mismatch_disabled(
    upload_mock: MagicMock,
    download_mock: MagicMock,
    tree_hash_mock: MagicMock,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1], Item=mock_item.marshal()
    )
    download_instance = download_mock.return_value
    download_instance.read.return_value = b"chunk"
    download_instance.checksum.return_value = (
        "deadbeef1"  # An example checksum which only has hex characters
    )

    tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

    facilitator = _create_facilitator(GlacierJobType.INVENTORY_RETRIEVAL)
    upload_mock.return_value.upload_part = _mock_upload
    facilitator.transfer()
    dynamo_item = glacier_retrieval_table_mock[0].get_item(
        TableName=glacier_retrieval_table_mock[1],
        Key=GlacierTransferPartRead(
            workflow_run="workflow1", glacier_object_id="archive1", part_number=1
        ).key,
    )
    glacier_transfer_part = GlacierTransferPart.parse(dynamo_item["Item"])
    assert glacier_transfer_part.checksum_sha_256 == "checksum1"
    assert glacier_transfer_part.e_tag == "etag1"
    assert glacier_transfer_part.tree_checksum == "treechecksum1"


def test_transfer_glacier_outside_download_window(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    marshaled_item = mock_item.marshal()
    marshaled_item["download_window"]["S"] = (
        datetime.now() - timedelta(hours=25)
    ).isoformat()
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1], Item=marshaled_item
    )
    facilitator = _create_facilitator()
    with pytest.raises(ExpiredDownloadWindow):
        facilitator.transfer()


@pytest.mark.parametrize(
    "total_chunks_count, downloaded_chunks_count, message_exist, message_count, message_body",
    [
        (1, 1, True, 1, '{"WorkflowRun": "workflow1", "GlacierObjectId": "archive1"}'),
        (5, 5, True, 1, '{"WorkflowRun": "workflow1", "GlacierObjectId": "archive1"}'),
        (5, 4, False, 0, None),
    ],
)
def test_send_validation_event_last_chunk_true(
    total_chunks_count: int,
    downloaded_chunks_count: int,
    message_exist: bool,
    message_count: int,
    message_body: str | None,
    sqs_client: SQSClient,
    validation_sqs_mock: SQSClient,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    _populate_glacier_retrieval_table(
        total_chunks_count,
        downloaded_chunks_count,
        glacier_retrieval_table_mock,
        mock_item,
    )

    facilitator = _create_facilitator()
    facilitator.send_validation_event()
    response = sqs_client.receive_message(
        QueueUrl=os.environ[OutputKeys.VALIDATION_SQS_URL]
    )

    assert message_exist == (True if "Messages" in response else False)

    assert message_count == (len(response["Messages"]) if "Messages" in response else 0)

    assert message_body == (
        response["Messages"][0]["Body"] if "Messages" in response else None
    )


@patch.object(GlacierToS3Facilitator, "_is_last_chunk")
@patch.object(GlacierToS3Facilitator, "_get_metadata")
def test_user_agent_on_sqs_client(
    _mock_is_last_chunk: Mock,
    _mock_get_metadata: Mock,
    glacier_client: GlacierClient,
    solution_user_agent: str,
) -> None:
    with patch("boto3.client") as mock_client:
        facilitator = _create_facilitator()
        facilitator.send_validation_event()

        assert mock_client.call_args[0][0] == "sqs"

        _config = mock_client.call_args[1]["config"]
        assert type(_config) is Config

        _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
        assert _config_user_agent_extra == __boto_config__.__getattribute__(
            "user_agent_extra"
        )
        assert _config_user_agent_extra == solution_user_agent


def _populate_glacier_retrieval_table(
    total_chunks_count: int,
    downloaded_chunks_count: int,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
    mock_item: GlacierTransferMetadata,
) -> None:
    _empty_glacier_retrieval_table(glacier_retrieval_table_mock)
    mock_item.chunks_count = total_chunks_count
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1], Item=mock_item.marshal()
    )
    for part_num in range(downloaded_chunks_count):
        part = GlacierTransferPart(
            workflow_run="workflow1",
            glacier_object_id="archive1",
            checksum_sha_256="checksumSHA256",
            e_tag="eTag",
            part_number=part_num,
        )
        glacier_retrieval_table_mock[0].put_item(
            TableName=glacier_retrieval_table_mock[1], Item=part.marshal()
        )


def _create_facilitator(
    glacier_job_type: str = GlacierJobType.ARCHIVE_RETRIEVAL,
) -> GlacierToS3Facilitator:
    return GlacierToS3Facilitator(
        glacier_client=boto3.client("glacier"),
        vault_name="vault1",
        workflow_run="workflow1",
        byte_range="0-100",
        glacier_object_id="archive1",
        s3_destination_bucket="bucket1",
        s3_destination_key="key1",
        upload_id="upload1",
        part_number=1,
        glacier_job_type=glacier_job_type,
    )


def _mock_upload(data: bytes, part_number: int) -> responses.GlacierRetrieval:
    return {
        "ETag": "etag1",
        "PartNumber": part_number,
        "ChecksumSHA256": "checksum1",
        "TreeChecksum": "treechecksum1",
    }


def _empty_glacier_retrieval_table(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str]
) -> None:
    response = glacier_retrieval_table_mock[0].scan(
        TableName=glacier_retrieval_table_mock[1], Select="ALL_ATTRIBUTES"
    )
    for item in response["Items"]:
        key = {"pk": item["pk"], "sk": item["sk"]}
        glacier_retrieval_table_mock[0].delete_item(
            TableName=glacier_retrieval_table_mock[1], Key=key
        )
