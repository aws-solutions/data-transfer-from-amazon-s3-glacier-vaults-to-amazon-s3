"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from base64 import b64decode, b64encode
from typing import Tuple

import botocore
import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
from mypy_boto3_glacier import GlacierClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import (
    CreateMultipartUploadOutputTypeDef,
    UploadPartOutputTypeDef,
)

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.glacier_s3_transfer.upload import S3Upload
from solution.application.glacier_s3_transfer.validator import validate_upload
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.hashing.tree_hash import TreeHash
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.glacier_transfer_part_model import GlacierTransferPart
from solution.application.model.responses import (
    GlacierRetrieval as GlacierRetrievalResponse,
)
from solution.application.util.exceptions import (
    GlacierValidationMismatch,
    InvalidGlacierRetrievalMetadata,
)
from solution.infrastructure.output_keys import OutputKeys

OUTPUT_BUCKET_NAME = "test_validator_output_bucket"
TEST_DATA = b"test_validator_data"
WORKFLOW_RUN = "test_validator_workflow_run"
JOB_ID = "test_validator_job_id"


@pytest.fixture
def output_bucket(s3_client: S3Client) -> S3Client:
    os.environ[OutputKeys.OUTPUT_BUCKET_NAME] = OUTPUT_BUCKET_NAME
    s3_client.create_bucket(Bucket=OUTPUT_BUCKET_NAME)
    return s3_client


@pytest.fixture(autouse=True)
def metric_table(metric_table_mock: CreateTableOutputTypeDef) -> None:
    # Create the mock table for each test
    pass


def create_multipart_upload(
    client: S3Client, s3_object_key: str
) -> CreateMultipartUploadOutputTypeDef:
    return client.create_multipart_upload(
        Bucket=OUTPUT_BUCKET_NAME,
        Key=s3_object_key,
        ChecksumAlgorithm="SHA256",
    )


def upload_part(
    upload_id: str, part_number: int, data: bytes, s3_object_key: str
) -> GlacierRetrievalResponse:
    s3_upload = S3Upload(
        bucket_name=OUTPUT_BUCKET_NAME,
        key=s3_object_key,
        upload_id=upload_id,
    )
    return s3_upload.upload_part(data, part_number)


def get_tree_hash(data: bytes, count: int = 1) -> str:
    glacier_hash = TreeHash()
    for _ in range(count):
        glacier_hash.update(data)
    return b64encode(glacier_hash.digest()).decode("ascii")


def insert_glacier_retrieval_part_data(
    ddb_accessor: DynamoDBAccessor,
    part_data: GlacierRetrievalResponse,
    tree_checksum: str,
    s3_object_key: str,
) -> None:
    glacier_transfer_part = GlacierTransferPart(
        workflow_run=WORKFLOW_RUN,
        glacier_object_id=s3_object_key,
        checksum_sha_256=part_data["ChecksumSHA256"],
        e_tag=part_data["ETag"],
        part_number=part_data["PartNumber"],
        tree_checksum=tree_checksum,
    )
    ddb_accessor.insert_item(glacier_transfer_part.marshal())


def insert_glacier_retrieval_metadata(
    ddb_accessor: DynamoDBAccessor,
    upload_id: str,
    tree_checksum: str,
    s3_object_key: str,
    chunks_count: int = 1,
    size: int = 1,
    retrieval_type: str = GlacierJobType.ARCHIVE_RETRIEVAL,
) -> None:
    glacier_transfer_metadata = GlacierTransferMetadata(
        workflow_run=WORKFLOW_RUN,
        job_id=JOB_ID,
        staged_job_id=JOB_ID,
        start_time="",
        vault_name=JOB_ID,
        retrieval_type=retrieval_type,
        description="",
        retrieve_status=f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.REQUESTED}",
        glacier_object_id=s3_object_key,
        size=size,
        file_name=s3_object_key,
        s3_destination_bucket=OUTPUT_BUCKET_NAME,
        s3_destination_key=s3_object_key,
        s3_storage_class="GLACIER",
        chunks_count=chunks_count,
        upload_id=upload_id,
        sha256_tree_hash=b64decode(tree_checksum).hex(),
    )
    ddb_accessor.insert_item(glacier_transfer_metadata.marshal())


def test_validation_happy_path(
    output_bucket: S3Client,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    # Arrange
    s3_object_key = "test_validation_happy_path"
    glacier_retrieval_ddb_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    upload_id = create_multipart_upload(output_bucket, s3_object_key=s3_object_key)[
        "UploadId"
    ]

    part_data = upload_part(
        upload_id=upload_id, part_number=1, data=TEST_DATA, s3_object_key=s3_object_key
    )

    tree_hash = get_tree_hash(TEST_DATA)

    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data,
        tree_checksum=tree_hash,
        s3_object_key=s3_object_key,
    )

    insert_glacier_retrieval_metadata(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        upload_id=upload_id,
        tree_checksum=tree_hash,
        s3_object_key=s3_object_key,
    )

    # Assert object hasn't yet been completed
    with pytest.raises(botocore.exceptions.ClientError):
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)

    # Act
    validate_upload(
        workflow_run=WORKFLOW_RUN,
        glacier_object_id=s3_object_key,
        glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
    )

    # Assert object has been completed and exists
    assert (
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)[
            "ResponseMetadata"
        ]["HTTPStatusCode"]
        == 200
    )

    # Assert metadata retrieve status has been updated
    archive_metadata = glacier_retrieval_ddb_accessor.get_item(
        key=GlacierTransferMetadataRead(
            workflow_run=WORKFLOW_RUN, glacier_object_id=s3_object_key
        ).key
    )
    assert archive_metadata
    assert GlacierTransferMetadata.parse(archive_metadata).retrieve_status.endswith(
        f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
    )


def test_validation_happy_path_inventory(
    output_bucket: S3Client,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    # Arrange
    s3_object_key = "test_validation_happy_path_inventory"
    glacier_retrieval_ddb_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    upload_id = create_multipart_upload(output_bucket, s3_object_key=s3_object_key)[
        "UploadId"
    ]

    part_data = upload_part(
        upload_id=upload_id, part_number=1, data=TEST_DATA, s3_object_key=s3_object_key
    )

    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data,
        tree_checksum="",
        s3_object_key=s3_object_key,
    )

    insert_glacier_retrieval_metadata(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        upload_id=upload_id,
        tree_checksum="",
        s3_object_key=s3_object_key,
        retrieval_type=GlacierJobType.INVENTORY_RETRIEVAL,
        size=len(TEST_DATA),
    )

    # Assert object hasn't yet been completed
    with pytest.raises(botocore.exceptions.ClientError):
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)

    # Act
    validate_upload(
        workflow_run=WORKFLOW_RUN,
        glacier_object_id=s3_object_key,
        glacier_job_type=GlacierJobType.INVENTORY_RETRIEVAL,
    )

    # Assert object has been completed and exists
    assert (
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)[
            "ResponseMetadata"
        ]["HTTPStatusCode"]
        == 200
    )


def test_validation_happy_path_two_parts(
    output_bucket: S3Client,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    # Arrange
    s3_object_key = "test_validation_happy_path_two_parts"
    glacier_retrieval_ddb_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    upload_id = create_multipart_upload(output_bucket, s3_object_key=s3_object_key)[
        "UploadId"
    ]

    test_data = (
        b"A" * 8 * 2**20
    )  # 8 MB to exceed mutipart upload minimum size requirements

    part_data = upload_part(
        upload_id=upload_id, part_number=1, data=test_data, s3_object_key=s3_object_key
    )

    part_data_2 = upload_part(
        upload_id=upload_id, part_number=2, data=test_data, s3_object_key=s3_object_key
    )
    part_tree_hash = get_tree_hash(test_data)
    archive_tree_hash = get_tree_hash(test_data, count=2)

    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data,
        tree_checksum=part_tree_hash,
        s3_object_key=s3_object_key,
    )
    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data_2,
        tree_checksum=part_tree_hash,
        s3_object_key=s3_object_key,
    )

    insert_glacier_retrieval_metadata(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        upload_id=upload_id,
        tree_checksum=archive_tree_hash,
        s3_object_key=s3_object_key,
        chunks_count=2,
    )

    # Assert object hasn't yet been completed
    with pytest.raises(botocore.exceptions.ClientError):
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)

    # Act
    validate_upload(
        workflow_run=WORKFLOW_RUN,
        glacier_object_id=s3_object_key,
        glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
    )

    # Assert object has been completed and exists
    assert (
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)[
            "ResponseMetadata"
        ]["HTTPStatusCode"]
        == 200
    )

    # Assert metadata retrieve status has been updated
    archive_metadata = glacier_retrieval_ddb_accessor.get_item(
        key=GlacierTransferMetadataRead(
            workflow_run=WORKFLOW_RUN, glacier_object_id=s3_object_key
        ).key
    )
    assert archive_metadata
    assert GlacierTransferMetadata.parse(archive_metadata).retrieve_status.endswith(
        f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
    )


def test_validation_missing_metadata() -> None:
    # Arrange/Act/Assert
    with pytest.raises(InvalidGlacierRetrievalMetadata):
        validate_upload(
            workflow_run="",
            glacier_object_id="",
            glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
        )


def test_validation_invalid_upload_id(
    output_bucket: S3Client,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    # Arrange
    s3_object_key = "test_validation_invalid_upload_id"
    glacier_retrieval_ddb_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    upload_id = create_multipart_upload(output_bucket, s3_object_key=s3_object_key)[
        "UploadId"
    ]

    part_data = upload_part(
        upload_id=upload_id, part_number=1, data=TEST_DATA, s3_object_key=s3_object_key
    )

    tree_hash = get_tree_hash(TEST_DATA)

    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data,
        tree_checksum=tree_hash,
        s3_object_key=s3_object_key,
    )

    insert_glacier_retrieval_metadata(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        upload_id="invalid_upload_id",
        tree_checksum=tree_hash,
        s3_object_key=s3_object_key,
    )

    # Assert object hasn't yet been completed
    with pytest.raises(botocore.exceptions.ClientError):
        output_bucket.head_object(Bucket=OUTPUT_BUCKET_NAME, Key=s3_object_key)

    # Act/Assert
    with pytest.raises(Exception):
        validate_upload(
            workflow_run=WORKFLOW_RUN,
            glacier_object_id=s3_object_key,
            glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
        )


def test_validation_mismatched_checksum(
    output_bucket: S3Client,
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str],
) -> None:
    # Arrange
    s3_object_key = "test_validation_mismatched_checksum"
    glacier_retrieval_ddb_accessor = DynamoDBAccessor(glacier_retrieval_table_mock[1])
    upload_id = create_multipart_upload(output_bucket, s3_object_key=s3_object_key)[
        "UploadId"
    ]

    part_data = upload_part(
        upload_id=upload_id, part_number=1, data=TEST_DATA, s3_object_key=s3_object_key
    )

    tree_hash = get_tree_hash(TEST_DATA)
    invalid_tree_hash = get_tree_hash(b"InvalidData")

    insert_glacier_retrieval_part_data(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        part_data=part_data,
        tree_checksum=tree_hash,
        s3_object_key=s3_object_key,
    )

    insert_glacier_retrieval_metadata(
        ddb_accessor=glacier_retrieval_ddb_accessor,
        upload_id=upload_id,
        tree_checksum=invalid_tree_hash,
        s3_object_key=s3_object_key,
    )

    # Act/Assert
    with pytest.raises(GlacierValidationMismatch):
        validate_upload(
            workflow_run=WORKFLOW_RUN,
            glacier_object_id=s3_object_key,
            glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
        )
