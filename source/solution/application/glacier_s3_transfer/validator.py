"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import logging
import os
from base64 import b64decode
from typing import TYPE_CHECKING, Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.glacier_s3_transfer.upload import S3Upload
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.hashing.tree_hash import TreeHash
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.glacier_transfer_part_model import (
    GlacierTransferPart,
    GlacierTransferPartRead,
)
from solution.application.util.exceptions import (
    GlacierValidationMismatch,
    InvalidGlacierRetrievalMetadata,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import CompleteMultipartUploadOutputTypeDef
else:
    S3Client = object
    CompleteMultipartUploadOutputTypeDef = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_upload(
    *, workflow_run: str, glacier_object_id: str, glacier_job_type: str
) -> None | CompleteMultipartUploadOutputTypeDef:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])

    glacier_metadata = get_archive_metadata(workflow_run, glacier_object_id)

    if (
        not glacier_metadata.upload_id
        or not glacier_metadata.s3_destination_bucket
        or not glacier_metadata.s3_destination_key
        or not glacier_metadata.size
        or not glacier_metadata.retrieve_status
    ):
        raise InvalidGlacierRetrievalMetadata("Missing validation metadata")

    if glacier_metadata.retrieve_status.endswith(
        f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
    ):
        logger.info(
            f"Glacier object {workflow_run}:{glacier_object_id} already downloaded, skipping validation"
        )
        return None

    glacier_part_items = get_glacier_object_parts(
        workflow_run, glacier_object_id, ddb_accessor
    )

    s3_upload = S3Upload(
        glacier_metadata.s3_destination_bucket,
        glacier_metadata.s3_destination_key,
        glacier_metadata.upload_id,
    )
    tree_hash = TreeHash()

    for glacier_part_dict in glacier_part_items:
        part = GlacierTransferPart.parse(glacier_part_dict)
        s3_upload.include_part(part)
        if glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
            if part.tree_checksum is None:
                raise InvalidGlacierRetrievalMetadata(
                    "Failed to retrieve tree_checksum from part metadata"
                )
            tree_hash.include(b64decode(part.tree_checksum.encode("ascii")))

    if glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
        glacier_checksum = glacier_metadata.sha256_tree_hash
        s3_checksum = tree_hash.digest().hex()
        if s3_checksum != glacier_checksum:
            logger.error(
                f"Archive Tree hash {s3_checksum} does not match expected {glacier_checksum}"
            )
            raise GlacierValidationMismatch

    if (
        glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL
        and s3_glacier_object_exists(
            glacier_metadata.s3_destination_bucket,
            glacier_metadata.s3_destination_key,
        )
    ):
        logger.info(
            f"S3 object already exists: {glacier_metadata.s3_destination_key} - skipping completing multipart upload"
        )
        return None

    upload_response = s3_upload.complete_upload()

    if glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
        update_archive_retrieve_status(workflow_run, glacier_object_id)

    if glacier_job_type is GlacierJobType.INVENTORY_RETRIEVAL:
        s3_file_size = s3_upload.get_file_size()
        if glacier_metadata.size != s3_file_size:
            logger.error(
                f"Inventory size {s3_file_size} does not match expected {glacier_metadata.size}"
            )
            raise GlacierValidationMismatch

    return upload_response


def s3_glacier_object_exists(
    s3_destination_bucket: str, s3_destination_key: str
) -> bool:
    s3_client: S3Client = boto3.client("s3")

    try:
        logger.info(f"Check if S3 object exists: {s3_destination_key}")
        s3_client.head_object(
            Bucket=s3_destination_bucket,
            Key=s3_destination_key,
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "404":
            logger.info(f"S3 object does not exist: {s3_destination_key}")
            return False
        else:
            logger.error(f"Exception occurred while checking archive existence {err}")
            raise
    return True


def get_glacier_object_parts(
    workflow_run: str, glacier_object_id: str, ddb_accessor: DynamoDBAccessor
) -> List[Dict[str, Any]]:
    return ddb_accessor.query_items(
        "pk = :pk",
        {
            ":pk": GlacierTransferModel(
                workflow_run=workflow_run, glacier_object_id=glacier_object_id
            ).key["pk"],
            ":sk": {"S": GlacierTransferPartRead.prefix},
        },
        "begins_with ( sk, :sk)",
        consistent_read=True,
    )


def update_archive_retrieve_status(workflow_run: str, archive_id: str) -> None:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])
    ddb_accessor.update_item(
        key=GlacierTransferMetadataRead(
            workflow_run=workflow_run, glacier_object_id=archive_id
        ).key,
        update_expression="SET retrieve_status = :rs",
        expression_attribute_values={
            ":rs": {
                "S": f"{workflow_run}/{GlacierTransferModel.StatusCode.DOWNLOADED}"
            },
        },
    )


def get_archive_metadata(workflow_run: str, archive_id: str) -> GlacierTransferMetadata:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])
    read_glacier_metadata = GlacierTransferMetadataRead(
        workflow_run=workflow_run, glacier_object_id=archive_id
    )
    ddb_metadata = ddb_accessor.get_item(
        read_glacier_metadata.key, consistent_read=True
    )

    if ddb_metadata is None:
        raise InvalidGlacierRetrievalMetadata(
            "Failed to retrieve chunks_count value from archive metadata"
        )

    return GlacierTransferMetadata.parse(ddb_metadata)
