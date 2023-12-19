"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import logging
import os
from base64 import b64encode
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import boto3

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.glacier_s3_transfer.download import GlacierDownload
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
    ExpiredDownloadWindow,
    GlacierValidationMismatch,
    InvalidGlacierRetrievalMetadata,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_glacier import GlacierClient

    from solution.application.model.responses import (
        GlacierRetrieval as GlacierRetrievalResponse,
    )
else:
    GlacierClient = object
    GlacierRetrievalResponse = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GlacierToS3Facilitator:
    def __init__(
        self,
        *,
        glacier_client: GlacierClient,
        vault_name: str,
        workflow_run: str,
        byte_range: str,
        glacier_object_id: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        upload_id: str,
        part_number: int,
        glacier_job_type: str,
    ) -> None:
        self.glacier_client = glacier_client
        self.vault_name = vault_name
        self.workflow_run = workflow_run
        self.byte_range = byte_range
        self.glacier_object_id = glacier_object_id

        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_key = s3_destination_key
        self.upload_id = upload_id
        self.part_number = part_number
        self.glacier_job_type = glacier_job_type

        self._get_metadata()

    def transfer(self) -> None | GlacierRetrievalResponse:
        """
        Transfers a chunk of data from an AWS Glacier vault to an S3 bucket.

        Returns a dictionary containing information about the uploaded part.

        :return: A dictionary containing information about the uploaded part.
        :rtype: GlacierRetrievalResponse

        :raises GlacierValidationMismatch: If the checksum of the downloaded chunk of data from Glacier does not match the expected checksum.

        :raises InvalidGlacierRetrievalMetadata: If download_window is not present

        :raises ExpiredDownloadWindow: If the download window has expired

        :raises botocore.exceptions.ClientError: If there is an error communicating with AWS.
        """
        job_id = self.metadata.staged_job_id
        download_window = self.metadata.download_window
        retrieve_status = self.metadata.retrieve_status

        if self.glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
            if retrieve_status.endswith(
                f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
            ):
                logger.info(
                    f"Archive {self.workflow_run}:{self.glacier_object_id} already downloaded, skipping chunk {self.part_number} download"
                )
                return None

            if download_window is None:
                raise InvalidGlacierRetrievalMetadata("Missing Download Window")
            if datetime.now() > (
                datetime.fromisoformat(download_window.replace("Z", ""))
                + timedelta(hours=24)
            ):
                raise ExpiredDownloadWindow

        try:
            download = GlacierDownload(
                self.glacier_client,
                job_id,
                self.vault_name,
                self.byte_range,
            )
            chunk = download.read()
        except self.glacier_client.exceptions.ResourceNotFoundException:
            raise ExpiredDownloadWindow

        if self.glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
            glacier_hash = TreeHash()
            glacier_hash.update(chunk)
            if glacier_hash.digest().hex() != download.checksum():
                raise GlacierValidationMismatch

        upload = S3Upload(
            self.s3_destination_bucket,
            self.s3_destination_key,
            self.upload_id,
        )
        part = upload.upload_part(chunk, self.part_number)

        if self.glacier_job_type is GlacierJobType.ARCHIVE_RETRIEVAL:
            part["TreeChecksum"] = b64encode(glacier_hash.digest()).decode("ascii")
        self._write_part_info(part)
        return part

    def send_validation_event(self) -> None:
        if not self._is_last_chunk():
            logger.info(
                "Exiting send validation events: More chunks remain for download"
            )
            return None
        sqs = boto3.client("sqs")
        validation_sqs_url = os.environ[OutputKeys.VALIDATION_SQS_URL]
        message_body = {
            "WorkflowRun": self.workflow_run,
            "GlacierObjectId": self.glacier_object_id,
        }
        sqs.send_message(
            QueueUrl=validation_sqs_url, MessageBody=json.dumps(message_body)
        )

    def _is_last_chunk(self) -> bool:
        self._get_metadata()
        ddb_accessor = DynamoDBAccessor(
            os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
        )
        self.glacier_part_items = ddb_accessor.query_items(
            "pk = :pk",
            {
                ":pk": GlacierTransferModel(
                    workflow_run=self.workflow_run,
                    glacier_object_id=self.glacier_object_id,
                ).key["pk"],
                ":sk": {"S": GlacierTransferPartRead.prefix},
            },
            "begins_with ( sk, :sk)",
            consistent_read=True,
        )
        if not self.metadata.chunks_count:
            return False
        return len(self.glacier_part_items) == int(self.metadata.chunks_count)

    def _write_part_info(self, part: GlacierRetrievalResponse) -> None:
        glacier_transfer_part = GlacierTransferPart(
            workflow_run=self.workflow_run,
            glacier_object_id=self.glacier_object_id,
            checksum_sha_256=part["ChecksumSHA256"],
            e_tag=part["ETag"],
            part_number=part["PartNumber"],
            tree_checksum=part.get("TreeChecksum"),
        )
        ddb_accessor = DynamoDBAccessor(
            os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
        )
        ddb_accessor.insert_item(glacier_transfer_part.marshal())

    def _get_metadata(self) -> None:
        """
        Retrieves Glacier retrieval metadata

        Returns the contents of the DynamoDB metadata entry for the current retrieval

        :raises InvalidGlacierRetrievalMetadata: If metadata could not be found

        """
        ddb_accessor = DynamoDBAccessor(
            os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
        )
        metadata = ddb_accessor.get_item(
            GlacierTransferMetadataRead(
                workflow_run=self.workflow_run, glacier_object_id=self.glacier_object_id
            ).key,
            consistent_read=True,
        )
        if metadata is None:
            raise InvalidGlacierRetrievalMetadata("Metadata not found")
        self.metadata: GlacierTransferMetadata = GlacierTransferMetadata.parse(metadata)
