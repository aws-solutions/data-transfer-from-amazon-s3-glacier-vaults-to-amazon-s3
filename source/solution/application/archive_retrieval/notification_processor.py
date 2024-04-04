"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import logging
import os
from typing import TYPE_CHECKING, List

import boto3

from solution.application import __boto_config__
from solution.application.chunking.chunk_generator import (
    calculate_chunk_size,
    generate_chunk_array,
)
from solution.application.model.facilitator import JobCompletionEvent
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3.client import S3Client
else:
    DynamoDBClient = object
    S3Client = object


logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


def handle_archive_job_notification(message_str: str) -> None:
    logging.info(f"SNS Archive job notification: {message_str}")
    message = json.loads(message_str)
    event: JobCompletionEvent = JobCompletionEvent.parse(
        message, raw_message=message_str
    )

    if not event.completed or not event.completion_date:
        logger.info(f"ignoring incomplete event: {message_str}")
        return

    ddb_client = boto3.client("dynamodb", config=__boto_config__)

    archive_id = message["ArchiveId"]
    glacier_transfer_record = get_glacier_transfer_metadata(ddb_client, event.job_id)

    if glacier_transfer_record is None:
        logger.info(
            f"Failed to pull Archive metadata for Archive: {archive_id}, Job:{event.job_id}"
        )
        return

    workflow_run = glacier_transfer_record.workflow_run
    retrieve_status = glacier_transfer_record.retrieve_status

    if retrieve_status.endswith(f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"):
        logger.info(
            f"Archive {workflow_run}:{archive_id} already downloaded, skipping job notification"
        )
        return

    if retrieve_status.endswith(f"/{GlacierTransferModel.StatusCode.STAGED}"):
        logger.info(
            f"Archive {workflow_run}:{archive_id} already staged, duplicate job notification"
        )
        return

    archive_size = glacier_transfer_record.size
    file_name = glacier_transfer_record.file_name
    storage_class = glacier_transfer_record.s3_storage_class
    vault_name = glacier_transfer_record.vault_name
    object_key = f"{workflow_run}/{file_name}"

    logging.info(
        f"Calculate chunks for Archive {workflow_run}:{archive_id} - Size: {archive_size} - file_name: {file_name} - s3_storage_class: {storage_class}"
    )

    if archive_size is None:
        logging.info("Can not retrieve Archive size from metadata.")
        return

    chunks = generate_chunk_array(archive_size, calculate_chunk_size(archive_size))

    upload_id = create_multipart_upload(object_key, storage_class)

    retrieve_status_staged = f"{workflow_run}/{GlacierTransferModel.StatusCode.STAGED}"
    update_glacier_transfer_metadata(
        ddb_client,
        workflow_run,
        archive_id,
        str(len(chunks)),
        upload_id,
        object_key,
        retrieve_status_staged,
        event.completion_date,
        event.job_id,
    )

    send_chunk_events(
        chunks,
        event.job_id,
        workflow_run,
        vault_name,
        archive_id,
        upload_id,
        object_key,
    )


def get_glacier_transfer_metadata(
    ddb_client: DynamoDBClient, job_id: str
) -> None | GlacierTransferMetadata:
    response = ddb_client.query(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        IndexName=os.environ[OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME],
        KeyConditionExpression="job_id = :ji",
        ExpressionAttributeValues={":ji": {"S": job_id}},
        Limit=1,
    )

    if response.get("Items"):
        return GlacierTransferMetadata.parse(response["Items"][0])
    return None


def create_multipart_upload(object_key: str, storage_class: str) -> str:
    bucket_name = os.environ[OutputKeys.OUTPUT_BUCKET_NAME]
    s3_client: S3Client = boto3.client("s3", config=__boto_config__)
    multipart_response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key,
        ChecksumAlgorithm="SHA256",
        StorageClass=storage_class,  # type: ignore
        ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
    )
    return multipart_response["UploadId"]


def send_chunk_events(
    chunks: List[str],
    job_id: str,
    workflow_run: str,
    vault_name: str,
    archive_id: str,
    upload_id: str,
    object_key: str,
) -> None:
    chunk_sqs_url = os.environ[OutputKeys.CHUNKS_SQS_URL]
    bucket_name = os.environ[OutputKeys.OUTPUT_BUCKET_NAME]
    sqs = boto3.client("sqs", config=__boto_config__)
    for index, chunk in enumerate(chunks):
        message_body = {
            "JobId": job_id,
            "VaultName": vault_name,
            "ByteRange": chunk,
            "S3DestinationBucket": bucket_name,
            "S3DestinationKey": object_key,
            "GlacierObjectId": archive_id,
            "UploadId": upload_id,
            "PartNumber": index + 1,
            "WorkflowRun": workflow_run,
        }
        sqs.send_message(QueueUrl=chunk_sqs_url, MessageBody=json.dumps(message_body))


def update_glacier_transfer_metadata(
    ddb_client: DynamoDBClient,
    workflow_run: str,
    archive_id: str,
    chunk_count: str,
    upload_id: str,
    object_key: str,
    retrieve_status: str,
    completion_date: str,
    job_id: str,
) -> None:
    bucket_name = os.environ[OutputKeys.OUTPUT_BUCKET_NAME]
    ddb_client.update_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Key=GlacierTransferMetadataRead(
            workflow_run=workflow_run, glacier_object_id=archive_id
        ).key,
        UpdateExpression="SET chunks_count = :cc, upload_id = :ui, s3_destination_bucket = :db, s3_destination_key = :dk, retrieve_status = :rs, download_window = :dw, staged_job_id = :sji",
        ExpressionAttributeValues={
            ":cc": {"N": chunk_count},
            ":ui": {"S": upload_id},
            ":db": {"S": bucket_name},
            ":dk": {"S": object_key},
            ":rs": {"S": retrieve_status},
            ":dw": {"S": completion_date},
            ":sji": {"S": job_id},
        },
    )
