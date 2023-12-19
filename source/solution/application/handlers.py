"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, cast

from solution.application.archive_naming.override import (
    create_header_file,
    upload_provided_file,
)
from solution.application.archive_retrieval import notification_processor
from solution.application.archive_retrieval.initiator import (
    extend_retrieval,
    initiate_retrieval,
)
from solution.application.archive_retrieval.timeout import calculate_timeout
from solution.application.chunking.chunk_generator import (
    calculate_chunk_size,
    generate_chunk_array,
)
from solution.application.completion.checker import check_workflow_completion
from solution.application.download_window.extension import generate_archives_s3_object
from solution.application.facilitator import processor
from solution.application.glacier_s3_transfer.facilitator import GlacierToS3Facilitator
from solution.application.glacier_s3_transfer.multipart_cleanup import MultipartCleanup
from solution.application.glacier_s3_transfer.validator import validate_upload
from solution.application.glacier_service.glacier_apis_factory import GlacierAPIsFactory
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.metrics.status_controller import StatusMetricController
from solution.application.model import events
from solution.application.operational_metrics.anonymized_stats import send_job_stats
from solution.application.partial_run.archives_status_cleanup import (
    cleanup_archives_status,
    generate_archives_needing_status_cleanup_s3_object,
)
from solution.application.post_workflow.dashboard_update import handle_failed_archives
from solution.application.util.exceptions import InvalidLambdaParameter

if TYPE_CHECKING:
    from mypy_boto3_glacier.type_defs import (
        InitiateJobInputRequestTypeDef,
        InitiateJobOutputTypeDef,
    )
    from mypy_boto3_s3.type_defs import CompleteMultipartUploadOutputTypeDef

    from solution.application.model.responses import (
        GlacierRetrieval as GlacierRetrievalResponse,
    )
    from solution.application.model.responses import (
        InitiateArchiveRetrieval as InitiateArchiveRetrievalResponse,
    )
else:
    GlacierRetrievalResponse = object
    CompleteMultipartUploadOutputTypeDef = object
    InitiateJobInputRequestTypeDef = object
    InitiateJobOutputTypeDef = object
    InitiateArchiveRetrievalResponse = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_EVENT_SOURCE = "aws:dynamodb"
SQS_EVENT_SOURCE = "aws:sqs"


def handler(func: Callable[[Any, Any], Any]) -> Any:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """A wrapper function for Lambda handlers"""

        logger.info(f"Invoking {func.__name__} lambda with Event: {args[0]}")
        return func(*args, **kwargs)

    return wrapper


@handler
def notifications_processor(event: dict[str, Any], _: Any) -> None:
    for record in event["Records"]:
        if record.get("eventSource") == SQS_EVENT_SOURCE:
            body = json.loads(record["body"])
            message = json.loads(body["Message"])

            logging.debug(f"SNS job notification: {message}")

            if message.get("Action") == "ArchiveRetrieval":
                notification_processor.handle_archive_job_notification(body["Message"])
            elif message.get("Action") == "InventoryRetrieval":
                processor.handle_job_notification(body["Message"])


@handler
def async_facilitator(event: dict[str, Any], _: Any) -> None:
    for record in event["Records"]:
        if record.get("eventSource") == DYNAMODB_EVENT_SOURCE and record.get(
            "eventName"
        ) in (
            "INSERT",
            "MODIFY",
        ):
            processor.handle_record_changed(record["dynamodb"]["NewImage"])


@handler
def archive_retrieval(event: dict[str, Any], _context: Any) -> None:
    for record in event["Records"]:
        if record.get("eventSource") == SQS_EVENT_SOURCE:
            body: events.GlacierRetrieval = json.loads(record["body"])
            facilitator = GlacierToS3Facilitator(
                glacier_client=GlacierAPIsFactory.create_instance(
                    os.getenv("MockGlacier") == "True"
                ),
                vault_name=body["VaultName"],
                workflow_run=body["WorkflowRun"],
                byte_range=body["ByteRange"],
                glacier_object_id=body["GlacierObjectId"],
                s3_destination_bucket=body["S3DestinationBucket"],
                s3_destination_key=body["S3DestinationKey"],
                upload_id=body["UploadId"],
                part_number=body["PartNumber"],
                glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
            )
            if facilitator.transfer():
                facilitator.send_validation_event()


@handler
def archive_validation(event: dict[str, Any], _context: Any) -> None:
    for record in event["Records"]:
        if record.get("eventSource") == SQS_EVENT_SOURCE:
            body = json.loads(record["body"])
            logging.debug(f"Validation body: {body}")

            validate_upload(
                workflow_run=body["WorkflowRun"],
                glacier_object_id=body["GlacierObjectId"],
                glacier_job_type=GlacierJobType.ARCHIVE_RETRIEVAL,
            )


@handler
def inventory_chunking(event: events.Chunk, _context: Any) -> Dict[str, Any]:
    inventory_size = int(event["TotalSize"])

    chunks = generate_chunk_array(
        inventory_size, calculate_chunk_size(inventory_size), False
    )

    return {"body": chunks}


@handler
def inventory_retrieval(
    event: events.GlacierRetrieval, _context: Any
) -> None | GlacierRetrievalResponse:
    facilitator = GlacierToS3Facilitator(
        glacier_client=GlacierAPIsFactory.create_instance(
            os.getenv("MockGlacier") == "True"
        ),
        vault_name=event["VaultName"],
        workflow_run=event["WorkflowRun"],
        byte_range=event["ByteRange"],
        glacier_object_id=event["GlacierObjectId"],
        s3_destination_bucket=event["S3DestinationBucket"],
        s3_destination_key=event["S3DestinationKey"],
        upload_id=event["UploadId"],
        part_number=event["PartNumber"],
        glacier_job_type=GlacierJobType.INVENTORY_RETRIEVAL,
    )
    return facilitator.transfer()


@handler
def archive_chunking(event: events.Chunk, _context: Any) -> Dict[str, Any]:
    archive_size = int(event["TotalSize"])
    archive_chunk_size = int(event["ChunkSize"])

    chunks = generate_chunk_array(archive_size, archive_chunk_size)

    return {"body": chunks}


@handler
def inventory_validation(
    event: events.MultipartUploadValidation, _context: Any
) -> None:
    validate_upload(
        workflow_run=event["WorkflowRun"],
        glacier_object_id=event["GlacierObjectId"],
        glacier_job_type=GlacierJobType.INVENTORY_RETRIEVAL,
    )


@handler
def initiate_archive_retrieval_batch(
    event: events.InitiateArchiveRetrievalBatch, _context: Any
) -> None:
    glacier_client = GlacierAPIsFactory.create_instance(
        os.getenv("MockGlacier") == "True"
    )
    account_id = event["AccountId"]
    sns_topic = event["SNSTopic"]
    items = event["Items"]

    initiate_retrieval(account_id, sns_topic, items, glacier_client)


@handler
def extend_archive_retrieval_batch(
    event: events.ExtendArchiveRetrievalBatch, _context: Any
) -> None:
    glacier_client = GlacierAPIsFactory.create_instance(
        os.getenv("MockGlacier") == "True"
    )
    account_id = event["AccountId"]
    sns_topic = event["SNSTopic"]
    items = event["Items"]

    extend_retrieval(account_id, sns_topic, items, glacier_client)


@handler
def initiate_archive_retrieval(
    event: events.InitiateArchiveRetrieval, _context: Any
) -> InitiateArchiveRetrievalResponse:
    glacier_client = GlacierAPIsFactory.create_instance(
        os.getenv("MockGlacier") == "True"
    )
    event["jobParameters"]["Type"] = GlacierJobType.ARCHIVE_RETRIEVAL
    initiate_job_response: InitiateJobOutputTypeDef = glacier_client.initiate_job(
        vaultName=event["vaultName"],
        accountId=event["accountId"],
        jobParameters=event["jobParameters"],
    )
    response = cast(InitiateArchiveRetrievalResponse, initiate_job_response)
    return response


@handler
def initiate_inventory_retrieval(
    event: InitiateJobInputRequestTypeDef, _context: Any
) -> InitiateJobOutputTypeDef:
    glacier_client = GlacierAPIsFactory.create_instance(
        os.getenv("MockGlacier") == "True"
    )
    event["jobParameters"]["Type"] = GlacierJobType.INVENTORY_RETRIEVAL
    response = glacier_client.initiate_job(
        vaultName=event["vaultName"],
        accountId=event["accountId"],
        jobParameters=event["jobParameters"],
    )
    return response


@handler
def archive_naming_overrides(
    event: events.ArchiveNamingOverride, _context: Any
) -> None:
    # Place an empty Naming override file in the inventory bucket to provide Glue with the correct headers
    # This is necessary for cases when the user does not provide a naming override file
    create_header_file(event["WorkflowRun"])

    if event.get("NameOverridePresignedURL") is not None:
        upload_provided_file(event["WorkflowRun"], event["NameOverridePresignedURL"])
    else:
        logger.info("No name override file is provided.")


@handler
def archives_needing_window_extension(event: dict[str, Any], _: Any) -> Dict[str, Any]:
    archives_s3_object = generate_archives_s3_object(
        event["workflow_run"], event["BucketName"]
    )
    return {"s3_key": archives_s3_object}


@handler
def cleanup_incomplete_multipart_uploads(
    event: events.RemoveMultipartUploads, _: Any
) -> int:
    # Explicitly checking for WorkflowRun presence to ensure we don't cancel all Multipart Uploads in the account
    if not event["WorkflowRun"]:
        raise InvalidLambdaParameter("WorkflowRun")
    cleaner = MultipartCleanup(
        workflow_run=event["WorkflowRun"], bucket_name=event["S3DestinationBucket"]
    )
    deleted_multipart_uploads = cleaner.cleanup()
    logger.info(f"Deleted {deleted_multipart_uploads} multipart uploads")
    return deleted_multipart_uploads


@handler
def anonymized_stats(event: dict[str, Any], _: Any) -> None:
    send_job_stats(
        event["stats_type"],
        event["workflow_run"],
    )


@handler
def initiation_timeout(event: dict[str, Any], _: Any) -> Dict[str, Any]:
    timeout = calculate_timeout(event["workflow_run"])
    return {"wait_sec": timeout}


@handler
def completion_checker(event: dict[str, Any], _: Any) -> Dict[str, Any]:
    status = check_workflow_completion(event["workflow_run"])
    return {"workflow_status": status}


@handler
def archives_needing_status_cleanup(event: dict[str, Any], _: Any) -> Dict[str, Any]:
    archives_s3_object = generate_archives_needing_status_cleanup_s3_object(
        event["workflow_run"], event["BucketName"]
    )
    return {"s3_key": archives_s3_object}


@handler
def cleanup_archives_status_batch(event: dict[str, Any], _: Any) -> None:
    cleanup_archives_status(event["Items"])


@handler
def update_metric_on_status_change(event: dict[str, Any], _: Any) -> None:
    status_metric_controller = StatusMetricController(event["Records"])
    status_metric_controller.handle_archive_status_changed()


@handler
def post_workflow_dashboard_update(event: dict[str, Any], _: Any) -> None:
    handle_failed_archives(event["WorkflowRun"], event["BucketName"])
