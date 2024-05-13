"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
from concurrent import futures
from datetime import datetime
from timeit import default_timer as timer
from typing import TYPE_CHECKING, Any, List

import boto3

from solution.application import __boto_config__
from solution.application.chunking.chunk_generator import calculate_chunk_size
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.model import events
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.partition_metric_record import PartitionMetricRecord
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import PutItemOutputTypeDef
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import JobParametersTypeDef
else:
    DynamoDBClient = object
    PutItemOutputTypeDef = object
    GlacierClient = object
    JobParametersTypeDef = object

logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


def initiate_retrieval(
    account_id: str,
    sns_topic: str,
    items: List[events.InitiateArchiveRetrievalItem],
    glacier_client: GlacierClient,
) -> None:
    ddb_client: DynamoDBClient = boto3.client("dynamodb", config=__boto_config__)

    if not items:
        return

    start = timer()

    workflow_run = items[0]["workflow_run"]
    with futures.ThreadPoolExecutor(max_workers=10) as upload_executor:
        upload_futures = [
            upload_executor.submit(
                initiate_request,
                item,
                ddb_client,
                glacier_client,
                sns_topic,
                account_id,
            )
            for item in items
        ]
        for future in upload_futures:
            future.result()

    tps = len(items) / (timer() - start)
    logger.info(f"TPS: {tps}")

    total_archives_size = sum(int(item["item"]["Size"]) for item in items)

    partition_metric_record = PartitionMetricRecord(
        pk=PartitionMetricRecord.partition_key(workflow_run),
        archives_count=len(items),
        archives_size=total_archives_size,
    )
    ddb_client.update_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        **partition_metric_record.update_parameters(),
    )


def extend_retrieval(
    account_id: str,
    sns_topic: str,
    items: List[events.ExtendArchiveRetrievalItem],
    glacier_client: GlacierClient,
) -> None:
    ddb_client: DynamoDBClient = boto3.client("dynamodb", config=__boto_config__)

    if not items:
        return

    with futures.ThreadPoolExecutor(max_workers=10) as upload_executor:
        upload_futures = [
            upload_executor.submit(
                extend_request,
                item,
                ddb_client,
                glacier_client,
                sns_topic,
                account_id,
            )
            for item in items
        ]
        for future in upload_futures:
            future.result()


def extend_request(
    item: events.ExtendArchiveRetrievalItem,
    ddb_client: DynamoDBClient,
    glacier_client: GlacierClient,
    sns_topic: str,
    account_id: str,
) -> None:
    vault_name = item["vault_name"]
    workflow_run = item["workflow_run"]
    archive = item["item"]
    archive_id = archive["archive_id"]["S"]
    tier = item["tier"]

    if is_already_downloaded(workflow_run, archive_id, ddb_client):
        logger.info(
            "Archive {workflow_run}:{archive_id} already downloaded, skipping job extension"
        )
        return

    job_id = glacier_initiate_job(
        glacier_client, vault_name, sns_topic, archive_id, tier, account_id
    )

    if not job_id:
        return

    ddb_client.update_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Key=GlacierTransferMetadataRead(
            workflow_run=workflow_run, glacier_object_id=archive_id
        ).key,
        UpdateExpression="SET job_id = :ji, retrieve_status = :rs",
        ExpressionAttributeValues={
            ":ji": {"S": f"{job_id}"},
            ":rs": {"S": f"{workflow_run}/{GlacierTransferModel.StatusCode.EXTENDED}"},
        },
    )


def glacier_initiate_job(
    glacier_client: GlacierClient,
    vault_name: str,
    sns_topic: str,
    archive_id: str,
    tier: str,
    account_id: str,
) -> str | None:
    job_parameters: JobParametersTypeDef = {
        "Type": GlacierJobType.ARCHIVE_RETRIEVAL,
        "SNSTopic": sns_topic,
        "ArchiveId": archive_id,
        "Tier": tier,
    }
    try:
        initiate_job_response = glacier_client.initiate_job(
            vaultName=vault_name,
            accountId=account_id,
            jobParameters=job_parameters,
        )
    except Exception as e:
        logger.error(
            f"An error occurred while initiating job for {job_parameters}. Error: {e}"
        )
        return None

    return initiate_job_response["jobId"]


def initiate_request(
    item: events.InitiateArchiveRetrievalItem,
    ddb_client: DynamoDBClient,
    glacier_client: GlacierClient,
    sns_topic: str,
    account_id: str,
) -> None:
    vault_name = item["vault_name"]
    workflow_run = item["workflow_run"]
    tier = item["tier"]
    archive = item["item"]
    s3_storage_class = item["s3_storage_class"]
    archive_id = archive["ArchiveId"]

    if is_already_downloaded(workflow_run, archive_id, ddb_client):
        logger.info(
            "Archive {workflow_run}:{archive_id} already downloaded, skipping job initiation"
        )
        return

    job_id = glacier_initiate_job(
        glacier_client, vault_name, sns_topic, archive_id, tier, account_id
    )

    if not job_id:
        return

    archive_metadata = GlacierTransferMetadata(
        workflow_run=workflow_run,
        glacier_object_id=archive_id,
        job_id=job_id,
        start_time=datetime.now().isoformat(),
        vault_name=vault_name,
        archive_id=archive_id,
        sha256_tree_hash=archive["SHA256TreeHash"],
        retrieval_type=GlacierJobType.ARCHIVE_RETRIEVAL,
        size=int(archive["Size"]),
        chunk_size=calculate_chunk_size(int(archive["Size"])),
        archive_creation_date=archive["CreationDate"],
        retrieve_status=f"{workflow_run}/{GlacierTransferModel.StatusCode.REQUESTED}",
        file_name=archive["Filename"] or archive_id,
        s3_storage_class=s3_storage_class,
        description=archive["ArchiveDescription"],
    )
    put_glacier_transfer_metadata(archive_metadata.marshal(), ddb_client)


def is_already_downloaded(
    workflow_run: str, archive_id: str, ddb_client: DynamoDBClient
) -> bool:
    response = ddb_client.get_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Key=GlacierTransferMetadataRead(
            workflow_run=workflow_run, glacier_object_id=archive_id
        ).key,
    )

    if "Item" in response:
        return bool(
            response["Item"]["retrieve_status"]["S"].endswith(
                f"/{GlacierTransferModel.StatusCode.DOWNLOADED}"
            )
        )
    return False


def put_glacier_transfer_metadata(
    item: dict[str, Any], ddb_client: DynamoDBClient
) -> PutItemOutputTypeDef:
    return ddb_client.put_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], Item={**item}
    )
