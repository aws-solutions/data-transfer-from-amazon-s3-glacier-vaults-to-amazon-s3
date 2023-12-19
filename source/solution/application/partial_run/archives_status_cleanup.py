"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List

import boto3

from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.paginator import QueryPaginator
    from mypy_boto3_dynamodb.type_defs import WriteRequestTypeDef
    from mypy_boto3_s3.client import S3Client
else:
    DynamoDBClient = object
    QueryPaginator = object
    S3Client = object
    WriteRequestTypeDef = object


def generate_archives_needing_status_cleanup_s3_object(
    workflow_run: str, bucket_name: str
) -> str:
    client: DynamoDBClient = boto3.client("dynamodb")

    archives_needing_status_cleanup = collect_non_downloaded_archives(
        client, workflow_run
    )

    return write_result_to_s3(
        archives_needing_status_cleanup, workflow_run, bucket_name
    )


def collect_non_downloaded_archives(
    client: DynamoDBClient, workflow_run: str
) -> List[Dict[str, Any]]:
    staged_archives = query_archives_status(
        client, workflow_run, GlacierTransferModel.StatusCode.STAGED
    )
    requested_archives = query_archives_status(
        client, workflow_run, GlacierTransferModel.StatusCode.REQUESTED
    )
    return staged_archives + requested_archives


def query_archives_status(
    client: DynamoDBClient, workflow_run: str, status: str
) -> List[Dict[str, Any]]:
    query_paginator: QueryPaginator = client.get_paginator("query")

    page_iterator = query_paginator.paginate(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        IndexName=os.environ[OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME],
        KeyConditionExpression="retrieve_status = :status",
        ExpressionAttributeValues={":status": {"S": f"{workflow_run}/{status}"}},
    )

    archives = []
    for page in page_iterator:
        archives.extend(page["Items"])

    return archives


def write_result_to_s3(
    archives: List[Dict[str, Any]], workflow_run: str, bucket_name: str
) -> str:
    s3_client: S3Client = boto3.client("s3")

    items_json_data = json.dumps(archives)
    file_name = f"{workflow_run}/archives_status_cleanup/{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    s3_client.put_object(Body=items_json_data, Bucket=bucket_name, Key=file_name)
    return file_name


def cleanup_archives_status(archives_list: List[Dict[str, Any]]) -> None:
    client: DynamoDBClient = boto3.client("dynamodb")

    put_request_list = []
    for archive in archives_list:
        archive_metadata = GlacierTransferMetadata.parse(archive["item"])
        archive_metadata.retrieve_status = f"{archive_metadata.retrieve_status}/{GlacierTransferModel.StatusCode.STOPPED}"

        put_request: WriteRequestTypeDef = {
            "PutRequest": {"Item": archive_metadata.marshal()}
        }
        put_request_list.append(put_request)

    for i in range(0, len(put_request_list), 25):
        batch = put_request_list[i : i + 25]
        if batch:
            client.batch_write_item(
                RequestItems={
                    os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]: batch
                }
            )
