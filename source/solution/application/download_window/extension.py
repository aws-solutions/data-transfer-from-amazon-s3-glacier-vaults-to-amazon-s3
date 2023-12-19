"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List

import boto3

from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.paginator import QueryPaginator
    from mypy_boto3_s3.client import S3Client
else:
    DynamoDBClient = object
    QueryPaginator = object
    S3Client = object

ARCHIVE_READY_WAIT = 5


def generate_archives_s3_object(workflow_run: str, bucket_name: str) -> str:
    client: DynamoDBClient = boto3.client("dynamodb")
    archives = query_archives_needing_extension(client, workflow_run)
    return write_result_to_s3(archives, workflow_run, bucket_name)


def query_archives_needing_extension(
    client: DynamoDBClient, workflow_run: str
) -> List[Dict[str, Any]]:
    query_paginator: QueryPaginator = client.get_paginator("query")

    window_threshold = datetime.now() - timedelta(hours=24 - ARCHIVE_READY_WAIT)
    formatted_window_threshold = window_threshold.strftime("%Y-%m-%dT%H:%M:%S")

    key_condition_expression = "retrieve_status = :status"

    filter_expression = "download_window < :threshold"

    expression_attribute_values = {
        ":threshold": {"S": formatted_window_threshold},
        ":status": {"S": f"{workflow_run}/{GlacierTransferModel.StatusCode.STAGED}"},
    }

    page_iterator = query_paginator.paginate(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        IndexName=os.environ[OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME],
        KeyConditionExpression=key_condition_expression,
        FilterExpression=filter_expression,
        ExpressionAttributeValues=expression_attribute_values,
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
    file_name = f"{workflow_run}/download_window_extension/{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    s3_client.put_object(Body=items_json_data, Bucket=bucket_name, Key=file_name)
    return file_name
