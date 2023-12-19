"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import csv
import io
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List

import boto3

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.model.metric_record import MetricRecord
from solution.application.partial_run.archives_status_cleanup import (
    collect_non_downloaded_archives,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3.client import S3Client
else:
    DynamoDBClient = object
    S3Client = object
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)


def handle_failed_archives(workflow_run: str, bucket_name: str) -> None:
    client: DynamoDBClient = boto3.client("dynamodb")
    failed_archives: List[Dict[str, Any]] = collect_non_downloaded_archives(
        client, workflow_run
    )
    _write_csv_to_s3(workflow_run, failed_archives, bucket_name)
    _update_metric_table(
        client,
        workflow_run,
        len(failed_archives),
        sum([int(d["size"]["N"]) for d in failed_archives]),
    )


def _write_csv_to_s3(
    workflow_run: str, failed_archives: List[Dict[str, Any]], bucket_name: str
) -> None:
    client: S3Client = boto3.client("s3")
    # Convert the DynamoDB JSON to a normal dictionary, ignoring data types
    # Convert each item in the list and filter the keys
    parsed_failed_archives = [
        GlacierTransferMetadata.parse(item) for item in failed_archives
    ]
    fieldnames_map = {
        "archive_id": "ArchiveId",
        "description": "ArchiveDescription",
        "archive_creation_date": "CreationDate",
        "size": "Size",
        "sha256_tree_hash": "SHA256TreeHash",
    }
    filtered_dicts = [
        {v: getattr(item, k) for k, v in fieldnames_map.items()}
        for item in parsed_failed_archives
    ]
    # Split the data into batches
    csv_buffer = io.StringIO()
    # Create a CSV buffer
    writer = csv.DictWriter(
        csv_buffer,
        fieldnames=list(fieldnames_map.values()),
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
        escapechar="\\",
        doublequote=False,
    )
    # Write data to the CSV buffer
    writer.writeheader()
    for item in filtered_dicts:
        writer.writerow(item)

    client.put_object(
        Bucket=bucket_name,
        Key=f"{workflow_run}/failed_archives/failed_archives.csv",
        Body=csv_buffer.getvalue().encode("utf-8"),
    )


def _update_metric_table(
    ddb_client: DynamoDBClient, workflow_run: str, count: int, size: int
) -> None:
    metric_record = MetricRecord(pk=workflow_run, count_failed=count, size_failed=size)
    ddb_client.update_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        **metric_record.update_parameters("failed"),
    )
