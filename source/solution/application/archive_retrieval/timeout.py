"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

from solution.application.chunking.chunk_generator import (
    calculate_chunk_size,
    generate_chunk_array,
)
from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.model.partition_metric_record import PartitionMetricRecord
from solution.infrastructure.output_keys import OutputKeys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def calculate_timeout(workflow_run: str) -> int:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.METRIC_TABLE_NAME])
    partition_metric = ddb_accessor.get_item(
        key={"pk": {"S": PartitionMetricRecord.partition_key(workflow_run)}}
    )

    if partition_metric is None:
        return 0

    partition_metric_record = PartitionMetricRecord.parse(partition_metric)

    if (
        not partition_metric_record.start_time
        or not partition_metric_record.archives_size
    ):
        logger.error("Failed to retrieve partition data from partition_metric_record")
        return 0

    archives_size = int(partition_metric_record.archives_size)
    start_time = partition_metric_record.start_time

    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])
    workflow_metadata = ddb_accessor.get_item(
        key={
            "pk": {"S": workflow_run},
            "sk": {"S": "meta"},
        },
    )

    if workflow_metadata is None:
        return 0

    daily_quota = int(workflow_metadata["daily_quota"]["N"])
    end = datetime.now()

    start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    time_taken = end - start
    logger.info(f"Time taken: {time_taken.total_seconds()}")

    processed_share = archives_size / daily_quota
    logger.info(f"Processed_share: {processed_share}")

    timeout = int(round(24 * 60 * 60 * processed_share) - time_taken.total_seconds())

    if timeout < 0:
        timeout = 0
    logger.info(f"Timeout: {timeout} secs")

    return timeout
