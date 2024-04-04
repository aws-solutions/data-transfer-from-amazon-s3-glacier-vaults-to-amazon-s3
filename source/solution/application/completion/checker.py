"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import datetime
import json
import logging
import os
from typing import TYPE_CHECKING

import boto3

from solution.application import __boto_config__
from solution.application.model.facilitator import AsyncRecord, JobCompletionEvent
from solution.application.model.metric_record import MetricRecord
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = object


logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


def check_workflow_completion(workflow_run: str) -> str:
    ddb_client: DynamoDBClient = boto3.client("dynamodb", config=__boto_config__)
    if is_workflow_completed(workflow_run, ddb_client):
        job_id = f"{workflow_run}|COMPLETION"
        async_record = AsyncRecord(
            job_id=job_id,
            finish_timestamp=datetime.datetime.now().isoformat(),
            job_result=json.dumps(
                {
                    "JobId": job_id,
                    "Completed": True,
                    "StatusCode": JobCompletionEvent.StatusCode.SUCCEEDED,
                }
            ),
        )
        update_record(async_record, ddb_client)
        return "Completed"
    return "InProgress"


def update_record(record: AsyncRecord, ddb_client: DynamoDBClient) -> None:
    update_parameters = record.inventory_job_completion_update_parameters
    ddb_client.update_item(
        TableName=os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME],
        **update_parameters,
    )


def is_workflow_completed(workflow_run: str, ddb_client: DynamoDBClient) -> bool:
    response = ddb_client.get_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Key=MetricRecord(pk=workflow_run).key,
    )

    if "Item" not in response:
        return False

    metric_record = MetricRecord.parse(response["Item"])

    if metric_record.count_downloaded is None or metric_record.count_total is None:
        return False

    if metric_record.count_downloaded >= metric_record.count_total:
        logger.info(f"Workflow completed!")
        return True
    logger.info(f"Workflow not completed!")
    return False
