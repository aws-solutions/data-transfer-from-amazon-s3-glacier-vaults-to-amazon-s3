"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import logging
import os
from typing import TYPE_CHECKING, Any

import boto3

from solution.application import __boto_config__
from solution.application.model.facilitator import AsyncRecord, JobCompletionEvent
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_stepfunctions import SFNClient
else:
    DynamoDBClient = object
    SFNClient = object

logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


def handle_job_notification(message: str) -> None:
    logging.info(f"SNS Inventory job notification: {message}")
    event: JobCompletionEvent = JobCompletionEvent.parse(
        json.loads(message), raw_message=message
    )

    if not event.completed:
        logger.info(f"ignoring incomplete event: {message}")
        return

    update = AsyncRecord(
        job_id=event.job_id,
        finish_timestamp=event.completion_date,
        job_result=event.raw_message,
    )
    update_record(record=update)


def handle_record_changed(attributes: dict[str, Any]) -> None:
    record = AsyncRecord.parse(attributes)

    logging.info("Async Record:")
    logging.info(record)

    if record.task_token and record.finish_timestamp and record.job_result:
        event = JobCompletionEvent.parse(
            json.loads(record.job_result), raw_message=record.job_result
        )
        notify_of_result(task_token=record.task_token, event=event)


def update_record(record: AsyncRecord) -> None:
    client: DynamoDBClient = boto3.client("dynamodb", config=__boto_config__)

    update_parameters = record.inventory_job_completion_update_parameters
    client.update_item(
        TableName=os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME],
        **update_parameters,
    )


def notify_of_result(task_token: str, event: JobCompletionEvent) -> None:
    if not task_token:
        logger.info(f"no task token to send result to for job: {event.job_id}")
        return

    client = get_sfn_client()
    try:
        if event.status_code == JobCompletionEvent.StatusCode.SUCCEEDED:
            client.send_task_success(taskToken=task_token, output=event.raw_message)
        else:
            client.send_task_failure(
                taskToken=task_token, error=event.status_code, cause=event.raw_message
            )
    except client.exceptions.TaskTimedOut:
        logger.error(
            f"Task Timed Out: Provided task does not exist anymore for job: {event.job_id}"
        )


def get_sfn_client() -> SFNClient:
    client: SFNClient = boto3.client("stepfunctions", config=__boto_config__)
    return client
