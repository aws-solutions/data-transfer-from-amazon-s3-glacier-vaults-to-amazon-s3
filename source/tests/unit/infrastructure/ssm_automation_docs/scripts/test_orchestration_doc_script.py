"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
from typing import Tuple
from unittest import mock

import boto3
import pytest
from mypy_boto3_dynamodb import DynamoDBClient

from solution.infrastructure.ssm_automation_docs.scripts.orchestration_doc_script import (
    script_handler,
)


def test_script_handler_launch_automation() -> None:
    # create a mock stepfunctions client
    with mock.patch("boto3.client") as mock_client, mock.patch(
        "solution.infrastructure.ssm_automation_docs.scripts.orchestration_doc_script.datetime"
    ) as mock_datetime:
        mock_sf = mock.Mock()
        mock_sf.start_execution.return_value = {
            "executionArn": "test_arn",
            "startDate": "test_date",
        }
        mock_client.return_value = mock_sf
        mock_datetime.timestamp.return_value = "1630251600.123456"

        # sample events to pass to the handler
        events = {
            "provided_inventory": "NO",
            "sns_topic_arn": "test_arn",
            "state_machine_arn": "test_arn",
            "description": "",
            "s3_storage_class": "S3 Glacier Instant Retrieval",
            "tier": "Bulk",
            "workflow_run": "workflow_1630251600_123456",
            "migration_type": "LAUNCH",
            "name_override_presigned_url": None,
            "vault_name": "test_vault",
        }

        script_handler(events, None)  # type: ignore

        events["s3_storage_class"] = "GLACIER_IR"

        # assert that the start_execution method was called with the correct arguments
        mock_sf.start_execution.assert_called_once_with(
            stateMachineArn=events.pop("state_machine_arn"), input=json.dumps(events)
        )


def test_script_handler_launch_automation_conditional_required_workflow() -> None:
    events = {
        "provided_inventory": "YES",
        "sns_topic_arn": "test_arn",
        "state_machine_arn": "test_arn",
        "description": "",
        "s3_storage_class": "S3 Glacier Instant Retrieval",
        "tier": "Bulk",
        "migration_type": "LAUNCH",
        "name_override_presigned_url": None,
        "vault_name": "test_vault",
    }
    with pytest.raises(ValueError) as err:
        script_handler(events, None)  # type: ignore
    assert "WorkflowRun is required when ProvidedInventory is set to YES." in str(
        err.value
    )


def test_script_handler_resume_automation(
    glacier_retrieval_table_mock: Tuple[DynamoDBClient, str]
) -> None:
    glacier_retrieval_table_mock[0].put_item(
        TableName=glacier_retrieval_table_mock[1],
        Item={
            "pk": {"S": "workflow_1630251600_123456"},
            "sk": {"S": "meta"},
            "vault_name": {"S": "test_vault_name"},
        },
    )

    with mock.patch("boto3.client") as mock_client, mock.patch(
        "solution.infrastructure.ssm_automation_docs.scripts.orchestration_doc_script.datetime"
    ) as mock_datetime:
        mock_sf = mock.Mock()
        mock_sf.start_execution.return_value = {
            "executionArn": "test_arn",
            "startDate": "test_date",
        }
        mock_client.side_effect = [mock_sf, glacier_retrieval_table_mock[0]]
        mock_datetime.timestamp.return_value = "1630251600.123456"

        # sample events to pass to the handler
        events = {
            "provided_inventory": "YES",
            "sns_topic_arn": "test_arn",
            "state_machine_arn": "test_arn",
            "description": "",
            "s3_storage_class": "S3 Glacier Instant Retrieval",
            "tier": "Bulk",
            "workflow_run": "workflow_1630251600_123456",
            "migration_type": "RESUME",
            "table_name": glacier_retrieval_table_mock[1],
            "name_override_presigned_url": None,
        }
        script_handler(events, None)  # type: ignore
        events.pop("table_name")
        events["s3_storage_class"] = "GLACIER_IR"
        events["vault_name"] = "test_vault_name"
        # assert that the start_execution method was called with the correct arguments
        mock_sf.start_execution.assert_called_once_with(
            stateMachineArn=events.pop("state_machine_arn"), input=json.dumps(events)
        )
