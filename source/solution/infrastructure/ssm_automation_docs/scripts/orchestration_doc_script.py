"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
from datetime import datetime
from typing import Dict

import boto3

s3_storage_class_mapping: Dict[str, str] = {
    "S3 Standard": "STANDARD",
    "S3 Intelligent Tiering": "INTELLIGENT_TIERING",
    "S3 Standard IA": "STANDARD_IA",
    "S3 One Zone IA": "ONEZONE_IA",
    "S3 Glacier Instant Retrieval": "GLACIER_IR",
    "S3 Glacier Flexible Retrieval": "GLACIER",
    "S3 Glacier Deep Archive": "DEEP_ARCHIVE",
}


def script_handler(events, _context):  # type: ignore
    sfn_client = boto3.client("stepfunctions")

    if events.get("provided_inventory") == "YES" and not events.get("workflow_run"):
        raise ValueError(
            "WorkflowRun is required when ProvidedInventory is set to YES."
        )

    if "vault_name" in events:
        # if vault_name in event than its a launch document
        workflow_run = create_workflow_name(events.get("workflow_run"))  # type: ignore
        vault_name = events["vault_name"]
        migration_type = "LAUNCH"
    else:
        # else it is a resume document
        workflow_run = events["workflow_run"]
        vault_name = retrieve_vault_name(workflow_run, events["table_name"])
        migration_type = "RESUME"

    state_machine_input = {
        "provided_inventory": events["provided_inventory"],
        "sns_topic_arn": events["sns_topic_arn"],
        "description": events["description"],
        "s3_storage_class": s3_storage_class_mapping[events["s3_storage_class"]],
        "tier": "Bulk",
        "workflow_run": workflow_run,
        "migration_type": migration_type,
        "name_override_presigned_url": events["name_override_presigned_url"]
        if events["name_override_presigned_url"]
        else None,
        "vault_name": vault_name,
    }
    sfn_client.start_execution(
        stateMachineArn=events["state_machine_arn"],
        input=json.dumps(state_machine_input),
    )


def create_workflow_name(workflow_run=None):  # type: ignore
    timestamp = datetime.timestamp(datetime.now()).__str__().split(".")
    return workflow_run or "workflow_" + timestamp[0] + "_" + timestamp[1]


def retrieve_vault_name(workflow_run: str, table_name: str):  # type: ignore
    ddb_client = boto3.client("dynamodb")
    response = ddb_client.get_item(
        TableName=table_name,
        Key={
            "pk": {"S": workflow_run},
            "sk": {"S": "meta"},
        },
    )
    if "Item" not in response:
        raise ValueError("Workflow run not found in DynamoDB")
    if "vault_name" not in response["Item"]:
        raise ValueError("No vault name found in DynamoDB")

    return response["Item"]["vault_name"]["S"]
