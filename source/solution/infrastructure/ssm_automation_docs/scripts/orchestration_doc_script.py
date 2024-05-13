"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
from datetime import datetime
from typing import Dict

import boto3
from botocore.config import Config

__boto_config__ = Config(user_agent_extra="AwsSolution/SO0293/v1.1.1")

s3_storage_class_mapping: Dict[str, str] = {
    "S3 Glacier Deep Archive": "DEEP_ARCHIVE",
    "S3 Glacier Flexible Retrieval": "GLACIER",
    "S3 Glacier Instant Retrieval": "GLACIER_IR",
    "S3 Intelligent - Tiering": "INTELLIGENT_TIERING",
    "S3 One Zone - Infrequent Access": "ONEZONE_IA",
    "S3 Standard": "STANDARD",
    "S3 Standard - Infrequent Access": "STANDARD_IA",
}


def script_handler(events, _context):  # type: ignore
    sfn_client = boto3.client("stepfunctions", config=__boto_config__)

    if events.get("provided_inventory") == "YES" and not events.get("workflow_run"):
        raise ValueError(
            "WorkflowRun is required when ProvidedInventory is set to YES."
        )

    check_cross_region_transfer(
        events["allow_cross_region_data_transfer"],
        events["acknowledge_cross_region"],
        events["bucket_name"],
        events["region"],
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
        "cross_region_transfer": str(
            events["allow_cross_region_data_transfer"]
            and events["acknowledge_cross_region"] == "YES"
        ),
        "name_override_presigned_url": events["name_override_presigned_url"]
        if events["name_override_presigned_url"]
        else "",
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
    ddb_client = boto3.client("dynamodb", config=__boto_config__)
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


def check_cross_region_transfer(allow_cross_region_data_transfer: bool, acknowledge_cross_region: str, bucket_name: str, region: str):  # type: ignore
    if allow_cross_region_data_transfer and acknowledge_cross_region == "YES":
        return
    s3_client = boto3.client("s3", config=__boto_config__)
    response = s3_client.get_bucket_location(Bucket=bucket_name)
    # Buckets in Region us-east-1 have a LocationConstraint of None.
    bucket_region = (
        response["LocationConstraint"]
        if response["LocationConstraint"] != None
        else "us-east-1"
    )
    if bucket_region != region:
        raise ValueError(
            "You need to first acknowledge that you are aware of the excessive additional cost associated with cross-region data transfer, "
            "or update the stack and use a destination bucket in the same region as the S3 Glacier vault. "
            "Follow the steps in the Implementation Guide to allow a destination bucket in a different region than the S3 Glacier vault."
        )
