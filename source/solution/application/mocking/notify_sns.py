"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

import boto3

from solution.application import __boto_config__
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.mocking.mock_glacier_apis import MockGlacierAPIs

if TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient
else:
    SNSClient = object

NOTIFICATION_DELAY_IN_SEC = 10


def notify_sns_job_completion(
    account_id: str,
    vault_name: str,
    job_id: str,
    sns_topic: str,
    retrieval_type: str,
    archive_id: str,
) -> None:
    client: SNSClient = boto3.client("sns", config=__boto_config__)
    aws_partition = client.meta.partition
    mock_client = MockGlacierAPIs()
    if retrieval_type == GlacierJobType.INVENTORY_RETRIEVAL:
        message = {
            "Action": "InventoryRetrieval",
            "Completed": True,
            "CompletionDate": datetime.utcnow().isoformat(),
            "CreationDate": "2023-01-01T02:02:02.002Z",
            "InventoryRetrievalParameters": {
                "Format": "CSV",
            },
            "InventorySizeInBytes": mock_client.MOCK_DATA[vault_name]["inventory-metadata"]["size"],  # type: ignore
            "JobDescription": "Mock response from mock lambda",
            "JobId": job_id,
            "SNSTopic": sns_topic,
            "StatusCode": "Succeeded",
            "StatusMessage": "Succeeded",
            "VaultARN": f"arn:{aws_partition}:glacier:{os.environ['AWS_REGION']}:{account_id}:vaults/{vault_name}",
        }
    else:
        inventory_body = generate_inventory_for_archive_retrieval(
            vault_name, mock_client
        )

        inventory_body_csv_str_list = inventory_body.splitlines()
        for line in inventory_body_csv_str_list:
            (
                cur_archive_id,
                _,
                creation_date,
                size,
                sha256_tree_hash,
            ) = line.split(",")
            if cur_archive_id == archive_id:
                break

        message = {
            "Action": "ArchiveRetrieval",
            "ArchiveId": archive_id,
            "ArchiveSizeInBytes": size,
            "ArchiveSHA256TreeHash": sha256_tree_hash,
            "Completed": True,
            "CompletionDate": datetime.utcnow().isoformat(),
            "CreationDate": creation_date,
            "JobId": job_id,
            "SHA256TreeHash": sha256_tree_hash,
            "SNSTopic": sns_topic,
            "StatusCode": "Succeeded",
            "StatusMessage": "Succeeded",
            "Tier": "Bulk",
            "VaultARN": f"arn:{aws_partition}:glacier:{os.environ['AWS_REGION']}:{account_id}:vaults/{vault_name}",
        }

    time.sleep(NOTIFICATION_DELAY_IN_SEC)

    client.publish(
        TopicArn=sns_topic,
        Message=json.dumps(message),
        Subject="Notification From Mocking Glacier Service",
    )


def generate_inventory_for_archive_retrieval(
    vault_name: str, mock_client: MockGlacierAPIs
) -> str:
    inventory_job_id = mock_client.MOCK_DATA[vault_name]["initiate-job"]["inventory-retrieval"]["jobId"]  # type: ignore

    if "body" in mock_client.MOCK_DATA[vault_name]["get-job-output"][inventory_job_id]:  # type: ignore
        return mock_client.MOCK_DATA[vault_name]["get-job-output"][inventory_job_id]["body"]  # type: ignore

    for key in mock_client.MOCK_DATA[vault_name]["get-job-output"][inventory_job_id]:  # type: ignore
        if key.startswith("bytes"):
            return mock_client.MOCK_DATA[vault_name]["get-job-output"][inventory_job_id][key]["body"]  # type: ignore

    return ""
