"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import time
from typing import TYPE_CHECKING, Any, Dict, List

import boto3
import pytest
from botocore.exceptions import WaiterError
from tests.integration.infrastructure.util import ddb_util, s3_util

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_s3 import S3Client
else:
    LambdaClient = object
    S3Client = object

import os

import pytest

WORKFLOW_RUN = "test_retrieve_archive_workflow"


@pytest.fixture(autouse=True, scope="module")
def setup() -> Any:
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    s3_util.delete_archives_from_s3(prefix=WORKFLOW_RUN)
    yield
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    s3_util.delete_archives_from_s3(prefix=WORKFLOW_RUN)


@pytest.fixture(scope="module")
def s3_client() -> S3Client:
    client: S3Client = boto3.client("s3")
    return client


@pytest.fixture(scope="module")
def lambda_client() -> LambdaClient:
    client: LambdaClient = boto3.client("lambda")
    return client


def invoke_initiate_archive_retrieval_lambda(
    lambda_client: LambdaClient, sns_topic: str, input_items: List[Dict[str, Any]]
) -> None:
    lambda_client.invoke(
        FunctionName=os.environ[OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_ARN],
        InvocationType="Event",
        Payload=json.dumps(
            {
                "AccountId": None,
                "SNSTopic": sns_topic,
                "Items": input_items,
            }
        ),
    )


def get_mock_inventory(vault_name: str) -> str:
    inventory_job_id = MOCK_DATA[vault_name]["initiate-job"][  # type: ignore
        GlacierJobType.INVENTORY_RETRIEVAL
    ]["jobId"]
    inventory_job_size = MOCK_DATA[vault_name]["inventory-metadata"]["size"]  # type: ignore
    inventory: str = MOCK_DATA[vault_name]["get-job-output"][inventory_job_id][  # type: ignore
        f"bytes=0-{inventory_job_size-1}"
    ][
        "body"
    ]
    return inventory


def build_initiate_retrieval_input(
    inventory: str, vault_name: str
) -> List[Dict[str, Any]]:
    initiate_retrieval_list = []
    # Iterate over the inventory, ignoring the headers
    for record in inventory.splitlines()[1:]:
        (
            archive_id,
            archive_description,
            creation_date,
            size,
            sha256_tree_hash,
        ) = record.split(",")
        # Create a Lambda usable item for each archive in the inventory
        initiate_retrieval_list.append(
            {
                "item": {
                    "CreationDate": creation_date,
                    "Size": size,
                    "ArchiveDescription": archive_description,
                    "ArchiveId": archive_id,
                    "SHA256TreeHash": sha256_tree_hash,
                    "Filename": archive_description,
                },
                "workflow_run": WORKFLOW_RUN,
                "tier": "Bulk",
                "vault_name": vault_name,
                "s3_storage_class": "GLACIER",
            }
        )
    return initiate_retrieval_list


def test_initiate_retrieval_batch(
    s3_client: S3Client, lambda_client: LambdaClient
) -> None:
    vault_name = "test_small_vault"
    sns_topic = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]

    mock_inventory = get_mock_inventory(vault_name)

    initiate_retrievals = build_initiate_retrieval_input(mock_inventory, vault_name)
    invoke_initiate_archive_retrieval_lambda(
        lambda_client, sns_topic, initiate_retrievals
    )

    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])
    waiter = s3_client.get_waiter("object_exists")
    try:
        for archive in initiate_retrievals:
            waiter.wait(
                Bucket=os.environ[OutputKeys.OUTPUT_BUCKET_NAME],
                Key=f"{WORKFLOW_RUN}/{archive['item']['Filename']}",
            )
    except WaiterError:
        pytest.fail(f"S3 object {archive['item']['Filename']} does not exist")

    time.sleep(4)  # Wait for Archive metadata to be updated
    for archive in initiate_retrievals:
        ddb_metadata = ddb_accessor.get_item(
            key=GlacierTransferMetadataRead(
                workflow_run=WORKFLOW_RUN,
                glacier_object_id=archive["item"]["ArchiveId"],
            ).key
        )
        if ddb_metadata is None:
            pytest.fail(f"Metadata not found for {archive['item']['Filename']}")
        metadata: GlacierTransferMetadata = GlacierTransferMetadata.parse(ddb_metadata)
        assert metadata.retrieve_status.endswith("/downloaded")
