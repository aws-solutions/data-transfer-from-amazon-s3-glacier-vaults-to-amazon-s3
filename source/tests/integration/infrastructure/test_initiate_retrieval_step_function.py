"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from tests.integration.infrastructure.util import ddb_util, s3_util, sfn_util

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.metric_record import MetricRecord
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object
    DynamoDBClient = object

WORKFLOW_RUN = "workflow_run_initiate_retrieval"
VAULT_NAME = "test_small_vault"
PARTITION_ID = "PartitionId=0000000000"


@pytest.fixture(scope="module")
def default_input() -> str:
    return json.dumps(
        {
            "workflow_run": WORKFLOW_RUN,
            "vault_name": VAULT_NAME,
            "description": "test",
            "tier": "Bulk",
            "prefix": f"{WORKFLOW_RUN}/sorted_inventory/{PARTITION_ID}/",
            "s3_storage_class": "GLACIER",
        }
    )


@pytest.fixture(scope="module")
def sfn_execution_arn(default_input: str, sfn_client: SFNClient) -> Any:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )

    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=60)
    return response["executionArn"]


@pytest.fixture(scope="module")
def sf_history_output(sfn_client: SFNClient, sfn_execution_arn: str) -> Any:
    return sfn_client.get_execution_history(
        executionArn=sfn_execution_arn, maxResults=1000
    )


@pytest.fixture(autouse=True, scope="module")
def setup(ddb_client: DynamoDBClient) -> Any:
    ddb_accessor = DynamoDBAccessor(
        os.environ[OutputKeys.METRIC_TABLE_NAME], client=ddb_client
    )
    ddb_accessor.insert_item(MetricRecord(pk=WORKFLOW_RUN).marshal())

    DAILY_QUOTA = 80 * 2**40
    ddb_accessor = DynamoDBAccessor(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], client=ddb_client
    )
    ddb_accessor.insert_item(
        {
            "pk": {"S": "workflow_run"},
            "sk": {"S": "meta"},
            "workflow_id": {"S": WORKFLOW_RUN},
            "daily_quota": {"N": str(DAILY_QUOTA)},
        }
    )

    file_name_prefix = f"{WORKFLOW_RUN}/sorted_inventory/{PARTITION_ID}/test_inventory"
    s3_util.put_inventory_file_in_s3(file_name_prefix, VAULT_NAME)
    yield
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    ddb_util.delete_all_table_items(os.environ[OutputKeys.METRIC_TABLE_NAME], "pk")
    s3_util.delete_all_inventory_files_from_s3(prefix=WORKFLOW_RUN)


def test_state_machine_start_execution(sfn_client: SFNClient) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN],
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None


def test_initate_retrieval_ddb_updated(
    sf_history_output: Any, sfn_client: SFNClient, ddb_client: DynamoDBClient
) -> None:
    ddb_accessor = DynamoDBAccessor(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], client=ddb_client
    )

    csv_str = ddb_util.get_inventory_csv(VAULT_NAME)
    body_dict = ddb_util.convert_csv_to_dict(csv_str)

    for _, v in body_dict.items():
        archive_id = v["ArchiveId"]
        archive_job_id = MOCK_DATA[VAULT_NAME]["initiate-job"][f"archive-retrieval:{archive_id}"]["jobId"]  # type: ignore
        ddb_metadata = ddb_accessor.get_item(
            GlacierTransferMetadataRead(
                workflow_run=WORKFLOW_RUN, glacier_object_id=archive_id
            ).key
        )
        assert ddb_metadata is not None
        metadata = GlacierTransferMetadata.parse(ddb_metadata)

        assert archive_job_id == metadata.job_id
        assert metadata.retrieve_status is not None
