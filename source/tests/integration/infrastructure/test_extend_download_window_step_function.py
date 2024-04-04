"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List

import boto3
import pytest
from tests.integration.infrastructure.util import ddb_util, sfn_util

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_stepfunctions import SFNClient
else:
    DynamoDBClient = object
    SFNClient = object

WORKFLOW_RUN = "workflow_run_extend_download_window"
VAULT_NAME = "test_small_vault"


@pytest.fixture(scope="module")
def default_input() -> str:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    return json.dumps(
        dict(
            vault_name=VAULT_NAME,
            sns_topic=topic_arn,
            workflow_run=WORKFLOW_RUN,
            check_archive_status=True,
            tier="Bulk",
            s3_storage_class="GLACIER",
        )
    )


@pytest.fixture(scope="module")
def archives_list() -> List[Dict[str, Any]]:
    archives = []
    for key in MOCK_DATA[VAULT_NAME]["initiate-job"]:  # type: ignore
        if key.startswith("archive-retrieval:"):
            archive_id = key.split(":")[1]
            archives.append(
                {
                    "archive_id": archive_id,
                    "download_window": "2023-05-09T15:52:27.757Z",
                }
            )
    archives[-1]["download_window"] = datetime.utcnow().isoformat() + "Z"
    return archives


@pytest.fixture(scope="module")
def archives_need_extension(
    archives_list: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    return archives_list[:-1]


@pytest.fixture(scope="module")
def archives_do_not_need_extension(
    archives_list: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    return [archives_list[-1]]


@pytest.fixture(scope="module")
def sfn_execution_arn(
    default_input: str,
    archives_list: List[Dict[str, Any]],
    sfn_client: SFNClient,
    ddb_client: DynamoDBClient,
) -> str:
    for index, archive in enumerate(archives_list):
        archive_id = archive["archive_id"]
        download_window = archive["download_window"]
        meta_model = GlacierTransferMetadataRead(
            workflow_run=WORKFLOW_RUN, glacier_object_id=archive_id
        )
        ddb_client.update_item(
            TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
            Key=meta_model.key,
            UpdateExpression="SET archive_id = :ai, retrieve_status = :rs, download_window = :dw, vault_name = :vn, archive_creation_date = :cd",
            ExpressionAttributeValues={
                ":ai": {"S": archive_id},
                ":rs": {
                    "S": f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.STAGED}"
                },
                ":dw": {"S": download_window},
                ":vn": {"S": VAULT_NAME},
                ":cd": {"S": f"2023-01-0{index}T01:01:01.001Z"},
            },
        )

    # Add 10 sec delay to allow the data to propagate to the index (GSI replica is eventually consistent)
    time.sleep(10)

    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.EXTEND_DOWNLOAD_WINDOW_STATE_MACHINE_ARN],
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
def setup() -> Any:
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    yield
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME], "job_id"
    )


def test_state_machine_start_execution(sfn_client: SFNClient) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.EXTEND_DOWNLOAD_WINDOW_STATE_MACHINE_ARN]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None


def test_state_machine_archives_needing_extension_lambda(
    sf_history_output: Any,
) -> None:
    events = [
        event
        for event in sf_history_output["events"]
        if "LambdaFunctionSucceeded" in event["type"]
    ]

    event_details = events[0]["lambdaFunctionSucceededEventDetails"]

    assert (
        f"{WORKFLOW_RUN}/download_window_extension/"
        in json.loads(event_details["output"])["s3_key"]
    )


def test_state_machine_distributed_map(sf_history_output: Any) -> None:
    events = [
        event
        for event in sf_history_output["events"]
        if "MapRunSucceeded" in event["type"]
    ]

    if not events:
        raise AssertionError(
            "Extend download window distributed map failed to run successfully."
        )


def test_extend_retrieval_status_updated(
    archives_need_extension: List[Dict[str, Any]],
    sf_history_output: Any,
    sfn_client: SFNClient,
    ddb_client: DynamoDBClient,
) -> None:
    ddb_accessor = DynamoDBAccessor(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], client=ddb_client
    )

    for archive in archives_need_extension:
        archive_id = archive["archive_id"]
        ddb_metadata = ddb_accessor.get_item(
            GlacierTransferMetadataRead(
                workflow_run=WORKFLOW_RUN, glacier_object_id=archive_id
            ).key
        )
        assert ddb_metadata is not None
        assert (
            f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.EXTENDED}"
            == ddb_metadata["retrieve_status"]["S"]
        )


def test_not_needing_extension_retrieval_status_not_updated(
    archives_do_not_need_extension: List[Dict[str, Any]],
    sf_history_output: Any,
    sfn_client: SFNClient,
    ddb_client: DynamoDBClient,
) -> None:
    ddb_accessor = DynamoDBAccessor(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], client=ddb_client
    )

    for archive in archives_do_not_need_extension:
        archive_id = archive["archive_id"]
        ddb_metadata = ddb_accessor.get_item(
            GlacierTransferMetadataRead(
                workflow_run=WORKFLOW_RUN, glacier_object_id=archive_id
            ).key
        )
        assert ddb_metadata is not None
        assert (
            f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.STAGED}"
            == ddb_metadata["retrieve_status"]["S"]
        )
