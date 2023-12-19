"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
import time
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from tests.integration.infrastructure.util import ddb_util, s3_util, sfn_util

from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadataRead,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.model.glacier_transfer_part_model import (
    GlacierTransferPartRead,
)
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object
    DynamoDBClient = object
    S3Client = object

VAULT_NAME = "test_vault_chunk_generation_vault"
WORKFLOW_RUN = "workflow_run_inventory_retrieval"


@pytest.fixture(autouse=True, scope="module")
def set_up() -> Any:
    yield
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME], "job_id"
    )
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    ddb_util.delete_all_table_items(os.environ[OutputKeys.METRIC_TABLE_NAME], "pk")
    s3_util.delete_all_inventory_files_from_s3(prefix=WORKFLOW_RUN)


@pytest.fixture(scope="module")
def default_input() -> str:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    return json.dumps(
        dict(
            provided_inventory="NO",
            vault_name=VAULT_NAME,
            description="This is a test",
            sns_topic=topic_arn,
            workflow_run=WORKFLOW_RUN,
            upload_id="test_upload_123",
            chunk_size=MOCK_DATA[VAULT_NAME]["inventory-metadata"]["chunkSize"],  # type: ignore
            name_override_presigned_url=None,
            tier="Bulk",
            migration_type="LAUNCH",
            s3_storage_class="GLACIER",
        )
    )


@pytest.fixture(scope="module")
def sfn_client() -> Any:
    return boto3.client("stepfunctions")


@pytest.fixture(scope="module")
def sf_history_output_no_inventory(default_input: str, sfn_client: SFNClient) -> Any:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=180)
    return sfn_client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )


@pytest.fixture(scope="module")
def sf_history_output_with_inventory(sfn_client: SFNClient) -> Any:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input='{"provided_inventory": "YES", "migration_type": "LAUNCH", "workflow_run": "workflow_run_inventory_retrieval", "name_override_presigned_url": null}',
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=150)
    return sfn_client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )


def test_state_machine_start_execution() -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None


def test_state_machine_start_execution_provided_inventory_yes(
    sf_history_output_with_inventory: Any,
) -> None:
    assert "retrieveInventory" not in sf_history_output_with_inventory


def test_state_machine_start_execution_provided_inventory_no(
    sf_history_output_no_inventory: Any,
) -> None:
    event_details = [
        event["stateEnteredEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if "stateEnteredEventDetails" in event
    ]
    for detail in event_details:
        if detail["name"] == "RetrieveInventoryInitiateJob":
            break
    else:
        pytest.fail(
            "The branching logic of the choice state of Get Inventory workflow did not work as expected when the inventory was not provided."
        )


def test_initiate_job_task_succeeded(sf_history_output_no_inventory: Any) -> None:
    event_details = [
        event["stateExitedEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if "stateExitedEventDetails" in event
    ]

    for detail in event_details:
        if detail["name"] == "MockGlacierInitiateJobTask":
            state_output = detail["output"]
            assert "JobId" in state_output and "Location" in state_output
            break


def test_multipart_upload_create_task_succeeded(
    sf_history_output_no_inventory: Any,
) -> None:
    event_details = [
        event["stateExitedEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if "stateExitedEventDetails" in event
    ]

    for detail in event_details:
        if detail["name"] == "MockS3CreateMultipartUploadTask":
            state_output = detail["output"]
            assert "UploadId" in state_output
            break


def test_dynamo_db_put_multipart_upload_behavior() -> None:
    meta_read = GlacierTransferMetadataRead(
        workflow_run=WORKFLOW_RUN, glacier_object_id=VAULT_NAME
    )
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    db_client: DynamoDBClient = boto3.client("dynamodb")

    query_response = db_client.get_item(
        TableName=table_name,
        Key=meta_read.key,
    )
    assert query_response["Item"]["upload_id"]["S"] is not None


def test_dynamo_db_put_item_async_behavior(sf_history_output_no_inventory: Any) -> None:
    event_details = [
        event["taskSucceededEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if "taskSucceededEventDetails" in event
    ]

    for detail in event_details:
        if detail["resourceType"] == "aws-sdk:dynamodb":
            state_output = json.loads(detail["output"])
            job_id = state_output["JobId"]

            table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
            db_client: DynamoDBClient = boto3.client("dynamodb")
            key = {"job_id": {"S": job_id}}
            item = db_client.get_item(TableName=table_name, Key=key)
            assert (
                item["Item"]["task_token"] is not None
                and item["Item"]["finish_timestamp"] is not None
            )
            break


def test_state_machine_distributed_map(sf_history_output_no_inventory: Any) -> None:
    events = [
        event
        for event in sf_history_output_no_inventory["events"]
        if "MapRunSucceeded" in event["type"]
    ]

    if not events:
        raise AssertionError(
            "Inventory retrieval distributed map failed to run successfully."
        )


def test_initiate_job_task_succeeded_for_glue_job_update(
    sf_history_output_with_inventory: Any,
    sf_history_output_no_inventory: Any,
) -> None:
    for history_output in (
        sf_history_output_with_inventory["events"],
        sf_history_output_no_inventory["events"],
    ):
        for event in history_output:
            detail = event.get("taskSucceededEventDetails")
            if detail and detail["resourceType"] == "aws-sdk:glue":
                assert "GlueOrderingJob" in json.loads(detail["output"])["JobName"]


def test_initiate_job_task_succeeded_for_glue_job_run(
    sf_history_output_with_inventory: Any, sf_history_output_no_inventory: Any
) -> None:
    for history_output in (
        sf_history_output_with_inventory["events"],
        sf_history_output_no_inventory["events"],
    ):
        for event in history_output:
            detail = event.get("taskSucceededEventDetails")
            if detail and detail["resourceType"] == "glue":
                job_info = json.loads(detail["output"])
                assert "GlueOrderingJob" in job_info["JobName"]
                assert job_info["JobRunState"] == "SUCCEEDED"


def test_inventory_retrieval_writes_parts_to_dynamo(
    sf_history_output_no_inventory: Any,
) -> None:
    event_details = [
        event["stateExitedEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if event["type"] == "MapStateExited"
    ]
    assert len(event_details) == 1
    detail = event_details[0]

    state_output = json.loads(detail["output"])
    state_output_part_mapping = {
        str(item["PartNumber"]): item for item in state_output["upload_part_result"]
    }
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    db_client: DynamoDBClient = boto3.client("dynamodb")
    part_model = GlacierTransferPartRead(
        workflow_run=WORKFLOW_RUN, glacier_object_id=VAULT_NAME, part_number=1
    )
    part1 = db_client.get_item(TableName=table_name, Key=part_model.key)["Item"]
    assert (
        part1["checksum_sha_256"]["S"]
        == state_output_part_mapping["1"]["ChecksumSHA256"]
    )
    assert part1["e_tag"]["S"] == state_output_part_mapping["1"]["ETag"]


def test_multipart_upload_completes_successfully(
    sf_history_output_no_inventory: Any,
) -> None:
    event_details = [
        event["stateExitedEventDetails"]
        for event in sf_history_output_no_inventory["events"]
        if "stateExitedEventDetails" in event
    ]

    for detail in event_details:
        if detail["name"] == "ValidateInventoryLambdaTask":
            state_output = json.loads(detail["output"])
            assert state_output["HTTPStatusCode"] == 200

            break


def test_state_machine_start_execution_inventory_already_downloaded(
    default_input: str, sfn_client: SFNClient
) -> Any:
    db_client: DynamoDBClient = boto3.client("dynamodb")
    db_client.update_item(
        TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
        Key=GlacierTransferMetadataRead(
            workflow_run=WORKFLOW_RUN, glacier_object_id=VAULT_NAME
        ).key,
        ExpressionAttributeValues={
            ":rs": {"S": f"{WORKFLOW_RUN}/{GlacierTransferModel.StatusCode.DOWNLOADED}"}
        },
        UpdateExpression="SET retrieve_status = :rs",
    )

    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=180)
    execution_history = sfn_client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )
    event_details = [
        event["stateEnteredEventDetails"]
        for event in execution_history["events"]
        if "stateEnteredEventDetails" in event
    ]
    for detail in event_details:
        if detail["name"] == "RetrieveInventoryInitiateJob":
            pytest.fail(
                "The branching logic of InventoryAlreadyDownloaded choice state did not work as expected when the inventory is already downloaded."
            )
            break


def test_state_machine_resuming_workflow(
    default_input: str, sfn_client: SFNClient
) -> None:
    # adding a dummy name to sorted_inventory folder to later make sure it is deleted
    file_name_prefix = f"{WORKFLOW_RUN}_RESUME/sorted_inventory/test_inventory"
    s3_util.put_inventory_file_in_s3(file_name_prefix, VAULT_NAME)
    s3_client: S3Client = boto3.client("s3")
    input_json = json.loads(default_input)
    input_json["migration_type"] = "RESUME"
    workflow_run = f"{WORKFLOW_RUN}_RESUME"
    input_json["workflow_run"] = workflow_run
    input_resume = json.dumps(input_json)
    db_client: DynamoDBClient = boto3.client("dynamodb")
    db_client.put_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Item={
            "pk": {"S": workflow_run},
            "count_total": {"N": "100"},
            "size_total": {"N": "2000"},
            "count_staged": {"N": "80"},
            "size_staged": {"N": "1600"},
            "count_requested": {"N": "80"},
            "size_requested": {"N": "1600"},
            "count_downloaded": {"N": "50"},
            "size_downloaded": {"N": "1000"},
        },
    )
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=input_resume,
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=420)
    metric_item = db_client.get_item(
        TableName=os.environ[OutputKeys.METRIC_TABLE_NAME],
        Key={"pk": {"S": workflow_run}},
    )
    # Making sure the sorted_invetory is updated
    time.sleep(10)

    sorted_inventory_dir = s3_client.list_objects(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Prefix=f"{WORKFLOW_RUN}_RESUME/sorted_inventory",
    )
    # Making sure to purge the sorted_inventory folder
    assert "test_inventory" not in sorted_inventory_dir["Contents"][0]["Key"]
    assert "PartitionId" in sorted_inventory_dir["Contents"][0]["Key"]
    # Making sure the numbers here are correct on resuming the workflow
    assert metric_item["Item"]["count_requested"]["N"] == "50"
    assert metric_item["Item"]["size_requested"]["N"] == "1000"
    assert metric_item["Item"]["count_staged"]["N"] == "50"
    assert metric_item["Item"]["size_staged"]["N"] == "1000"
