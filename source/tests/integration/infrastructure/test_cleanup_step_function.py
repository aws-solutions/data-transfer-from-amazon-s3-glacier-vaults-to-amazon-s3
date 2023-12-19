"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from tests.integration.infrastructure.util import sfn_util

from solution.application.glacier_s3_transfer.multipart_cleanup import MultipartCleanup
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_stepfunctions import SFNClient
else:
    S3Client = object
    SFNClient = object


@pytest.fixture(scope="module")
def s3_client() -> S3Client:
    client: S3Client = boto3.client("s3")
    return client


@pytest.fixture(scope="module")
def sfn_client() -> SFNClient:
    client: SFNClient = boto3.client("stepfunctions")
    return client


@pytest.fixture(scope="module")
def bucket() -> str:
    return os.environ[OutputKeys.OUTPUT_BUCKET_NAME]


@pytest.fixture(scope="module")
def workflow_run() -> str:
    return "cleanup_workflow_run"


def test_start_cloudwatch_dashboard_sfn(
    sfn_client: SFNClient, workflow_run: str, s3_client: S3Client
) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.CLEANUP_STATE_MACHINE_ARN],
        input=json.dumps({"workflow_run": workflow_run}),
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=150)
    exec_history = sfn_client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )
    cw_output = [
        event["stateExitedEventDetails"]["output"]
        for event in exec_history["events"]
        if "TaskStateExited" in event["type"]
        and event["stateExitedEventDetails"]["name"] == "CloudwatchDashboardUpdateState"
    ][0]
    cw_output_json = json.loads(cw_output)
    cw_output_error_cause = json.loads(cw_output_json["errors"]["Cause"])
    cw_exec_arn = cw_output_error_cause["ExecutionArn"]
    cw_sfn_exec = sfn_client.describe_execution(executionArn=cw_exec_arn)
    assert json.loads(cw_sfn_exec["input"])["workflow_run"] == workflow_run


def test_post_workflow_dashboard_update(
    sfn_client: SFNClient, workflow_run: str, s3_client: S3Client
) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.CLEANUP_STATE_MACHINE_ARN],
        input=json.dumps({"workflow_run": workflow_run}),
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=150)
    obj = s3_client.get_object(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Key=f"{workflow_run}/failed_archives/failed_archives.csv",
    )
    assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_multipart_upload_cleanup(
    s3_client: S3Client, bucket: str, sfn_client: SFNClient, workflow_run: str
) -> None:
    """Validate that open multipart uploads are deleted. Ensure that only multipart uploads with the correct key prefix are deleted"""

    cleaner_setup = MultipartCleanup(workflow_run, bucket)
    cleaner_setup.cleanup()
    different_prefix = "different_prefix"
    cleaner_setup2 = MultipartCleanup(different_prefix, bucket)
    cleaner_setup2.cleanup()

    _create_multipart_uploads(
        s3_client=s3_client,
        bucket=bucket,
        num_uploads=15,
        prefix=workflow_run,
    )

    _create_multipart_uploads(
        s3_client=s3_client,
        bucket=bucket,
        num_uploads=5,
        prefix=different_prefix,
    )

    _validate_deleted_multipart_uploads(
        sfn_client=sfn_client, prefix=workflow_run, num_deleted=15
    )

    _validate_deleted_multipart_uploads(
        sfn_client=sfn_client, prefix=different_prefix, num_deleted=5
    )


def _create_multipart_uploads(
    s3_client: S3Client,
    bucket: str,
    num_uploads: int,
    prefix: str,
) -> None:
    for i in range(num_uploads):
        s3_client.create_multipart_upload(Bucket=bucket, Key=f"{prefix}/{i}")


def _validate_deleted_multipart_uploads(
    sfn_client: SFNClient, prefix: str, num_deleted: int
) -> None:
    executionArn = _delete_multipart_uploads(
        sfn_client=sfn_client,
        prefix=prefix,
    )

    assert (
        json.loads(sfn_client.describe_execution(executionArn=executionArn)["output"])[
            "multipart_upload_cleanup"
        ]
        == num_deleted
    )


def _delete_multipart_uploads(sfn_client: SFNClient, prefix: str) -> str:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[OutputKeys.CLEANUP_STATE_MACHINE_ARN],
        input=json.dumps({"workflow_run": prefix}),
    )
    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=180)
    return response["executionArn"]
