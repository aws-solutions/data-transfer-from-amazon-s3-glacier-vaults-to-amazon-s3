"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import time
from typing import TYPE_CHECKING, Any, Dict

import boto3
import pytest
from tests.integration.infrastructure.util import ddb_util, s3_util, sfn_util, ssm_util

from solution.infrastructure.output_keys import OutputKeys

VAULT_NAME = "test_medium_vault"
WORKFLOW_RUN = "workflow_run_ssm"


if TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
else:
    SSMClient = object


@pytest.fixture(scope="module")
def default_input() -> Dict[str, Any]:
    return {
        "ProvidedInventory": [
            "NO",
        ],
        "VaultName": [
            "test_medium_vault",
        ],
        "AcknowledgeAdditionalCostForCrossRegionTransfer": [
            "YES",
        ],
    }


@pytest.fixture(autouse=True, scope="module")
def setup() -> Any:
    yield
    sfn_util.stop_execution_running_sfn(
        [
            os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
            os.environ[OutputKeys.ORCHESTRATOR_STATE_MACHINE_ARN],
        ]
    )
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME], "job_id"
    )
    s3_util.delete_all_inventory_files_from_s3(prefix=WORKFLOW_RUN)


def test_launch_automation_runbook_execute_with_provided_inventory_success(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    default_input["ProvidedInventory"] = ["YES"]
    default_input["WorkflowRun"] = [WORKFLOW_RUN]
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Success"
    default_input.pop("WorkflowRun")
    default_input["ProvidedInventory"] = ["NO"]


def test_launch_automation_runbook_execute_with_provided_inventory_failure(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    default_input["ProvidedInventory"] = ["YES"]
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Failed"
    default_input["ProvidedInventory"] = ["NO"]


def test_launch_automation_runbook_execute_input_with_desc_success(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    default_input["Description"] = ["A random description provided by user -*&5%"]
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Success"


def test_launch_automation_runbook_execute_no_provided_inventory_success(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Success"


def test_launch_automation_runbook_execute_custom_workflow_run(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    default_input["WorkflowRun"] = [WORKFLOW_RUN]
    input_no_inventory = default_input
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=input_no_inventory,
    )
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Success"


def test_resume_automation_runbook_execute_input_with_workflow_run_success(
    ssm_client: SSMClient, default_input: Dict[str, Any]
) -> None:
    default_input["WorkflowRun"] = [f"{WORKFLOW_RUN}_RESUME"]
    ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )
    # Trying to recreate a real life scenario where the resume tests are continuing from a previous launch
    # sleep is to make sure ddb is updated with the workflow run
    time.sleep(2)
    default_input.pop("VaultName")
    response = ssm_client.start_automation_execution(
        DocumentName=os.environ[OutputKeys.SSM_RESUME_AUTOMATION_RUNBOOK_NAME],
        Parameters=default_input,
    )

    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    status = ssm_util.poll_execution_status(response["AutomationExecutionId"])
    assert status == "Success"
