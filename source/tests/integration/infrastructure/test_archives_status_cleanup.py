"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING

import boto3
import pytest

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object

WORKFLOW_RUN = "workflow_run_archives_status_cleanup"


@pytest.fixture(scope="module")
def default_input() -> str:
    return json.dumps({"workflow_run": WORKFLOW_RUN})


def test_state_machine_start_execution(
    default_input: str, sfn_client: SFNClient
) -> None:
    response = sfn_client.start_execution(
        stateMachineArn=os.environ[
            OutputKeys.ARCHIVES_STATUS_CLEANUP_STATE_MACHINE_ARN
        ],
        input=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
