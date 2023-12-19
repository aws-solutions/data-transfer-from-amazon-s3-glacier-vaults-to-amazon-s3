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
from tests.integration.infrastructure.util import ddb_util, s3_util

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object


VAULT_NAME = "test_small_vault_archive_range"
WORKFLOW_RUN = "workflow_run_orchestrator"


@pytest.fixture
def default_input() -> str:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    return json.dumps(
        dict(
            provided_inventory="NO",
            vault_name=VAULT_NAME,
            description="This is a test",
            sns_topic=topic_arn,
            workflow_run=WORKFLOW_RUN,
            chunk_size=2**20,
            archive_chunk_size=2**20,
            name_override_presigned_url=None,
            migration_type="LAUNCH",
            tier="Bulk",
            s3_storage_class="STANDARD",
        )
    )


def empty_ddb_tables() -> None:
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME], "pk", "sk"
    )
    ddb_util.delete_all_table_items(
        os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME], "job_id"
    )
    ddb_util.delete_all_table_items(os.environ[OutputKeys.METRIC_TABLE_NAME], "pk")


@pytest.fixture(autouse=True, scope="module")
def setup() -> Any:
    empty_ddb_tables()
    s3_util.delete_all_inventory_files_from_s3(prefix=WORKFLOW_RUN)
    s3_util.delete_archives_from_s3(prefix=WORKFLOW_RUN)


def test_state_machine_start_execution(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.ORCHESTRATOR_STATE_MACHINE_ARN],
        input=default_input,
    )

    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
