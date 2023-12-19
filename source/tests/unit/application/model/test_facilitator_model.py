"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from solution.application.model.facilitator import AsyncRecord


def test_get_key() -> None:
    record = AsyncRecord(job_id="job-123")
    assert "job_id" in record.key
    assert record.key["job_id"]["S"] == "job-123"


def test_update_parameters() -> None:
    record = AsyncRecord(
        job_id="123",
        job_result="foo",
        finish_timestamp="bar",
        workflow_run="workflow_test",
    )
    assert record.inventory_job_completion_update_parameters["Key"] == record.key
    assert (
        record.inventory_job_completion_update_parameters["UpdateExpression"]
        is not None
    )
    for value in record.inventory_job_completion_update_parameters[
        "ExpressionAttributeValues"
    ].values():
        assert "S" in value
        assert value["S"] in ("foo", "bar", "workflow_test")
