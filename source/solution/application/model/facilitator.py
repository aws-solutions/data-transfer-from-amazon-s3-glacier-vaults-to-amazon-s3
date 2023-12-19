"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from dataclasses import dataclass
from typing import Any, TypedDict

from solution.application.model.base import Model, UpdateExpressionParameters


@dataclass
class JobCompletionEvent(Model):
    class StatusCode:
        SUCCEEDED = "Succeeded"
        FAILED = "Failed"
        IN_PROGRESS = "InProgress"

    raw_message: str
    job_id: str = Model.field(["JobId"])
    completed: bool = Model.field(["Completed"])
    status_code: str = Model.field(["StatusCode"])
    completion_date: str | None = Model.field(["CompletionDate"], optional=True)


@dataclass
class AsyncRecord(Model):
    job_id: str = Model.field(["job_id", "S"])
    job_result: str | None = Model.field(["job_result", "S"], optional=True)
    finish_timestamp: str | None = Model.field(["finish_timestamp", "S"], optional=True)
    task_token: str | None = Model.field(["task_token", "S"], optional=True)
    start_timestamp: str | None = Model.field(["start_timestamp", "S"], optional=True)
    workflow_run: str | None = Model.field(["workflow_run", "S"], optional=True)

    @property
    def key(self) -> dict[str, Any]:
        return {"job_id": {"S": self.job_id}}

    @property
    def inventory_job_completion_update_parameters(self) -> UpdateExpressionParameters:
        return UpdateExpressionParameters(
            Key=self.key,
            UpdateExpression="SET finish_timestamp = :ft, job_result = :jr",
            ExpressionAttributeValues={
                ":jr": {"S": self.job_result},
                ":ft": {"S": self.finish_timestamp},
            },
        )

    @property
    def archive_initiate_job_update_parameters(self) -> UpdateExpressionParameters:
        return UpdateExpressionParameters(
            Key=self.key,
            UpdateExpression="SET start_timestamp = :st, workflow_run = :wr",
            ExpressionAttributeValues={
                ":st": {"S": self.start_timestamp},
                ":wr": {"S": self.workflow_run},
            },
        )
