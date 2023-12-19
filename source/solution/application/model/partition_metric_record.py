"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

from solution.application.model.base import Model, UpdateExpressionParameters


@dataclass
class PartitionMetricRecord(Model):
    pk: str = Model.field(["pk", "S"])
    archives_count: int | None = Model.field(
        ["archives_count", "N"], optional=True, marshal_as=str
    )
    archives_size: int | None = Model.field(
        ["archives_size", "N"], optional=True, marshal_as=str
    )
    start_time: str | None = Model.field(["start_time", "S"], optional=True)

    @property
    def key(self) -> Dict[str, Any]:
        return {"pk": {"S": self.pk}}

    @staticmethod
    def partition_key(workflow_id: str) -> str:
        return f"{workflow_id}|PARTITION"

    def update_parameters(self) -> UpdateExpressionParameters:
        return UpdateExpressionParameters(
            Key=self.key,
            UpdateExpression="ADD archives_count :ac, archives_size :as",
            ExpressionAttributeValues={
                ":ac": self.marshal()["archives_count"],
                ":as": self.marshal()["archives_size"],
            },
        )

    @staticmethod
    def format_partition_keys_for_step_function(workflow_id_path: str) -> str:
        return f"States.Format('{{}}|PARTITION', {workflow_id_path})"
