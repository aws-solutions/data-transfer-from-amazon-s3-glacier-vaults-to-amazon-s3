"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

from solution.application.model.base import Model, UpdateExpressionParameters


class RetrieveStatusMetricData:
    def __init__(self, count: str, size: str, retrieval_status: str):
        self.count = count
        self.size = size
        self.status = retrieval_status

    @property
    def to_dict(self) -> dict[str, Any]:
        return {
            self.status: {
                "count": self.count,
                "size": self.size,
            }
        }

    def expression_attribute_values(self, *sfn_assets: Any) -> Dict[str, Any]:
        """
        Function attempts to create both versions of expression attribute values dictionary
        If UpdateItem is called from a Step Function, sfn_assets should be provided with JsonPath and DynamoAttributeValue
        If UpdateItem is called from a Lambda, sfn_assets should be empty
        """
        expr_attr_vals = {}
        for k, v in self.to_dict[self.status].items():
            if not sfn_assets:
                expr_attr_vals[f":update_{k}_{self.status}"] = {"N": v}
            else:
                json_path, dynamo_attr_val = sfn_assets
                expr_attr_vals[f":update_{k}_{self.status}"] = (
                    dynamo_attr_val.from_number(int(v))
                    if "$." not in v
                    else dynamo_attr_val.number_from_string(json_path.string_at(v))
                )

        return expr_attr_vals

    def update_expression_atomic_counter(self) -> str:
        expressions_list = []
        for k in self.to_dict[self.status].keys():
            expressions_list.append(f"{k}_{self.status} :update_{k}_{self.status}")
        return "ADD " + ", ".join(expressions_list)


@dataclass
class MetricRecord(Model):
    pk: str = Model.field(["pk", "S"])
    count_total: int | None = Model.field(
        ["count_total", "N"], optional=True, marshal_as=str
    )
    size_total: int | None = Model.field(
        ["size_total", "N"], optional=True, marshal_as=str
    )
    count_skipped: int | None = Model.field(
        ["count_skipped", "N"], optional=True, marshal_as=str
    )
    size_skipped: int | None = Model.field(
        ["size_skipped", "N"], optional=True, marshal_as=str
    )
    count_staged: int | None = Model.field(
        ["count_staged", "N"], optional=True, marshal_as=str
    )
    size_staged: int | None = Model.field(
        ["size_staged", "N"], optional=True, marshal_as=str
    )
    count_requested: int | None = Model.field(
        ["count_requested", "N"], optional=True, marshal_as=str
    )
    size_requested: int | None = Model.field(
        ["size_requested", "N"], optional=True, marshal_as=str
    )
    count_downloaded: int | None = Model.field(
        ["count_downloaded", "N"], optional=True, marshal_as=str
    )
    size_downloaded: int | None = Model.field(
        ["size_downloaded", "N"], optional=True, marshal_as=str
    )
    count_failed: int | None = Model.field(
        ["count_failed", "N"], optional=True, marshal_as=str
    )
    size_failed: int | None = Model.field(
        ["size_failed", "N"], optional=True, marshal_as=str
    )

    @property
    def key(self) -> Dict[str, Any]:
        return {"pk": {"S": self.pk}}

    @staticmethod
    def partition_key(workflow_id: str) -> str:
        return workflow_id

    def update_parameters(self, retrieve_status: str) -> UpdateExpressionParameters:
        marshaled = self.marshal()
        metric_data = RetrieveStatusMetricData(
            marshaled[f"count_{retrieve_status}"]["N"],
            marshaled[f"size_{retrieve_status}"]["N"],
            retrieve_status,
        )

        return UpdateExpressionParameters(
            Key=self.key,
            UpdateExpression=metric_data.update_expression_atomic_counter(),
            ExpressionAttributeValues=metric_data.expression_attribute_values(),
        )
