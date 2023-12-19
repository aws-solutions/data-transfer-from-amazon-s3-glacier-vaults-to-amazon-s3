"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from aws_cdk.aws_stepfunctions import JsonPath
from aws_cdk.aws_stepfunctions_tasks import DynamoAttributeValue

from solution.application.model.base import UpdateExpressionParameters
from solution.application.model.metric_record import (
    MetricRecord,
    RetrieveStatusMetricData,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import CreateTableOutputTypeDef
else:
    CreateTableOutputTypeDef = object
    DynamoDBClient = object


def test_metric_values_update_expression_atomic_counter() -> None:
    metric_data = RetrieveStatusMetricData(
        count="100", size="10240", retrieval_status="requested"
    )
    update_expression = metric_data.update_expression_atomic_counter()
    expected_expression = "ADD count_requested :update_count_requested, size_requested :update_size_requested"
    assert update_expression == expected_expression


def test_metric_values_update_attribute_values_sfn() -> None:
    metric_data = RetrieveStatusMetricData(
        count="100", size="10240", retrieval_status="requested"
    )
    update_attribute_values = metric_data.expression_attribute_values(
        *[JsonPath, DynamoAttributeValue]
    )
    assert update_attribute_values[":update_count_requested"].to_object()["N"] == "100"
    assert update_attribute_values[":update_size_requested"].to_object()["N"] == "10240"


def test_metric_values_update_attribute_values() -> None:
    metric_data = RetrieveStatusMetricData(
        count="1", size="100", retrieval_status="requested"
    )
    update_attribute_values = metric_data.expression_attribute_values()

    assert update_attribute_values[":update_count_requested"]["N"] == "1"
    assert update_attribute_values[":update_size_requested"]["N"] == "100"


def test_update_parameters(
    metric_table_mock: CreateTableOutputTypeDef, dynamodb_client: DynamoDBClient
) -> None:
    dynamodb_client.put_item(
        TableName=os.environ["MetricTableName"], Item={"pk": {"S": "metric_record"}}
    )
    for item in [
        MetricRecord(pk="metric_record", count_staged=10, size_staged=100),
        MetricRecord(pk="metric_record", count_staged=1, size_staged=10),
    ]:
        update_expr_params: UpdateExpressionParameters = item.update_parameters(
            "staged"
        )
        dynamodb_client.update_item(
            TableName=os.environ["MetricTableName"], **update_expr_params
        )

    response = dynamodb_client.get_item(
        TableName=os.environ["MetricTableName"], Key={"pk": {"S": "metric_record"}}
    )
    assert response["Item"]["count_staged"]["N"] == "11"
    assert response["Item"]["size_staged"]["N"] == "110"
