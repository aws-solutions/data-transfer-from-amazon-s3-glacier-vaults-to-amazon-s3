"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import os
from typing import TYPE_CHECKING

import boto3

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object


def test_lambda_invoke(lambda_client: LambdaClient) -> None:
    lambda_name = os.environ[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN]

    test_event = '{"TotalSize": 100, "ChunkSize": 10}'

    lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )


def test_lambda_invoke_incomplete_parameters_request(
    lambda_client: LambdaClient,
) -> None:
    lambda_name = os.environ[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN]

    test_event = '{"InventorySize": 100}'

    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )
    response_payload = response["Payload"]
    response_payload_data = json.loads(response_payload.read())
    assert "errorMessage" in response_payload_data
