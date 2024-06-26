"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING

import boto3

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object


def test_lambda_invoke(lambda_client: LambdaClient) -> None:
    # ClientError exception will be thrown if the invocation of the Lambda function fails.

    lambda_name = os.environ[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN]

    test_event = ""

    lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )
