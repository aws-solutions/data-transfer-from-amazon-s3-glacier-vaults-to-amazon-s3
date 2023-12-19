"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_lambda as lambda_
from constructs import Construct

from solution.infrastructure.output_keys import OutputKeys


class MockGlacierStack(Stack):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        self.mock_notify_sns_lambda = lambda_.Function(
            self,
            "MockNotifySns",
            handler="solution.application.mocking.handlers.mock_notify_sns_handler",
            runtime=lambda_.Runtime.PYTHON_3_10,
            code=lambda_.Code.from_asset("source", exclude=["tests/**"]),
            description="Lambda to mock notifying SNS job completion.",
            timeout=Duration.seconds(20),
        )

        self.outputs = [
            CfnOutput(
                self,
                OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN,
                value=self.mock_notify_sns_lambda.function_arn,
            )
        ]
