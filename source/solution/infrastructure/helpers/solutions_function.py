"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import Any

import aws_cdk.aws_iam as iam
from aws_cdk import Aws, CfnCondition, Fn
from aws_cdk.aws_lambda import CfnFunction, Function, Runtime, Tracing
from cdk_nag import NagSuppressions
from constructs import Construct

from solution.application.util.exceptions import ResourceNotFound

DEFAULT_RUNTIME = Runtime.PYTHON_3_11


class SolutionsPythonFunction(Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        enable_x_ray_tracing: str,
        **kwargs: Any,
    ):
        self.scope = scope
        self.construct_id = construct_id

        # create default least privileged role for this function unless a role is passed
        if not kwargs.get("role"):
            kwargs["role"] = self._create_role()

        # set runtime to Python 3.11 unless a runtime is passed
        if not kwargs.get("runtime"):
            kwargs["runtime"] = DEFAULT_RUNTIME

        # create default environment variable LOGGING_LEVEL
        kwargs.setdefault("environment", {})["LOGGING_LEVEL"] = str(logging.INFO)

        # initialize the parent Function
        super().__init__(scope, construct_id, **kwargs)

        self._configure_x_ray_tracing(enable_x_ray_tracing)

    def _create_role(self) -> iam.Role:
        """
        Build a role that allows an AWS Lambda Function to log to CloudWatch
        :param name: The name of the role. The final name will be "{name}-Role"
        :return: aws_cdk.aws_iam.Role
        """
        role = iam.Role(
            self.scope,
            f"{self.construct_id}-Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "LambdaFunctionServiceRolePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            resources=[
                                f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/*"
                            ],
                        )
                    ]
                )
            },
        )

        NagSuppressions.add_resource_suppressions(
            role.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Lambda needs to be able to create a logging group and add logs to it",
                    "appliesTo": [
                        "Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/lambda/*",
                    ],
                },
            ],
        )
        return role

    def _configure_x_ray_tracing(self, enable_x_ray_tracing: str) -> None:
        x_ray_tracing_condition = CfnCondition(
            self.scope,
            f"{self.construct_id}-XRayTracingCondition",
            expression=Fn.condition_equals(enable_x_ray_tracing, "true"),
        )

        tracing_configuration = {"Mode": Tracing.ACTIVE}

        x_ray_tracing_if_condition = Fn.condition_if(
            x_ray_tracing_condition.logical_id, tracing_configuration, {}
        )

        cfn_function = self.node.default_child

        assert isinstance(cfn_function, CfnFunction)

        cfn_function.add_override(
            "Properties.TracingConfig", x_ray_tracing_if_condition
        )

        tracing_policy = iam.Policy(
            self.scope,
            f"{self.construct_id}-TracingPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "xray:PutTraceSegments",
                        "xray:PutTelemetryRecords",
                        "xray:GetSamplingRules",
                        "xray:GetSamplingTargets",
                        "xray:GetSamplingStatisticSummaries",
                    ],
                    resources=["*"],
                )
            ],
        )

        policy_if_condition = Fn.condition_if(
            x_ray_tracing_condition.logical_id,
            iam.Effect.ALLOW,
            iam.Effect.DENY,
        )

        cfn_policy = tracing_policy.node.default_child

        assert isinstance(cfn_policy, iam.CfnPolicy)

        cfn_policy.add_override(
            "Properties.PolicyDocument.Statement.Effect", policy_if_condition
        )

        assert self.role is not None

        tracing_policy.attach_to_role(self.role)

        NagSuppressions.add_resource_suppressions(
            tracing_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcard permissions needed for X-Ray tracing",
                    "appliesTo": ["Resource::*"],
                },
            ],
        )
