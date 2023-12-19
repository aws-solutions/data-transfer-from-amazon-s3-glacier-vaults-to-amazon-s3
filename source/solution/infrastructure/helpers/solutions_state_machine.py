"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any

from aws_cdk import Aws, CfnCondition, CfnElement, Fn, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk.aws_stepfunctions import CfnStateMachine, StateMachine, StateMachineType
from cdk_nag import NagSuppressions
from constructs import Construct


class SolutionsStateMachine(StateMachine):
    STANDARD = StateMachineType.STANDARD
    EXPRESS = StateMachineType.EXPRESS

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        enable_step_function_logging: str,
        enable_x_ray_tracing: str,
        **kwargs: Any,
    ):
        self.scope = scope
        self.construct_id = construct_id

        # initialize the parent Function
        super().__init__(scope, construct_id, **kwargs)

        self._configure_logging(enable_step_function_logging)
        self._configure_x_ray_tracing(enable_x_ray_tracing)

        NagSuppressions.add_resource_suppressions(
            self,
            [
                {
                    "id": "AwsSolutions-SF1",
                    "reason": "Step Function logging is configurable based on a CloudFormation parameter, due to price considerations.",
                },
                {
                    "id": "AwsSolutions-SF2",
                    "reason": "Step Function X-Ray tracing is configurable based on a CloudFormation parameter, due to price considerations.",
                },
            ],
        )

    def _configure_logging(self, enable_step_function_logging: str) -> None:
        log_group_deploy_condition = CfnCondition(
            self.scope,
            f"{self.construct_id}-LogGroupCondition",
            expression=Fn.condition_equals(enable_step_function_logging, "true"),
        )

        self._add_logging_policy(log_group_deploy_condition)

        log_group = logs.LogGroup(
            self.scope,
            f"{self.construct_id}-LogGroup",
            log_group_name=self._generate_log_group_name(),
        )
        cfn_log_group = log_group.node.default_child
        assert isinstance(cfn_log_group, logs.CfnLogGroup)
        cfn_log_group.cfn_options.condition = log_group_deploy_condition

        assert isinstance(log_group.node.default_child, CfnElement)
        log_group_logical_id = Stack.of(self.scope).get_logical_id(
            log_group.node.default_child
        )

        logging_configuration = {
            "Destinations": [
                {
                    "CloudWatchLogsLogGroup": {
                        "LogGroupArn": {"Fn::GetAtt": [log_group_logical_id, "Arn"]}
                    }
                }
            ],
            "Level": "ALL",
        }

        logging_configuration_if_condition = Fn.condition_if(
            log_group_deploy_condition.logical_id,
            logging_configuration,
            {},
        )

        cfn_sfn = self.node.default_child
        assert isinstance(cfn_sfn, CfnStateMachine)
        cfn_sfn.add_override(
            "Properties.LoggingConfiguration", logging_configuration_if_condition
        )

    def _add_logging_policy(self, log_group_deploy_condition: CfnCondition) -> None:
        # https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html#cloudwatch-iam-policy
        # Need to specify * in the Resource field because CloudWatch API actions, such as CreateLogDelivery and DescribeLogGroups,
        # don't support Resource types defined by Amazon CloudWatch Logs
        logging_state_machine_policy = iam.Policy(
            self.scope,
            f"{self.construct_id}-LoggingPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogDelivery",
                        "logs:CreateLogStream",
                        "logs:GetLogDelivery",
                        "logs:UpdateLogDelivery",
                        "logs:DeleteLogDelivery",
                        "logs:ListLogDeliveries",
                        "logs:PutLogEvents",
                        "logs:PutResourcePolicy",
                        "logs:DescribeResourcePolicies",
                        "logs:DescribeLogGroups",
                    ],
                    # Sonarqube ruleKey: python:S6304
                    # This is safe as per safe as per public document https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html#cloudwatch-iam-policy
                    resources=["*"],  # NOSONAR
                )
            ],
        )

        policy_if_condition = Fn.condition_if(
            log_group_deploy_condition.logical_id,
            iam.Effect.ALLOW,
            iam.Effect.DENY,
        )

        cfn_policy = logging_state_machine_policy.node.default_child
        assert isinstance(cfn_policy, iam.CfnPolicy)
        cfn_policy.add_override(
            "Properties.PolicyDocument.Statement.Effect", policy_if_condition
        )

        self._suppress_cdk_nag(logging_state_machine_policy)
        logging_state_machine_policy.attach_to_role(self.role)

    def _generate_log_group_name(self) -> str:
        # Extract the trailing GUID characters from AWS::StackId and concatenate it to log_group_name
        # to ensure log group name remains the same throughout the stack's lifetime,
        # while varies for every stack instance.
        # https://github.com/aws/aws-cdk/discussions/25257
        stack_id = Fn.select(2, Fn.split("/", Aws.STACK_ID))

        # When creating a state machine with logging enabled, Step Functions must update CloudWatch Logs resource policy
        # with the log group. CloudWatch Logs resource policies are limited to 5120 characters.
        # To avoid reaching the CloudWatch Logs resource policy size limit,
        # CloudWatch Logs log group names should be perfixed with /aws/vendedlogs/.
        # https://docs.aws.amazon.com/step-functions/latest/dg/bp-cwl.html
        return f"/aws/vendedlogs/states/{self.construct_id}-LogGroup-{stack_id}"

    def _configure_x_ray_tracing(self, enable_x_ray_tracing: str) -> None:
        x_ray_tracing_condition = CfnCondition(
            self.scope,
            f"{self.construct_id}-XRayTracingCondition",
            expression=Fn.condition_equals(enable_x_ray_tracing, "true"),
        )

        cfn_sfn = self.node.default_child
        assert isinstance(cfn_sfn, CfnStateMachine)
        cfn_sfn.add_override(
            "Properties.TracingConfiguration.Enabled", enable_x_ray_tracing
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

    def _suppress_cdk_nag(self, logging_state_machine_policy: iam.Policy) -> None:
        NagSuppressions.add_resource_suppressions(
            logging_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Need to specify wildcard because CloudWatch API actions, such as CreateLogDelivery and DescribeLogGroups don't support Resource types defined by Amazon CloudWatch Logs",
                    "appliesTo": ["Resource::*"],
                },
            ],
        )
