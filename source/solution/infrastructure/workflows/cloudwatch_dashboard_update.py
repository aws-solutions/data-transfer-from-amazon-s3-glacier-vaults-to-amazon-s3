"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import Optional

from aws_cdk import Aws, CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as eventbridge
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from cdk_nag import NagSuppressions

from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.helpers.cloudwatch_dashboard import (
    METRIC_LABEL_LIST,
    METRICS_NAMESPACE,
    CwDashboard,
)
from solution.infrastructure.helpers.solutions_state_machine import (
    SolutionsStateMachine,
)
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo) -> None:
        if stack_info.tables.metric_table is None:
            raise ResourceNotFound("Metric Table")
        if stack_info.queues.chunks_retrieval_queue is None:
            raise ResourceNotFound("Chunks Queue")
        if stack_info.queues.validation_queue is None:
            raise ResourceNotFound("Validation Queue")
        if stack_info.queues.notifications_queue is None:
            raise ResourceNotFound("Notifications Queue")

        self.scope = stack_info.scope
        CwDashboard(
            self.scope,
            [
                ("Notifications Queue", stack_info.queues.notifications_queue),
                ("Chunks retrieval Queue", stack_info.queues.chunks_retrieval_queue),
                ("Validation Queue", stack_info.queues.validation_queue),
            ],
        )  # To create the CloudWatch Dashboard

        stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger = (
            eventbridge.Rule(
                self.scope,
                "CloudWatchDashboardUpdate",
                schedule=eventbridge.Schedule.expression("cron(0/5 * * * ? *)"),
            )
        )

        stack_info.outputs[
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_RULE_NAME
        ] = CfnOutput(
            self.scope,
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_RULE_NAME,
            value=stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger.rule_name,
        )

        get_metrics_from_metric_table = tasks.DynamoGetItem(
            self.scope,
            "GetMetricsFromMetricTable",
            key={
                "pk": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.workflow_run")
                ),
            },
            table=stack_info.tables.metric_table,
            result_path="$.get_metrics",
        )

        put_metrics_to_cw = self.put_metrics_to_cloudwatch

        stack_info.default_retry.apply_to_steps(
            [get_metrics_from_metric_table, put_metrics_to_cw]
        )

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.cloudwatch_dashboard_update_state_machine = SolutionsStateMachine(
            self.scope,
            "CloudWatchDashboardUpdateStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=get_metrics_from_metric_table.next(put_metrics_to_cw),
            state_machine_type=SolutionsStateMachine.EXPRESS,
        )

        assert (
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine
            is not None
        )

        stack_info.tables.metric_table.grant_read_data(
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine
        )
        put_metric_data_policy = iam.Policy(
            self.scope,
            "PutMetricDataPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                    conditions={
                        "StringEquals": {"cloudwatch:namespace": METRICS_NAMESPACE}
                    },
                )
            ],
        )
        put_metric_data_policy.attach_to_role(
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine.role
        )

        stack_info.outputs[
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_STATE_MACHINE_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_STATE_MACHINE_ARN,
            value=stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_arn,
        )

        NagSuppressions.add_resource_suppressions(
            put_metric_data_policy.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "PutMetricData action is allowed for all resources metrics withing a namespace.",
                    "appliesTo": [
                        "Resource::*",
                    ],
                }
            ],
        )

    @property
    def put_metrics_to_cloudwatch(self) -> tasks.CallAwsService:
        metric_data = []
        dimensions = [{"Name": "WorkflowRun", "Value.$": "$.get_metrics.Item.pk.S"}]

        for name, label in METRIC_LABEL_LIST:
            metric_status, metric_type = name.split("Archive")
            metric_data.append(
                {
                    "Dimensions": dimensions,
                    "MetricName": name,
                    "Value.$": f"States.StringToJson($.get_metrics.Item.{metric_type.lower()}_{metric_status.lower()}.N)",
                }
            )

        return tasks.CallAwsService(
            self.scope,
            "PutMetricsToCloudwatch",
            service="cloudwatch",
            action="putMetricData",
            parameters={"MetricData": metric_data, "Namespace": METRICS_NAMESPACE},
            iam_resources=["arn:aws:states:::aws-sdk:cloudwatch:putMetricData"],
        )
