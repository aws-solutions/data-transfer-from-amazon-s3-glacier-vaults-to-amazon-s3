"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

from aws_cdk import CfnOutput, CfnParameter, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as eventbridge
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn

from solution.infrastructure.helpers.task_retry import TaskRetry


@dataclass
class StateMachines:
    cleanup_state_machine: sfn.StateMachine | None = field(default=None)
    extend_download_window_state_machine: sfn.StateMachine | None = field(default=None)
    inventory_retrieval_state_machine: sfn.StateMachine | None = field(default=None)
    initiate_retrieval_state_machine: sfn.StateMachine | None = field(default=None)
    orchestrator_state_machine: sfn.StateMachine | None = field(default=None)
    cloudwatch_dashboard_update_state_machine: sfn.StateMachine | None = field(
        default=None
    )
    cleanup_archives_status_state_machine: sfn.StateMachine | None = field(default=None)


@dataclass
class Lambdas:
    async_facilitator_lambda: lambda_.Function | None = field(default=None)
    archives_needing_window_extension_lambda: lambda_.Function | None = field(
        default=None
    )
    archive_naming_override_lambda: lambda_.Function | None = field(default=None)
    initiate_inventory_retrieval_lambda: lambda_.Function | None = field(default=None)
    inventory_chunk_determination_lambda: lambda_.Function | None = field(default=None)
    inventory_retrieval_lambda_function: lambda_.Function | None = field(default=None)
    inventory_validation_lambda: lambda_.Function | None = field(default=None)
    initiate_archive_retrieval_lambda: lambda_.Function | None = field(default=None)
    chunk_retrieval_lambda: lambda_.Function | None = field(default=None)
    archive_validation_lambda: lambda_.Function | None = field(default=None)
    extend_download_window_initiate_retrieval_lambda: lambda_.Function | None = field(
        default=None
    )
    cleanup_incomplete_multipart_uploads_lambda: lambda_.Function | None = field(
        default=None
    )
    send_anonymized_stats_lambda: lambda_.Function | None = field(default=None)
    notifications_processor_lambda: lambda_.Function | None = field(default=None)
    calculate_timeout_lambda: lambda_.Function | None = field(default=None)
    completion_checker_lambda: lambda_.Function | None = field(default=None)
    archives_needing_status_cleanup_lambda: lambda_.Function | None = field(
        default=None
    )
    cleanup_archives_status_lambda: lambda_.Function | None = field(default=None)
    post_workflow_dashboard_update: lambda_.Function | None = field(default=None)
    metric_update_on_status_change_lambda: lambda_.Function | None = field(default=None)


@dataclass
class Buckets:
    inventory_bucket: s3.Bucket | None = field(default=None)
    access_logs_bucket: s3.Bucket | None = field(default=None)


@dataclass
class Interfaces:
    output_bucket: s3.IBucket | None = field(default=None)


@dataclass
class Parameters:
    destination_bucket_parameter: CfnParameter | None = field(default=None)
    enable_ddb_backup_parameter: CfnParameter | None = field(default=None)
    enable_step_function_logging_parameter: CfnParameter | None = field(default=None)
    enable_lambda_tracing_parameter: CfnParameter | None = field(default=None)
    enable_step_function_tracing_parameter: CfnParameter | None = field(default=None)


@dataclass
class Tables:
    glacier_retrieval_table: dynamodb.Table | None = field(default=None)
    metric_table: dynamodb.Table | None = field(default=None)
    async_facilitator_table: dynamodb.Table | None = field(default=None)


@dataclass
class EventbridgeRules:
    extend_download_window_trigger: eventbridge.Rule | None = field(default=None)
    cloudwatch_dashboard_update_trigger: eventbridge.Rule | None = field(default=None)
    completion_checker_trigger: eventbridge.Rule | None = field(default=None)


@dataclass
class Policies:
    get_job_output_policy: iam.Policy | None = field(default=None)


@dataclass
class Queues:
    notifications_queue: sqs.Queue | None = field(default=None)
    chunks_retrieval_queue: sqs.Queue | None = field(default=None)
    validation_queue: sqs.Queue | None = field(default=None)


@dataclass(kw_only=True)
class StackInfo:
    scope: Stack
    lambda_source: lambda_.Code
    async_facilitator_topic: sns.Topic
    outputs: Dict[str, CfnOutput]
    default_retry: TaskRetry
    queues: Queues = field(default_factory=lambda: Queues())
    parameters: Parameters = field(default_factory=lambda: Parameters())
    tables: Tables = field(default_factory=lambda: Tables())
    buckets: Buckets = field(default_factory=lambda: Buckets())
    lambdas: Lambdas = field(default_factory=lambda: Lambdas())
    state_machines: StateMachines = field(default_factory=lambda: StateMachines())
    eventbridge_rules: EventbridgeRules = field(
        default_factory=lambda: EventbridgeRules()
    )
    policies: Policies = field(default_factory=lambda: Policies())
    interfaces: Interfaces = field(default_factory=lambda: Interfaces())
