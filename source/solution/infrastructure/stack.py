"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Optional

from aws_cdk import (
    Aws,
    CfnElement,
    CfnOutput,
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    StackSynthesizer,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as eventbridge
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import DynamoEventSource, SqsDlq, SqsEventSource
from cdk_nag import NagSuppressions
from constructs import Construct

from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.infrastructure.helpers.logs_insights_query import LogsInsightsQuery
from solution.infrastructure.helpers.solutions_function import SolutionsPythonFunction
from solution.infrastructure.helpers.solutions_table import SolutionsTable
from solution.infrastructure.helpers.system_manager import SystemManager
from solution.infrastructure.helpers.task_retry import TaskRetry
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows import (
    archives_status_cleanup,
    cleanup,
    cloudwatch_dashboard_update,
    extend_download_window,
    get_inventory,
    initiate_retrieval,
    orchestrator,
    retrieve_archive,
)
from solution.infrastructure.workflows.stack_info import StackInfo


class SolutionStack(Stack):
    outputs: dict[str, CfnOutput]
    skip_integration_tests: bool = True

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        synthesizer: Optional[StackSynthesizer] = None,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            synthesizer=synthesizer,
            description=f"({scope.node.try_get_context('SOLUTION_ID')}) - "
            f"{scope.node.try_get_context('SOLUTION_NAME')} - "
            f"Version: {scope.node.try_get_context('SOLUTION_VERSION')}",
        )
        skip_integration_tests = scope.node.try_get_context("skip_integration_tests")
        if skip_integration_tests:
            self.skip_integration_tests = skip_integration_tests.lower() == "true"

        output_bucket_name_context = scope.node.try_get_context("output_bucket_name")

        _excluded_assets = (
            ["**/mock_glacier_data.py", "**/__pycache__/**", "tests/**"]
            if self.skip_integration_tests
            else ["**/__pycache__/**", "tests/**"]
        )
        lambda_source = lambda_.Code.from_asset("source", exclude=_excluded_assets)
        # Sonarqube ruleKey: python:S6327
        # This is necessary and SNS has server-side encryption disabled due to Glacier service limitation.
        # All notifications from Glacier contain only metadata about the job completion status, and there is NO customer sensitive data in these notifications.
        async_facilitator_topic = sns.Topic(self, "AsyncFacilitatorTopic")  # NOSONAR

        stack_info = StackInfo(
            scope=self,
            lambda_source=lambda_source,
            async_facilitator_topic=async_facilitator_topic,
            outputs={},
            default_retry=TaskRetry(),
        )

        notifications_dlq = sqs.DeadLetterQueue(
            max_receive_count=10,
            queue=sqs.Queue(
                self,
                "NotificationsDLQ",
                enforce_ssl=True,
                encryption=sqs.QueueEncryption.SQS_MANAGED,
            ),
        )

        stack_info.queues.notifications_queue = sqs.Queue(
            self,
            "NotificationsQueue",
            visibility_timeout=Duration.seconds(905),
            delivery_delay=Duration.seconds(10),
            dead_letter_queue=notifications_dlq,
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        chunks_retrieval_queue_dlq = sqs.DeadLetterQueue(
            max_receive_count=96,
            queue=sqs.Queue(
                self,
                "ChunksRetrievalDLQ",
                enforce_ssl=True,
                encryption=sqs.QueueEncryption.SQS_MANAGED,
            ),
        )

        stack_info.queues.chunks_retrieval_queue = sqs.Queue(
            self,
            "ChunksRetrievalQueue",
            visibility_timeout=Duration.seconds(905),
            delivery_delay=Duration.seconds(10),
            dead_letter_queue=chunks_retrieval_queue_dlq,
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        validation_queue_dlq = sqs.DeadLetterQueue(
            max_receive_count=24,
            queue=sqs.Queue(
                self,
                "ValidationDLQ",
                enforce_ssl=True,
                encryption=sqs.QueueEncryption.SQS_MANAGED,
            ),
        )

        stack_info.queues.validation_queue = sqs.Queue(
            self,
            "ValidationQueue",
            visibility_timeout=Duration.seconds(905),
            delivery_delay=Duration.seconds(10),
            dead_letter_queue=validation_queue_dlq,
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        stack_info.outputs[OutputKeys.NOTIFICATIONS_SQS_URL] = CfnOutput(
            self,
            OutputKeys.NOTIFICATIONS_SQS_URL,
            value=stack_info.queues.notifications_queue.queue_url,
        )

        stack_info.outputs[OutputKeys.CHUNKS_SQS_URL] = CfnOutput(
            self,
            OutputKeys.CHUNKS_SQS_URL,
            value=stack_info.queues.chunks_retrieval_queue.queue_url,
        )

        stack_info.outputs[OutputKeys.VALIDATION_SQS_URL] = CfnOutput(
            self,
            OutputKeys.VALIDATION_SQS_URL,
            value=stack_info.queues.validation_queue.queue_url,
        )

        stack_info.outputs[OutputKeys.NOTIFICATIONS_SQS_ARN] = CfnOutput(
            self,
            OutputKeys.NOTIFICATIONS_SQS_ARN,
            value=stack_info.queues.notifications_queue.queue_arn,
        )

        stack_info.outputs[OutputKeys.CHUNKS_SQS_ARN] = CfnOutput(
            self,
            OutputKeys.CHUNKS_SQS_ARN,
            value=stack_info.queues.chunks_retrieval_queue.queue_arn,
        )

        stack_info.outputs[OutputKeys.VALIDATION_SQS_ARN] = CfnOutput(
            self,
            OutputKeys.VALIDATION_SQS_ARN,
            value=stack_info.queues.validation_queue.queue_arn,
        )

        async_facilitator_topic.add_subscription(
            subscriptions.SqsSubscription(stack_info.queues.notifications_queue)
        )

        # Bucket naming rules: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
        stack_info.parameters.destination_bucket_parameter = CfnParameter(
            self,
            "DestinationBucketParameter",
            type="String",
            description="(Required) The destination Amazon S3 bucket name",
            allowed_pattern="(?!(^xn--|^sthree-|.+-s3alias$|.+--ol-s3$))^[a-zA-Z0-9][a-zA-Z0-9-._]{1,253}[a-zA-Z0-9]$",
        )

        if output_bucket_name_context:
            stack_info.parameters.destination_bucket_parameter.default = (
                output_bucket_name_context
            )

        stack_info.parameters.enable_ddb_backup_parameter = CfnParameter(
            self,
            "EnableDDBBackupParameter",
            type="String",
            default="false",
            description="Enable or disable DynamoDB table backups (default is false)",
            allowed_values=["true", "false"],
        )

        stack_info.parameters.enable_step_function_logging_parameter = CfnParameter(
            self,
            "EnableStepFunctionLoggingParameter",
            type="String",
            default="false",
            description="Enable or disable Step Functions logging (default is false)",
            allowed_values=["true", "false"],
        )

        stack_info.parameters.enable_lambda_tracing_parameter = CfnParameter(
            self,
            "EnableLambdaTracingParameter",
            type="String",
            default="false",
            description="Enable or disable Lambda functions X-Ray tracing (default is false)",
            allowed_values=["true", "false"],
        )

        stack_info.parameters.enable_step_function_tracing_parameter = CfnParameter(
            self,
            "EnableStepFunctionTracingParameter",
            type="String",
            default="false",
            description="Enable or disable Step Functions X-Ray tracing (default is false)",
            allowed_values=["true", "false"],
        )

        stack_info.tables.async_facilitator_table = SolutionsTable(
            self,
            "AsyncFacilitatorTable",
            stack_info.parameters.enable_ddb_backup_parameter.value_as_string,
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
        )

        stack_info.outputs[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
            value=stack_info.tables.async_facilitator_table.table_name,
        )

        stack_info.tables.glacier_retrieval_table = SolutionsTable(
            self,
            "GlacierObjectRetrieval",
            stack_info.parameters.enable_ddb_backup_parameter.value_as_string,
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )

        glacier_retrieval_job_index_name = "GlacierRetrievalJobIndex"
        stack_info.tables.glacier_retrieval_table.add_global_secondary_index(
            index_name=glacier_retrieval_job_index_name,
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
        )

        stack_info.outputs[OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME,
            value=glacier_retrieval_job_index_name,
        )

        stack_info.outputs[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME,
            value=stack_info.tables.glacier_retrieval_table.table_name,
        )

        stack_info.tables.metric_table = SolutionsTable(
            self,
            "MetricTable",
            stack_info.parameters.enable_ddb_backup_parameter.value_as_string,
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
        )

        stack_info.outputs[OutputKeys.METRIC_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.METRIC_TABLE_NAME,
            value=stack_info.tables.metric_table.table_name,
        )

        stack_info.async_facilitator_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                conditions={"Bool": {"aws:SecureTransport": False}},
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                resources=[stack_info.async_facilitator_topic.topic_arn],
            )
        )

        stack_info.async_facilitator_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["SNS:Publish"],
                effect=iam.Effect.ALLOW,
                resources=[stack_info.async_facilitator_topic.topic_arn],
                principals=[
                    iam.ServicePrincipal("glacier.amazonaws.com"),
                ],
                conditions={"StringEquals": {"AWS:SourceOwner": Aws.ACCOUNT_ID}},
            )
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.async_facilitator_topic.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-SNS2",
                    "reason": "SNS Topic does not have server-side encryption enabled to be able to receive notifications from Glacier",
                },
            ],
        )

        stack_info.outputs[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN,
            value=stack_info.async_facilitator_topic.topic_arn,
        )

        stack_info.lambdas.notifications_processor_lambda = SolutionsPythonFunction(
            self,
            "NotificationsProcessor",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.notifications_processor",
            code=lambda_source,
            memory_size=256,
            timeout=Duration.minutes(15),
        )

        stack_info.eventbridge_rules.completion_checker_trigger = eventbridge.Rule(
            self,
            "WorkflowCompletionCheckerTrigger",
            schedule=eventbridge.Schedule.expression("cron(0/15 * * * ? *)"),
        )

        stack_info.outputs[
            OutputKeys.WORKFLOW_COMPLETION_CHECKER_RULE_NAME
        ] = CfnOutput(
            self,
            OutputKeys.WORKFLOW_COMPLETION_CHECKER_RULE_NAME,
            value=stack_info.eventbridge_rules.completion_checker_trigger.rule_name,
        )

        stack_info.lambdas.completion_checker_lambda = SolutionsPythonFunction(
            self,
            "CompletionChecker",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.completion_checker",
            code=lambda_source,
            memory_size=256,
            timeout=Duration.minutes(3),
            environment={
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
                OutputKeys.ASYNC_FACILITATOR_TABLE_NAME: stack_info.tables.async_facilitator_table.table_name,
            },
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.completion_checker_lambda
        )

        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.lambdas.completion_checker_lambda
        )

        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.lambdas.notifications_processor_lambda
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.notifications_processor_lambda
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.notifications_processor_lambda
        )

        NOTIFICATIONS_PROCESSOR_MAX_CONCURRNECY = 45
        stack_info.lambdas.notifications_processor_lambda.add_event_source(
            SqsEventSource(
                stack_info.queues.notifications_queue,
                max_concurrency=NOTIFICATIONS_PROCESSOR_MAX_CONCURRNECY,
                batch_size=1,
            )
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
            stack_info.tables.async_facilitator_table.table_name,
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME,
            stack_info.tables.glacier_retrieval_table.table_name,
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.METRIC_TABLE_NAME,
            stack_info.tables.metric_table.table_name,
        )

        stack_info.lambdas.async_facilitator_lambda = SolutionsPythonFunction(
            self,
            "AsyncFacilitator",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.async_facilitator",
            code=lambda_source,
            memory_size=256,
            timeout=Duration.seconds(120),
        )

        async_facilitator_dlq = sqs.Queue(
            stack_info.scope,
            "AsyncFacilitatorDLQ",
            enforce_ssl=True,
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        ASYNC_FACILITATOR_MAX_RETRY_ATTEMPTS = 5
        stack_info.lambdas.async_facilitator_lambda.add_event_source(
            DynamoEventSource(
                stack_info.tables.async_facilitator_table,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                filters=[
                    lambda_.FilterCriteria.filter(
                        {"eventName": lambda_.FilterRule.or_("INSERT", "MODIFY")}
                    )
                ],
                bisect_batch_on_error=True,
                retry_attempts=ASYNC_FACILITATOR_MAX_RETRY_ATTEMPTS,
                on_failure=SqsDlq(async_facilitator_dlq),
            )
        )
        stack_info.lambdas.async_facilitator_lambda.add_environment(
            OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
            stack_info.tables.async_facilitator_table.table_name,
        )

        stack_info.outputs[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME,
            value=stack_info.lambdas.async_facilitator_lambda.function_name,
        )

        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.lambdas.async_facilitator_lambda
        )
        stack_info.tables.async_facilitator_table.grant_stream_read(
            stack_info.lambdas.async_facilitator_lambda
        )

        stack_info.lambdas.metric_update_on_status_change_lambda = SolutionsPythonFunction(
            self,
            "MetricsProcessor",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.update_metric_on_status_change",
            code=lambda_source,
            memory_size=256,
            timeout=Duration.minutes(15),
            environment={
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
            },
        )

        stack_info.outputs[OutputKeys.METRIC_UPDATE_LAMBDA_NAME] = CfnOutput(
            self,
            OutputKeys.METRIC_UPDATE_LAMBDA_NAME,
            value=stack_info.lambdas.metric_update_on_status_change_lambda.function_name,
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.metric_update_on_status_change_lambda
        )
        stack_info.tables.metric_table.grant_stream_read(
            stack_info.lambdas.metric_update_on_status_change_lambda
        )

        MAX_RETRY_ATTEMPTS = 10
        stack_info.lambdas.metric_update_on_status_change_lambda.add_event_source(
            DynamoEventSource(
                stack_info.tables.glacier_retrieval_table,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                filters=[
                    lambda_.FilterCriteria.filter(
                        {
                            "eventName": lambda_.FilterRule.or_("INSERT", "MODIFY"),
                            "dynamodb": {
                                "NewImage": {
                                    "sk": {"S": ["meta"]},
                                    "retrieval_type": {
                                        "S": [GlacierJobType.ARCHIVE_RETRIEVAL]
                                    },
                                }
                            },
                        }
                    )
                ],
                batch_size=20,
                parallelization_factor=1,
                max_batching_window=Duration.seconds(300),
                retry_attempts=MAX_RETRY_ATTEMPTS,
            )
        )

        assert stack_info.lambdas.metric_update_on_status_change_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.metric_update_on_status_change_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "When activating stream for dynamodb table. It automatically allow listStreams to all resources with a wildcard and should be suppressed",
                    "appliesTo": [
                        "Resource::*",
                    ],
                }
            ],
        )

        assert isinstance(
            stack_info.tables.async_facilitator_table.node.default_child, CfnElement
        )
        async_facilitator_table_logical_id = Stack.of(self).get_logical_id(
            stack_info.tables.async_facilitator_table.node.default_child
        )

        stack_info.lambdas.async_facilitator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:SendTaskSuccess",
                    "states:DescribeExecution",
                    "states:SendTaskFailure",
                ],
                resources=[
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InventoryRetrievalStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InventoryRetrievalStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InitiateMetadataStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InitiateMetadataStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:RetrieveArchiveStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:RetrieveArchiveStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:OrchestratorStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:OrchestratorStateMachine*",
                ],
            ),
        )

        NagSuppressions.add_resource_suppressions(
            async_facilitator_dlq,
            [
                {
                    "id": "AwsSolutions-SQS3",
                    "reason": "The SQS queue is used as a dead-letter queue (DLQ) and does not need DLQ enabled",
                },
            ],
        )

        assert stack_info.lambdas.async_facilitator_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.async_facilitator_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "When activating stream for dynamodb table. It automatically allow listStreams to all resources with a wildcard and should be suppressed",
                    "appliesTo": [
                        f"Resource::<{async_facilitator_table_logical_id}.Arn>/stream/*",
                        "Resource::*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Lambda permission needed to SendTaskSuccess and SendTaskFailure.",
                    "appliesTo": [
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:InventoryRetrievalStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:InventoryRetrievalStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:InitiateMetadataStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:InitiateMetadataStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:RetrieveArchiveStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:RetrieveArchiveStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:ExtendDownloadWindowStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:ExtendDownloadWindowStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:OrchestratorStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:OrchestratorStateMachine*",
                    ],
                },
            ],
        )

        assert stack_info.lambdas.notifications_processor_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.notifications_processor_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "When activating stream for dynamodb table. It automatically allow listStreams to all resources with a wildcard and should be suppressed",
                    "appliesTo": [
                        "Resource::<GlacierObjectRetrieval80EDDD39.Arn>/index/*"
                    ],
                },
            ],
        )

        # Bucket to store buckets access logs
        stack_info.buckets.access_logs_bucket = s3.Bucket(
            self,
            "BucketAccessLogs",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
        )

        # Bucket to store the restored vault.
        stack_info.interfaces.output_bucket = s3.Bucket.from_bucket_name(
            self,
            "OutputBucket",
            bucket_name=stack_info.parameters.destination_bucket_parameter.value_as_string,
        )

        stack_info.outputs[OutputKeys.OUTPUT_BUCKET_NAME] = CfnOutput(
            self,
            OutputKeys.OUTPUT_BUCKET_NAME,
            value=stack_info.interfaces.output_bucket.bucket_name,
        )

        # Bucket to store the inventory and the Glue output after it's sorted.
        stack_info.buckets.inventory_bucket = s3.Bucket(
            self,
            "InventoryBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            server_access_logs_bucket=stack_info.buckets.access_logs_bucket,
            server_access_logs_prefix="inventory_bucket_access_logs",
        )

        stack_info.outputs[OutputKeys.INVENTORY_BUCKET_NAME] = CfnOutput(
            self,
            OutputKeys.INVENTORY_BUCKET_NAME,
            value=stack_info.buckets.inventory_bucket.bucket_name,
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.OUTPUT_BUCKET_NAME,
            stack_info.interfaces.output_bucket.bucket_name,
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.CHUNKS_SQS_URL,
            stack_info.queues.chunks_retrieval_queue.queue_url,
        )

        stack_info.lambdas.notifications_processor_lambda.add_environment(
            OutputKeys.GLACIER_RETRIEVAL_JOB_INDEX_NAME,
            glacier_retrieval_job_index_name,
        )

        stack_info.queues.chunks_retrieval_queue.grant_send_messages(
            stack_info.lambdas.notifications_processor_lambda
        )

        stack_info.interfaces.output_bucket.grant_read_write(
            stack_info.lambdas.notifications_processor_lambda
        )

        assert stack_info.lambdas.notifications_processor_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.notifications_processor_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier archives over to s3 in any location",
                    "appliesTo": [
                        "Resource::arn:<AWS::Partition>:s3:::<DestinationBucketParameter>/*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )

        get_inventory.Workflow(stack_info)
        initiate_retrieval.Workflow(stack_info)
        retrieve_archive.Workflow(stack_info)
        extend_download_window.Workflow(stack_info)
        cloudwatch_dashboard_update.Workflow(stack_info)
        cleanup.Workflow(stack_info)
        archives_status_cleanup.Workflow(stack_info)
        orchestrator.Workflow(stack_info)

        LogsInsightsQuery(stack_info)

        # Creating the System Manager Resources
        allow_cross_region_data_transfer = self.skip_integration_tests == False
        SystemManager(stack_info, allow_cross_region_data_transfer)
        self.outputs = stack_info.outputs
        self.stack_info = stack_info
