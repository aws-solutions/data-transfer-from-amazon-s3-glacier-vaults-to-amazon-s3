"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import CfnElement, CfnOutput, Duration, Stack
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from cdk_nag import NagSuppressions

from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.helpers.solutions_function import SolutionsPythonFunction
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo):
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier retrieval table")
        if stack_info.tables.metric_table is None:
            raise ResourceNotFound("Metric table")
        if stack_info.buckets.output_bucket is None:
            raise ResourceNotFound("Output bucket")
        if stack_info.policies.get_job_output_policy is None:
            raise ResourceNotFound("Get job output policy")
        if stack_info.parameters.enable_lambda_tracing_parameter is None:
            raise ResourceNotFound("Enable lambda tracing parameter")
        if stack_info.queues.chunks_retrieval_queue is None:
            raise ResourceNotFound("Chunks Queue")
        if stack_info.queues.validation_queue is None:
            raise ResourceNotFound("Validation Queue")

        stack_info.lambdas.chunk_retrieval_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ChunkRetrieval",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.archive_retrieval",
            code=stack_info.lambda_source,
            memory_size=1536,
            timeout=Duration.minutes(15),
            description="Lambda to retrieve chunks from Glacier, upload them to S3 and generate file checksums.",
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        CHUNKS_RETRIEVAL_MAX_CONCURRNECY = 70
        stack_info.lambdas.chunk_retrieval_lambda.add_event_source(
            SqsEventSource(
                stack_info.queues.chunks_retrieval_queue,
                max_concurrency=CHUNKS_RETRIEVAL_MAX_CONCURRNECY,
                batch_size=1,
            )
        )

        stack_info.buckets.output_bucket.grant_put(
            stack_info.lambdas.chunk_retrieval_lambda
        )

        assert stack_info.lambdas.chunk_retrieval_lambda.role is not None
        stack_info.policies.get_job_output_policy.attach_to_role(
            stack_info.lambdas.chunk_retrieval_lambda.role
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.chunk_retrieval_lambda
        )

        stack_info.lambdas.chunk_retrieval_lambda.add_environment(
            OutputKeys.VALIDATION_SQS_URL,
            stack_info.queues.validation_queue.queue_url,
        )

        stack_info.queues.validation_queue.grant_send_messages(
            stack_info.lambdas.chunk_retrieval_lambda
        )

        stack_info.outputs[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN,
            value=stack_info.lambdas.chunk_retrieval_lambda.function_name,
        )

        stack_info.outputs[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.CHUNK_RETRIEVAL_LAMBDA_NAME,
            value=stack_info.lambdas.chunk_retrieval_lambda.function_name,
        )

        stack_info.lambdas.archive_validation_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ArchiveValidation",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.archive_validation",
            code=stack_info.lambda_source,
            memory_size=256,
            timeout=Duration.minutes(15),
            description="Lambda to validate and complete the S3 multipart upload.",
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        stack_info.outputs[OutputKeys.ARCHIVE_VALIDATION_LAMBDA_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.ARCHIVE_VALIDATION_LAMBDA_ARN,
            value=stack_info.lambdas.archive_validation_lambda.function_name,
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.archive_validation_lambda
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.archive_validation_lambda
        )

        stack_info.buckets.output_bucket.grant_read_write(
            stack_info.lambdas.archive_validation_lambda
        )

        stack_info.lambdas.archive_validation_lambda.add_environment(
            OutputKeys.METRIC_TABLE_NAME,
            stack_info.tables.metric_table.table_name,
        )

        VALIDATION_MAX_CONCURRNECY = 70
        stack_info.lambdas.archive_validation_lambda.add_event_source(
            SqsEventSource(
                stack_info.queues.validation_queue,
                max_concurrency=VALIDATION_MAX_CONCURRNECY,
                batch_size=5,
            )
        )

        stack_info.tables.glacier_retrieval_table.grant_stream_read(
            stack_info.lambdas.archive_validation_lambda
        )

        assert stack_info.lambdas.archive_validation_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.archive_validation_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "When activating stream for dynamodb table. It automatically allow listStreams to all resources with a wildcard and should be suppressed",
                    "appliesTo": ["Resource::*"],
                },
            ],
        )

        assert isinstance(
            stack_info.buckets.output_bucket.node.default_child, CfnElement
        )
        output_bucket_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.buckets.output_bucket.node.default_child
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        assert isinstance(stack_info.tables.metric_table.node.default_child, CfnElement)
        metric_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.metric_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.chunk_retrieval_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier archives over to s3 in any location",
                    "appliesTo": [
                        f"Resource::<{output_bucket_logical_id}.Arn>/*",
                        "Action::s3:Abort*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for Glacier Object Retrieval table index to allow read/write",
                    "appliesTo": [
                        f"Resource::<{glacier_retrieval_table_logical_id}.Arn>/index/*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for Metric table to allow read/write",
                    "appliesTo": [
                        f"Resource::<{metric_table_logical_id}.Arn>/index/*",
                    ],
                },
            ],
        )

        assert stack_info.lambdas.archive_validation_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.archive_validation_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier inventory over to s3 in any location",
                    "appliesTo": [
                        f"Resource::<{output_bucket_logical_id}.Arn>/*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcard permission is granted to the step function to be able to write to archive download throttling table",
                    "appliesTo": [
                        f"Resource::<{glacier_retrieval_table_logical_id}.Arn>/index/*"
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.lambdas.chunk_retrieval_lambda.node.default_child, CfnElement
        )
        assert isinstance(
            stack_info.lambdas.archive_validation_lambda.node.default_child, CfnElement
        )

        NagSuppressions.add_stack_suppressions(
            stack_info.scope,
            [
                {
                    "id": "AwsSolutions-L1",
                    "reason": "Python 3.10 is the latest version, but there is a bug in cdk-nag due to sorting",
                }
            ],
        )
