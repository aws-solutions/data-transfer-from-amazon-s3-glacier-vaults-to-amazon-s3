"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from cdk_nag import NagSuppressions

from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.helpers.distributed_map import (
    DistributedMap,
    ItemReaderConfig,
    ResultConfig,
)
from solution.infrastructure.helpers.solutions_function import SolutionsPythonFunction
from solution.infrastructure.helpers.solutions_state_machine import (
    SolutionsStateMachine,
)
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo):
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier retrieval table")
        if stack_info.buckets.inventory_bucket is None:
            raise ResourceNotFound("Inventory bucket")
        if stack_info.parameters.enable_lambda_tracing_parameter is None:
            raise ResourceNotFound("Enable lambda tracing parameter")

        stack_info.lambdas.archives_needing_status_cleanup_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ArchivesNeedingStatusCleanup",
            stack_info.cfn_conditions.is_gov_cn_partition_condition,
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.archives_needing_status_cleanup",
            code=stack_info.lambda_source,
            memory_size=10240,
            timeout=Duration.minutes(15),
            description="Lambda to query dynamodb to generate a list of all archives needing status cleanup.",
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME: "staged_archives_index",
            },
        )

        archives_needing_status_cleanup_lambda_task = tasks.LambdaInvoke(
            stack_info.scope,
            "ArchivesNeedingStatusCleanupTask",
            lambda_function=stack_info.lambdas.archives_needing_status_cleanup_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "workflow_run.$": "$.workflow_run",
                    "BucketName": stack_info.buckets.inventory_bucket.bucket_name,
                }
            ),
            payload_response_only=True,
            result_path="$.archives_needing_status_cleanup",
            retry_on_service_exceptions=False,
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.lambdas.archives_needing_status_cleanup_lambda
        )

        stack_info.lambdas.cleanup_archives_status_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "CleanupArchivesStatus",
            stack_info.cfn_conditions.is_gov_cn_partition_condition,
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.cleanup_archives_status_batch",
            code=stack_info.lambda_source,
            memory_size=1536,
            timeout=Duration.minutes(15),
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        cleanup_archives_status_lambda_task = tasks.LambdaInvoke(
            stack_info.scope,
            "CleanupArchivesStatusTask",
            lambda_function=stack_info.lambdas.cleanup_archives_status_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "Items.$": "$.Items",
                },
            ),
            result_path="$.cleanup_archives_status_lambda_task_result",
            retry_on_service_exceptions=False,
        )

        stack_info.default_retry.apply_to_steps(
            [
                archives_needing_status_cleanup_lambda_task,
                cleanup_archives_status_lambda_task,
            ]
        )

        item_reader_config = ItemReaderConfig(
            item_reader_resource=f"arn:aws:states:::s3:getObject",
            reader_config={"InputType": "JSON"},
            item_reader_parameters={
                "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                "Key.$": "$.archives_needing_status_cleanup.s3_key",
            },
        )

        result_config = ResultConfig(
            result_writer={
                "Resource": f"arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                    "Prefix.$": f"States.Format('{{}}/ArchivesStatusCleanupDistributedMapOutput', $.workflow_run)",
                },
            },
            result_path="$.archives_status_cleanup_map_result",
        )

        archives_status_cleanup_map_state = DistributedMap(
            stack_info.scope,
            "ArchivesStatusCleanupDistributedMap",
            definition=cleanup_archives_status_lambda_task,
            item_reader_config=item_reader_config,
            result_config=result_config,
            max_concurrency=10,
            item_selector={
                "item.$": "$$.Map.Item.Value",
                "workflow_run.$": "$.workflow_run",
            },
            max_items_per_batch=10000,
        )

        step_function_definition = archives_needing_status_cleanup_lambda_task.next(
            archives_status_cleanup_map_state
        )

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.cleanup_archives_status_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "ArchivesStatusCleanupStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=step_function_definition,
        )

        stack_info.lambdas.cleanup_archives_status_lambda.grant_invoke(
            stack_info.state_machines.cleanup_archives_status_state_machine
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.cleanup_archives_status_lambda
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.state_machines.cleanup_archives_status_state_machine
        )

        cleanup_archives_status_state_machine_policy = iam.Policy(
            stack_info.scope,
            "CleanupArchivesStatusStateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[
                        stack_info.state_machines.cleanup_archives_status_state_machine.state_machine_arn
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:{Aws.PARTITION}:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{stack_info.state_machines.cleanup_archives_status_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        cleanup_archives_status_state_machine_policy.attach_to_role(
            stack_info.state_machines.cleanup_archives_status_state_machine.role
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.archives_needing_status_cleanup_lambda
        )

        stack_info.outputs[
            OutputKeys.ARCHIVES_STATUS_CLEANUP_STATE_MACHINE_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.ARCHIVES_STATUS_CLEANUP_STATE_MACHINE_ARN,
            value=stack_info.state_machines.cleanup_archives_status_state_machine.state_machine_arn,
        )

        assert stack_info.lambdas.cleanup_archives_status_lambda.role is not None
        cleanup_archives_status_state_machine_policy.attach_to_role(
            stack_info.lambdas.cleanup_archives_status_lambda.role
        )

        assert isinstance(
            stack_info.lambdas.cleanup_archives_status_lambda.node.default_child,
            CfnElement,
        )
        cleanup_archives_status_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.cleanup_archives_status_lambda.node.default_child
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.cleanup_archives_status_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for Glacier Object Retrieval table index to allow read/write",
                    "appliesTo": [
                        f"Resource::<{glacier_retrieval_table_logical_id}.Arn>/index/*",
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.state_machines.cleanup_archives_status_state_machine.node.default_child,
            CfnElement,
        )
        cleanup_archives_status_state_machine_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.state_machines.cleanup_archives_status_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            cleanup_archives_status_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:<AWS::Partition>:states:<AWS::Region>:<AWS::AccountId>:execution:<{cleanup_archives_status_state_machine_logical_id}.Name>/*"
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.lambdas.archives_needing_status_cleanup_lambda.node.default_child,
            CfnElement,
        )
        assert (
            stack_info.lambdas.archives_needing_status_cleanup_lambda.role is not None
        )
        archives_needing_status_cleanup_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.archives_needing_status_cleanup_lambda.node.default_child
        )

        assert isinstance(
            stack_info.buckets.inventory_bucket.node.default_child, CfnElement
        )
        inventory_bucket_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.buckets.inventory_bucket.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.cleanup_archives_status_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier archives over to s3 in any location",
                    "appliesTo": [
                        f"Resource::<{archives_needing_status_cleanup_lambda_logical_id}.Arn>:*",
                        f"Resource::<{cleanup_archives_status_lambda_logical_id}.Arn>:*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required to export the results of the Distributed Map state to S3 bucket",
                    "appliesTo": ["Action::s3:Abort*", "Action::s3:DeleteObject*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for reading a file as dataset in a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.archives_needing_status_cleanup_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcard permission is granted to the lambda function to be able to write to glacier retrieval table",
                    "appliesTo": [
                        f"Resource::<{glacier_retrieval_table_logical_id}.Arn>/index/*"
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required to export the results of the Distributed Map state to S3 bucket",
                    "appliesTo": ["Action::s3:Abort*", "Action::s3:DeleteObject*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for reading a file as dataset in a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )
