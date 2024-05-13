"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as eventbridge
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
        if stack_info.tables.async_facilitator_table is None:
            raise ResourceNotFound("Async faciliatator table")
        if stack_info.buckets.inventory_bucket is None:
            raise ResourceNotFound("Inventory bucket")
        if stack_info.lambdas.async_facilitator_lambda is None:
            raise ResourceNotFound("Async facilitator lambda")
        if stack_info.parameters.enable_lambda_tracing_parameter is None:
            raise ResourceNotFound("Enable lambda tracing parameter")

        stack_info.eventbridge_rules.extend_download_window_trigger = eventbridge.Rule(
            stack_info.scope,
            "ExtendDownloadWindowTrigger",
            schedule=eventbridge.Schedule.expression("cron(0/30 * * * ? *)"),
        )

        stack_info.outputs[OutputKeys.EXTEND_DOWNLOAD_WINDOW_RULE_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_RULE_NAME,
            value=stack_info.eventbridge_rules.extend_download_window_trigger.rule_name,
        )

        glacier_retrieval_index_name = "staged_archives_index"
        stack_info.tables.glacier_retrieval_table.add_global_secondary_index(
            index_name=glacier_retrieval_index_name,
            partition_key=dynamodb.Attribute(
                name="retrieve_status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="archive_creation_date", type=dynamodb.AttributeType.STRING
            ),
        )

        stack_info.outputs[OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME,
            value=glacier_retrieval_index_name,
        )

        stack_info.lambdas.archives_needing_window_extension_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ArchivesNeedingWindowExtension",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.archives_needing_window_extension",
            code=stack_info.lambda_source,
            timeout=Duration.seconds(15),
            description="Lambda to query dynamodb to generate a list of all archives needing download window extension.",
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME: glacier_retrieval_index_name,
            },
        )

        archives_needing_window_extension_lambda_task = tasks.LambdaInvoke(
            stack_info.scope,
            "ArchivesNeedingWindowExtensionTask",
            lambda_function=stack_info.lambdas.archives_needing_window_extension_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "workflow_run.$": "$.workflow_run",
                    "tier.$": "$.tier",
                    "vault_name.$": "$.vault_name",
                    "s3_storage_class.$": "$.s3_storage_class",
                    "BucketName": stack_info.buckets.inventory_bucket.bucket_name,
                }
            ),
            payload_response_only=True,
            result_path="$.archives_needing_window_extension",
            retry_on_service_exceptions=False,
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.lambdas.archives_needing_window_extension_lambda
        )

        stack_info.lambdas.extend_download_window_initiate_retrieval_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ExtendDownloadInitiateRetrieval",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.extend_archive_retrieval_batch",
            code=stack_info.lambda_source,
            memory_size=1769,
            timeout=Duration.minutes(15),
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                OutputKeys.ASYNC_FACILITATOR_TABLE_NAME: stack_info.tables.async_facilitator_table.table_name,
            },
        )

        stack_info.outputs[
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA,
            value=stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.function_name,
        )
        stack_info.outputs[
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA_ARN,
            value=stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.function_arn,
        )

        extend_download_window_initiate_archive_retrieval_lambda_policy = iam.Policy(
            stack_info.scope,
            "ExtendDownloadInitiateRetrievalLambdaPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glacier:InitiateJob",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:glacier:{Aws.REGION}:{Aws.ACCOUNT_ID}:vaults/*"
                    ],
                ),
            ],
        )

        extend_download_window_initiate_job = tasks.LambdaInvoke(
            stack_info.scope,
            "ExtendDownloadWindowInitiateJob",
            lambda_function=stack_info.lambdas.extend_download_window_initiate_retrieval_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "AccountId": Stack.of(stack_info.scope).account,
                    "SNSTopic": stack_info.async_facilitator_topic.topic_arn,
                    "Items.$": "$.Items",
                },
            ),
            result_path="$.initiate_job_result",
            retry_on_service_exceptions=False,
        )
        stack_info.default_retry.apply_to_steps([extend_download_window_initiate_job])

        item_reader_config = ItemReaderConfig(
            item_reader_resource=f"arn:aws:states:::s3:getObject",
            reader_config={"InputType": "JSON"},
            item_reader_parameters={
                "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                "Key.$": "$.archives_needing_window_extension.s3_key",
            },
        )

        result_config = ResultConfig(
            result_writer={
                "Resource": f"arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                    "Prefix.$": f"States.Format('{{}}/ExtendDownloadWindowDistributedMapOutput', $.workflow_run)",
                },
            },
            result_path="$.extend_download_window_map_result",
        )

        extend_download_window_distributed_map_state = DistributedMap(
            stack_info.scope,
            "ExtendDownloadWindowDistributedMap",
            definition=extend_download_window_initiate_job,
            item_reader_config=item_reader_config,
            result_config=result_config,
            item_selector={
                "item.$": "$$.Map.Item.Value",
                "tier.$": "$.tier",
                "vault_name.$": "$.vault_name",
                "workflow_run.$": "$.workflow_run",
            },
            max_items_per_batch=100,
        )

        step_function_definition = archives_needing_window_extension_lambda_task.next(
            extend_download_window_distributed_map_state
        )

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.extend_download_window_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "ExtendDownloadWindowStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=step_function_definition,
        )

        stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.grant_invoke(
            stack_info.state_machines.extend_download_window_state_machine
        )
        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.state_machines.extend_download_window_state_machine
        )

        extend_download_window_state_machine_policy = iam.Policy(
            stack_info.scope,
            "ExtendDownloadWindowStateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[
                        stack_info.state_machines.extend_download_window_state_machine.state_machine_arn
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:{Aws.PARTITION}:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{stack_info.state_machines.extend_download_window_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        extend_download_window_state_machine_policy.attach_to_role(
            stack_info.state_machines.extend_download_window_state_machine.role
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.archives_needing_window_extension_lambda
        )

        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.state_machines.extend_download_window_state_machine
        )

        stack_info.outputs[
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_STATE_MACHINE_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.EXTEND_DOWNLOAD_WINDOW_STATE_MACHINE_ARN,
            value=stack_info.state_machines.extend_download_window_state_machine.state_machine_arn,
        )

        NagSuppressions.add_resource_suppressions(
            extend_download_window_initiate_archive_retrieval_lambda_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for archive retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:<AWS::Partition>:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*"
                    ],
                },
            ],
        )

        assert (
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.role
            is not None
        )
        extend_download_window_initiate_archive_retrieval_lambda_policy.attach_to_role(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.role
        )

        assert isinstance(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.node.default_child,
            CfnElement,
        )
        extend_download_window_initiate_retrieval_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.node.default_child
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
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
            stack_info.state_machines.extend_download_window_state_machine.node.default_child,
            CfnElement,
        )
        extend_download_window_state_machine_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.state_machines.extend_download_window_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            extend_download_window_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:<AWS::Partition>:states:<AWS::Region>:<AWS::AccountId>:execution:<{extend_download_window_state_machine_logical_id}.Name>/*"
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.lambdas.archives_needing_window_extension_lambda.node.default_child,
            CfnElement,
        )
        assert (
            stack_info.lambdas.archives_needing_window_extension_lambda.role is not None
        )
        archives_needing_window_extension_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.archives_needing_window_extension_lambda.node.default_child
        )

        assert isinstance(
            stack_info.buckets.inventory_bucket.node.default_child, CfnElement
        )
        inventory_bucket_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.buckets.inventory_bucket.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.extend_download_window_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier archives over to s3 in any location",
                    "appliesTo": [
                        f"Resource::<{archives_needing_window_extension_lambda_logical_id}.Arn>:*",
                        f"Resource::<{extend_download_window_initiate_retrieval_lambda_logical_id}.Arn>:*",
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
            stack_info.lambdas.archives_needing_window_extension_lambda.role.node.find_child(
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
