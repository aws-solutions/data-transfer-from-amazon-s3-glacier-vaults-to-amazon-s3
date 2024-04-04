"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from cdk_nag import NagSuppressions

from solution.application.model.partition_metric_record import PartitionMetricRecord
from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.helpers.distributed_map import (
    DistributedMap,
    ItemReaderConfig,
    ResultConfig,
)
from solution.infrastructure.helpers.nested_distributed_map import NestedDistributedMap
from solution.infrastructure.helpers.solutions_function import SolutionsPythonFunction
from solution.infrastructure.helpers.solutions_state_machine import (
    SolutionsStateMachine,
)
from solution.infrastructure.helpers.task_retry import TaskRetry
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo):
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier retrieval table")
        if stack_info.tables.async_facilitator_table is None:
            raise ResourceNotFound("Async facilitator table")
        if stack_info.tables.metric_table is None:
            raise ResourceNotFound("Metric table")
        if stack_info.buckets.inventory_bucket is None:
            raise ResourceNotFound("Inventory bucket")
        if stack_info.parameters.enable_lambda_tracing_parameter is None:
            raise ResourceNotFound("Enable lambda tracing parameter")

        stack_info.lambdas.initiate_archive_retrieval_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "InitiateArchiveRetrieval",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.initiate_archive_retrieval_batch",
            code=stack_info.lambda_source,
            runtime=lambda_.Runtime.PYTHON_3_10,
            memory_size=1769,
            timeout=Duration.minutes(15),
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                OutputKeys.ASYNC_FACILITATOR_TABLE_NAME: stack_info.tables.async_facilitator_table.table_name,
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
            },
        )

        stack_info.outputs[
            OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_ARN,
            value=stack_info.lambdas.initiate_archive_retrieval_lambda.function_arn,
        )

        stack_info.outputs[
            OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_NAME
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_NAME,
            value=stack_info.lambdas.initiate_archive_retrieval_lambda.function_name,
        )

        initiate_archive_retrieval_lambda_policy = iam.Policy(
            stack_info.scope,
            "InitiateArchiveRetrievalLambdaPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glacier:InitiateJob",
                    ],
                    resources=[
                        f"arn:aws:glacier:{Aws.REGION}:{Aws.ACCOUNT_ID}:vaults/*"
                    ],
                ),
            ],
        )

        assert stack_info.lambdas.initiate_archive_retrieval_lambda.role is not None
        initiate_archive_retrieval_lambda_policy.attach_to_role(
            stack_info.lambdas.initiate_archive_retrieval_lambda.role
        )

        retrieve_archive_initiate_job = tasks.LambdaInvoke(
            stack_info.scope,
            "RetrieveArchiveInitiateJob",
            lambda_function=stack_info.lambdas.initiate_archive_retrieval_lambda,
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
        stack_info.default_retry.apply_to_steps([retrieve_archive_initiate_job])

        reset_partitions_metadata = tasks.DynamoUpdateItem(
            stack_info.scope,
            "ResetPartitionsMetadata",
            table=stack_info.tables.metric_table,
            key={
                "pk": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at(
                        PartitionMetricRecord.format_partition_keys_for_step_function(
                            "$.workflow_run"
                        )
                    )
                )
            },
            expression_attribute_values={
                ":as": tasks.DynamoAttributeValue.from_number(0),
                ":ac": tasks.DynamoAttributeValue.from_number(0),
                ":st": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.Execution.StartTime")
                ),
            },
            update_expression="SET archives_size = :as, archives_count = :ac, start_time = :st",
            result_path="$.reset_partitions_metadata_result",
        )

        item_selector = {
            "workflow_run.$": "$.workflow_run",
            "vault_name.$": "$.vault_name",
            "description.$": "$.description",
            "tier.$": "$.tier",
            "s3_storage_class.$": "$.s3_storage_class",
        }

        inner_map_retry = TaskRetry(
            errors=["States.All"],
            interval=Duration.seconds(600),
            max_attempts=20,
            backoff_rate=1.5,
        )
        initiate_retrieval_distributed_map = NestedDistributedMap(
            scope=stack_info.scope,
            nested_distributed_map_id="InitiateRetrieval",
            definition=retrieve_archive_initiate_job,
            item_selector=item_selector,
            inventory_bucket=stack_info.buckets.inventory_bucket,
            max_concurrency=1,
            inner_max_concurrency=1,
            max_items_per_batch=100,
            retry=inner_map_retry.custom_state_params(),
        )

        list_inventory_partitions = tasks.CallAwsService(
            stack_info.scope,
            "ListInventoryPartitions",
            service="s3",
            action="listObjectsV2",
            parameters={
                "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                "Prefix": sfn.JsonPath.string_at(
                    "States.Format('{}/sorted_inventory/', $.workflow_run)"
                ),
                "Delimiter": "/",
            },
            iam_resources=[stack_info.buckets.inventory_bucket.bucket_arn],
            result_path="$.partitions",
        )

        stack_info.lambdas.calculate_timeout_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "CalculateTimeout",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.initiation_timeout",
            code=stack_info.lambda_source,
            runtime=lambda_.Runtime.PYTHON_3_10,
            memory_size=256,
            timeout=Duration.minutes(1),
            environment={
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.calculate_timeout_lambda
        )

        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.calculate_timeout_lambda
        )

        calculate_timeout_task = tasks.LambdaInvoke(
            stack_info.scope,
            "CalculateTimeoutTask",
            lambda_function=stack_info.lambdas.calculate_timeout_lambda,
            result_path="$.timeout",
            retry_on_service_exceptions=False,
        )
        stack_info.default_retry.apply_to_steps([calculate_timeout_task])

        wait_state = sfn.Wait(
            stack_info.scope,
            "Timeout",
            time=sfn.WaitTime.seconds_path("$.timeout.Payload.wait_sec"),
        )

        definition = (
            reset_partitions_metadata.next(
                initiate_retrieval_distributed_map.distributed_map_state
            )
            .next(calculate_timeout_task)
            .next(wait_state)
        )

        item_reader_config = ItemReaderConfig()
        result_config = ResultConfig(
            result_path="$.partitions_map_result",
        )

        partitions_distributed_map_state = DistributedMap(
            stack_info.scope,
            "PartitionsDistributedMap",
            definition=definition,
            item_reader_config=item_reader_config,
            result_config=result_config,
            max_concurrency=1,
            items_path="$.partitions.CommonPrefixes",
            item_selector={
                "workflow_run.$": "$.workflow_run",
                "prefix.$": "$$.Map.Item.Value.Prefix",
                "vault_name.$": "$.vault_name",
                "Input.$": "$$.Execution.Input",
                "description.$": "$$.Execution.Input.description",
                "tier.$": "$$.Execution.Input.tier",
                "s3_storage_class.$": "$.s3_storage_class",
                "bucket": stack_info.buckets.inventory_bucket.bucket_name,
            },
        )

        stack_info.default_retry.apply_to_steps([list_inventory_partitions])

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.initiate_retrieval_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "InitiateRetrievalStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=list_inventory_partitions.next(partitions_distributed_map_state),
        )

        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.state_machines.initiate_retrieval_state_machine
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.state_machines.initiate_retrieval_state_machine
        )

        stack_info.lambdas.calculate_timeout_lambda.grant_invoke(
            stack_info.state_machines.initiate_retrieval_state_machine
        )
        stack_info.lambdas.initiate_archive_retrieval_lambda.grant_invoke(
            stack_info.state_machines.initiate_retrieval_state_machine
        )
        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.lambdas.initiate_archive_retrieval_lambda
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.initiate_archive_retrieval_lambda
        )
        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.initiate_archive_retrieval_lambda
        )

        stack_info.outputs[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN,
            value=stack_info.state_machines.initiate_retrieval_state_machine.state_machine_arn,
        )

        initiate_retrieval_distributed_map.configure_step_function(
            stack_info.scope,
            "InitiateRetrieval",
            stack_info.state_machines.initiate_retrieval_state_machine,
            stack_info.buckets.inventory_bucket,
        )

        assert isinstance(
            stack_info.lambdas.initiate_archive_retrieval_lambda.node.default_child,
            CfnElement,
        )
        initiate_archive_retrieval_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.initiate_archive_retrieval_lambda.node.default_child
        )

        assert isinstance(
            stack_info.lambdas.calculate_timeout_lambda.node.default_child,
            CfnElement,
        )
        calculate_timeout_lambda_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.lambdas.calculate_timeout_lambda.node.default_child
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child,
            CfnElement,
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            initiate_archive_retrieval_lambda_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for archive retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:aws:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*"
                    ],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.initiate_retrieval_state_machine.role.node.find_child(
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
                        f"Resource::<{initiate_archive_retrieval_lambda_logical_id}.Arn>:*",
                        f"Resource::<{calculate_timeout_lambda_logical_id}.Arn>:*",
                    ],
                },
            ],
        )

        assert stack_info.lambdas.calculate_timeout_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.calculate_timeout_lambda.role.node.find_child(
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

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.initiate_archive_retrieval_lambda.role.node.find_child(
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
