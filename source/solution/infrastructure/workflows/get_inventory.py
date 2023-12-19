"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk.aws_glue_alpha import Code, GlueVersion, Job, JobExecutable, PythonVersion
from cdk_nag import NagSuppressions

from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.operational_metrics.anonymized_stats import StatsType
from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.glue_helper.glue_sfn_update import GlueSfnUpdate
from solution.infrastructure.helpers.distributed_map import DistributedMap
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

        GLUE_MAX_CONCURENT_RUNS = 10
        query_inventory_metadata = tasks.DynamoGetItem(
            stack_info.scope,
            "QueryInventoryMetadata",
            key={
                "pk": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at(
                        GlacierTransferModel.format_partition_keys_for_step_function(
                            "$.workflow_run", "$.vault_name"
                        )
                    )
                ),
                "sk": tasks.DynamoAttributeValue.from_string("meta"),
            },
            table=stack_info.tables.glacier_retrieval_table,
            result_path="$.inventory_metadata_result",
        )

        stack_info.lambdas.archive_naming_override_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "ArchiveNamingOverride",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.archive_naming_overrides",
            code=stack_info.lambda_source,
            memory_size=256,
            timeout=Duration.minutes(3),
            environment={
                OutputKeys.INVENTORY_BUCKET_NAME: stack_info.buckets.inventory_bucket.bucket_name,
            },
        )
        stack_info.buckets.inventory_bucket.grant_write(
            stack_info.lambdas.archive_naming_override_lambda
        )

        stack_info.lambdas.initiate_inventory_retrieval_lambda = (
            SolutionsPythonFunction(
                stack_info.scope,
                "InitiateInventoryRetrieval",
                stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
                handler="solution.application.handlers.initiate_inventory_retrieval",
                code=stack_info.lambda_source,
                memory_size=256,
                timeout=Duration.seconds(15),
            )
        )
        stack_info.outputs[
            OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_NAME
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_NAME,
            value=stack_info.lambdas.initiate_inventory_retrieval_lambda.function_name,
        )
        stack_info.outputs[
            OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_ARN,
            value=stack_info.lambdas.initiate_inventory_retrieval_lambda.function_arn,
        )

        initiate_inventory_retrieval_lambda_policy = iam.Policy(
            stack_info.scope,
            "InitiateInventoryRetrievalLambdaPolicy",
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

        assert stack_info.lambdas.initiate_inventory_retrieval_lambda.role is not None
        initiate_inventory_retrieval_lambda_policy.attach_to_role(
            stack_info.lambdas.initiate_inventory_retrieval_lambda.role
        )

        archive_naming_override_job = tasks.LambdaInvoke(
            stack_info.scope,
            "ArchiveNamingOverrideJob",
            lambda_function=stack_info.lambdas.archive_naming_override_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "WorkflowRun.$": "$.workflow_run",
                    "NameOverridePresignedURL.$": "$.name_override_presigned_url",
                }
            ),
            result_path="$.archive_naming_override_job_result",
            retry_on_service_exceptions=False,
        )

        retrieve_inventory_initiate_job = tasks.LambdaInvoke(
            stack_info.scope,
            "RetrieveInventoryInitiateJob",
            lambda_function=stack_info.lambdas.initiate_inventory_retrieval_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "accountId": Stack.of(stack_info.scope).account,
                    "jobParameters": {
                        "Type": GlacierJobType.INVENTORY_RETRIEVAL,
                        "Format": "CSV",
                        "SNSTopic": stack_info.async_facilitator_topic.topic_arn,
                    },
                    "vaultName.$": "$.vault_name",
                },
            ),
            result_path="$.initiate_job_result",
            retry_on_service_exceptions=False,
        )

        glue_job_role = iam.Role(
            stack_info.scope,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        stack_info.tables.metric_table.grant_read_write_data(glue_job_role)
        stack_info.buckets.inventory_bucket.grant_read_write(glue_job_role)

        workflow_run_id = "workflow_run_id"
        glue_script_location = f"{workflow_run_id}/scripts/inventory_sort_script.py"
        glue_job = Job(
            stack_info.scope,
            "GlueOrderingJob",
            executable=JobExecutable.python_etl(
                glue_version=GlueVersion.V3_0,
                python_version=PythonVersion.THREE,
                script=Code.from_bucket(
                    stack_info.buckets.inventory_bucket, glue_script_location
                ),
            ),
            role=glue_job_role,
            default_arguments={
                "--workflow_run_name": "",
                "--metric_table_name": stack_info.tables.metric_table.table_name,
            },
        )

        glue_job_state_policy = iam.Policy(
            stack_info.scope,
            "GlueJobStatePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["glue:UpdateJob", "glue:StartJobRun"],
                    resources=[
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{glue_job.job_name}"
                    ],
                ),
            ],
        )

        dynamo_db_put_state_json = {
            "Type": "Task",
            "Parameters": {
                "TableName": stack_info.tables.async_facilitator_table.table_name,
                "Item": {
                    "task_token": {
                        "S.$": "$$.Task.Token",
                    },
                    "job_id": {
                        "S.$": "$.initiate_job_result.Payload.jobId",
                    },
                    "start_timestamp": {
                        "S.$": "$$.Execution.StartTime",
                    },
                },
            },
            "ResultPath": "$.async_ddb_put_result",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken",
            "TimeoutSeconds": 6 * 60 * 60,  # 6 Hours
            "Retry": stack_info.default_retry.custom_state_params(),
        }

        dynamo_db_put = sfn.CustomState(
            stack_info.scope,
            "AsyncFacilitatorDynamoDBPut",
            state_json=dynamo_db_put_state_json,
        )

        stack_info.lambdas.inventory_chunk_determination_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "InventoryChunkDetermination",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.inventory_chunking",
            code=stack_info.lambda_source,
            description="Lambda to generate the correct byte offsets to retrieve the inventory.",
        )

        generate_chunk_array_lambda = tasks.LambdaInvoke(
            stack_info.scope,
            "GenerateChunkArrayLambda",
            lambda_function=stack_info.lambdas.inventory_chunk_determination_lambda,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object(
                {
                    "TotalSize.$": "$.async_ddb_put_result.InventorySizeInBytes",
                }
            ),
            result_path="$.chunking_result",
            retry_on_service_exceptions=False,
        )

        stack_info.outputs[
            OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN,
            value=stack_info.lambdas.inventory_chunk_determination_lambda.function_name,
        )

        initiate_s3_multipart_upload = tasks.CallAwsService(
            stack_info.scope,
            "InitiateInventoryMultipartUpload",
            service="S3",
            action="createMultipartUpload",
            iam_resources=[stack_info.buckets.inventory_bucket.bucket_arn],
            parameters={
                "Bucket": stack_info.buckets.inventory_bucket.bucket_name,
                "Key.$": "States.Format('{}/original_inventory/inventory.csv', $.workflow_run)",
                "ChecksumAlgorithm": "SHA256",
            },
            result_path="$.multipart_upload_result",
        )

        dynamo_db_put_upload_id = tasks.CallAwsService(
            stack_info.scope,
            "PutInventoryMultipartUploadMetadata",
            service="DynamoDB",
            action="putItem",
            iam_resources=[stack_info.tables.glacier_retrieval_table.table_arn],
            parameters={
                "TableName": stack_info.tables.glacier_retrieval_table.table_name,
                "Item": {
                    "pk": {
                        "S.$": "States.Format('{}|{}', $.workflow_run, $.vault_name)"
                    },
                    "sk": {"S": "meta"},
                    "job_id": {
                        "S.$": "$.initiate_job_result.Payload.jobId",
                    },
                    "staged_job_id": {
                        "S.$": "$.initiate_job_result.Payload.jobId",
                    },
                    "start_time": {
                        "S.$": "$$.Execution.StartTime",
                    },
                    "retrieval_type": {"S": GlacierJobType.INVENTORY_RETRIEVAL},
                    "file_name": {
                        "S.$": "$.vault_name",
                    },
                    "description": {
                        "S.$": "$.description",
                    },
                    "vault_name": {
                        "S.$": "$.vault_name",
                    },
                    "size": {
                        "N.$": "States.JsonToString($.async_ddb_put_result.InventorySizeInBytes)",
                    },
                    "upload_id": {"S.$": "$.multipart_upload_result.UploadId"},
                    "retrieve_status": {
                        "S.$": f"States.Format('{{}}/{GlacierTransferModel.StatusCode.STAGED}', $.workflow_run)"
                    },
                    "s3_storage_class": {"S.$": "$.s3_storage_class"},
                    "s3_destination_bucket": {
                        "S": stack_info.buckets.inventory_bucket.bucket_name
                    },
                    "s3_destination_key": {
                        "S.$": "States.Format('{}/original_inventory/inventory.csv', $.workflow_run)"
                    },
                    "chunks_count": {
                        "N.$": "States.Format('{}', States.ArrayLength($.chunking_result.body))"
                    },
                },
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        stack_info.lambdas.inventory_retrieval_lambda_function = SolutionsPythonFunction(
            stack_info.scope,
            "InventoryChunkDownload",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.inventory_retrieval",
            code=stack_info.lambda_source,
            memory_size=1536,
            timeout=Duration.minutes(15),
            description="Lambda to download inventory chunks from Glacier.",
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        stack_info.policies.get_job_output_policy = iam.Policy(
            stack_info.scope,
            "GetJobOutputPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["glacier:GetJobOutput"],
                    resources=[
                        f"arn:aws:glacier:{Aws.REGION}:{Aws.ACCOUNT_ID}:vaults/*",
                    ],
                ),
            ],
        )

        stack_info.buckets.inventory_bucket.grant_put(
            stack_info.lambdas.inventory_retrieval_lambda_function
        )

        inventory_retrieval_lambda = tasks.LambdaInvoke(
            stack_info.scope,
            "InventoryChunkDownloadLambda",
            lambda_function=stack_info.lambdas.inventory_retrieval_lambda_function,
            payload_response_only=True,
            retry_on_service_exceptions=False,
        )

        stack_info.outputs[
            OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_NAME
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_NAME,
            value=stack_info.lambdas.inventory_retrieval_lambda_function.function_name,
        )

        stack_info.default_retry.apply_to_steps([inventory_retrieval_lambda])
        retrieve_inventory_map_retry = TaskRetry(
            errors=["States.All"],
            interval=Duration.seconds(30),
            max_attempts=5,
            backoff_rate=5.0,
        )

        distributed_map_state = DistributedMap(
            stack_info.scope,
            "InventoryChunkRetrievalDistributedMap",
            definition=inventory_retrieval_lambda,
            items_path="$.chunking_result.body",
            item_selector={
                "JobId.$": "$.initiate_job_result.Payload.jobId",
                "VaultName.$": "$.vault_name",
                "ByteRange.$": "$$.Map.Item.Value",
                "S3DestinationBucket": stack_info.buckets.inventory_bucket.bucket_name,
                "S3DestinationKey.$": "States.Format('{}/original_inventory/inventory.csv', $.workflow_run)",
                "GlacierObjectId.$": "$.vault_name",
                "UploadId.$": "$.multipart_upload_result.UploadId",
                "PartNumber.$": "States.MathAdd($$.Map.Item.Index, 1)",
                "WorkflowRun.$": "$.workflow_run",
            },
            result_path="$.upload_part_result",
            retry=retrieve_inventory_map_retry.custom_state_params(),
        )

        glue_sfn_update = GlueSfnUpdate(
            stack_info.scope,
            stack_info.buckets.inventory_bucket.bucket_name,
            stack_info.buckets.inventory_bucket.bucket_arn,
            glue_job.job_name,
            glue_job.role.role_arn,
            stack_info.tables.metric_table.table_name,
            glue_max_concurent_runs=GLUE_MAX_CONCURENT_RUNS,
        )

        glue_autogenerate_etl_script = glue_sfn_update.autogenerate_etl_script()
        glue_start_job = glue_sfn_update.start_job()
        stack_info.default_retry.apply_to_steps(
            [glue_autogenerate_etl_script, glue_start_job]
        )

        glue_order_archives = glue_autogenerate_etl_script.next(glue_start_job)

        stack_info.lambdas.inventory_validation_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "InventoryValidation",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.inventory_validation",
            code=stack_info.lambda_source,
            description="Lambda to validate and complete the multipart inventory upload.",
            timeout=Duration.seconds(15),
            environment={
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
            },
        )

        stack_info.outputs[OutputKeys.INVENTORY_VALIDATION_LAMBDA_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.INVENTORY_VALIDATION_LAMBDA_ARN,
            value=stack_info.lambdas.inventory_validation_lambda.function_name,
        )

        inventory_validation_lambda_task = tasks.LambdaInvoke(
            stack_info.scope,
            "InventoryValidationLambdaTask",
            lambda_function=stack_info.lambdas.inventory_validation_lambda,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object(
                {
                    "WorkflowRun.$": "$.workflow_run",
                    "GlacierObjectId.$": "$.vault_name",
                }
            ),
            result_path="$.validate_lambda",
            retry_on_service_exceptions=False,
        )

        assert isinstance(
            stack_info.lambdas.inventory_validation_lambda.node.default_child,
            CfnElement,
        )
        inventory_validation_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.inventory_validation_lambda.node.default_child
        )

        update_inventory_metadata = tasks.DynamoUpdateItem(
            stack_info.scope,
            "UpdateInventoryMetadata",
            table=stack_info.tables.glacier_retrieval_table,
            key={
                "pk": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at(
                        GlacierTransferModel.format_partition_keys_for_step_function(
                            "$.workflow_run", "$.vault_name"
                        )
                    )
                ),
                "sk": tasks.DynamoAttributeValue.from_string("meta"),
            },
            expression_attribute_values={
                ":rs": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at(
                        f"States.Format('{{}}/{GlacierTransferModel.StatusCode.DOWNLOADED}', $.workflow_run)"
                    )
                ),
            },
            update_expression="SET retrieve_status = :rs",
            result_path="$.update_inventory_metadata_result",
        )

        stack_info.lambdas.send_anonymized_stats_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "SendAnonymizedStats",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.anonymized_stats",
            code=stack_info.lambda_source,
            description="Lambda to send pre archives copying anonymized stats.",
            timeout=Duration.seconds(15),
            environment={
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                "ACCOUNT_ID": Aws.ACCOUNT_ID,
                "REGION": Aws.REGION,
                "VERSION": stack_info.scope.node.try_get_context("SOLUTION_VERSION")
                or "v1.0.0",
                "SOLUTION_ID": stack_info.scope.node.try_get_context("SOLUTION_ID")
                or "SO0293",
                "SEND_ANONYMIZED_STATISTICS": "Yes",
            },
        )

        send_anonymized_stats_task = tasks.LambdaInvoke(
            stack_info.scope,
            "SendAnonymizedSubmissionStatsTask",
            lambda_function=stack_info.lambdas.send_anonymized_stats_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "stats_type": StatsType.SUBMISSION,
                    "workflow_run.$": "$.workflow_run",
                }
            ),
            retry_on_service_exceptions=False,
        )

        get_inventory_success = sfn.Succeed(stack_info.scope, "GetInventorySucceed")

        send_anonymized_stats_task.add_catch(
            get_inventory_success,
            errors=["States.ALL"],
            result_path="$.send_anonymized_stats_task_error",
        )

        stack_info.tables.metric_table.grant_read_data(
            stack_info.lambdas.send_anonymized_stats_lambda
        )

        stack_info.tables.glacier_retrieval_table.grant_read_data(
            stack_info.lambdas.send_anonymized_stats_lambda
        )

        stack_info.default_retry.apply_to_steps(
            [
                archive_naming_override_job,
                query_inventory_metadata,
                retrieve_inventory_initiate_job,
                generate_chunk_array_lambda,
                initiate_s3_multipart_upload,
                dynamo_db_put_upload_id,
                inventory_validation_lambda_task,
                update_inventory_metadata,
                send_anonymized_stats_task,
            ]
        )

        inventory_already_downloaded_choice = (
            sfn.Choice(stack_info.scope, "InventoryAlreadyDownloaded?")
            .when(
                sfn.Condition.and_(
                    sfn.Condition.is_present(
                        "$.inventory_metadata_result.Item.retrieve_status"
                    ),
                    sfn.Condition.string_matches(
                        "$.inventory_metadata_result.Item.retrieve_status.S",
                        f"*/{GlacierTransferModel.StatusCode.DOWNLOADED}",
                    ),
                ),
                glue_order_archives,
            )
            .otherwise(retrieve_inventory_initiate_job)
        )

        query_inventory_metadata.next(inventory_already_downloaded_choice)

        retrieve_inventory_initiate_job.next(dynamo_db_put).next(
            generate_chunk_array_lambda
        ).next(initiate_s3_multipart_upload).next(dynamo_db_put_upload_id).next(
            distributed_map_state
        ).next(
            inventory_validation_lambda_task
        ).next(
            update_inventory_metadata
        ).next(
            glue_order_archives
        ).next(
            send_anonymized_stats_task
        ).next(
            get_inventory_success
        )

        definition = (
            sfn.Choice(stack_info.scope, "Provided Inventory?")
            .when(
                sfn.Condition.string_equals("$.provided_inventory", "YES"),
                glue_order_archives,
            )
            .otherwise(query_inventory_metadata)
        )

        archive_naming_override_job.next(
            definition
        )  # Always check for naming overrides

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.inventory_retrieval_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "InventoryRetrievalStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=archive_naming_override_job,
        )

        stack_info.lambdas.inventory_retrieval_lambda_function.grant_invoke(
            stack_info.state_machines.inventory_retrieval_state_machine
        )

        stack_info.lambdas.initiate_inventory_retrieval_lambda.grant_invoke(
            stack_info.state_machines.inventory_retrieval_state_machine
        )

        stack_info.lambdas.archive_naming_override_lambda.grant_invoke(
            stack_info.state_machines.inventory_retrieval_state_machine
        )

        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.state_machines.inventory_retrieval_state_machine
        )
        glue_job_state_policy.attach_to_role(
            stack_info.state_machines.inventory_retrieval_state_machine.role
        )
        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.state_machines.inventory_retrieval_state_machine
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.state_machines.inventory_retrieval_state_machine
        )
        stack_info.tables.metric_table.grant_write_data(
            stack_info.state_machines.inventory_retrieval_state_machine
        )

        stack_info.outputs[
            OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN
        ] = CfnOutput(
            stack_info.scope,
            OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN,
            value=stack_info.state_machines.inventory_retrieval_state_machine.state_machine_arn,
        )

        assert isinstance(
            stack_info.lambdas.inventory_retrieval_lambda_function.node.default_child,
            CfnElement,
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.inventory_retrieval_lambda_function
        )

        inventory_retrieval_state_machine_policy = iam.Policy(
            stack_info.scope,
            "InventoryRetrievalStateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[
                        stack_info.state_machines.inventory_retrieval_state_machine.state_machine_arn
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{stack_info.state_machines.inventory_retrieval_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        inventory_retrieval_state_machine_policy.attach_to_role(
            stack_info.state_machines.inventory_retrieval_state_machine.role
        )

        assert isinstance(
            stack_info.buckets.inventory_bucket.node.default_child, CfnElement
        )
        inventory_bucket_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.buckets.inventory_bucket.node.default_child
        )
        assert stack_info.lambdas.archive_naming_override_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.archive_naming_override_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier naming override files to our inventory bucket",
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                    ],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            initiate_inventory_retrieval_lambda_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for inventory retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:aws:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*"
                    ],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_job,
            [
                {
                    "id": "AwsSolutions-GL1",
                    "reason": "The Glue job does not use a security configuration with CloudWatch Log encryption enabled. Will be addressed later",
                },
                {
                    "id": "AwsSolutions-GL3",
                    "reason": "The Glue job does not use a security configuration with job bookmark encryption enabled. Will be addressed later",
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_job_role,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "By default Put object is not provided by the glue job default role. ",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSGlueServiceRole"
                    ],
                }
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_job_role.node.find_child("DefaultPolicy").node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Automatically the gluejob default added these wildcarded values. Hence the wildcard needs to suppressed.",
                    "appliesTo": [
                        "Action::s3:GetObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:List*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Making sure that we can put various objects in the bucket. Output and the script should be store in this bucket ",
                    "appliesTo": [f"Resource::<{inventory_bucket_logical_id}.Arn>/*"],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.policies.get_job_output_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for inventory retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:aws:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*",
                    ],
                }
            ],
        )

        assert stack_info.lambdas.inventory_retrieval_lambda_function.role is not None
        stack_info.policies.get_job_output_policy.attach_to_role(
            stack_info.lambdas.inventory_retrieval_lambda_function.role
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )
        _suppression_reason_iam5 = (
            "Allow for copying glacier inventory over to s3 in any location"
        )
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.inventory_retrieval_lambda_function.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": _suppression_reason_iam5,
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
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
            ],
        )

        assert stack_info.lambdas.inventory_validation_lambda.role is not None
        stack_info.tables.glacier_retrieval_table.grant_read_data(
            stack_info.lambdas.inventory_validation_lambda
        )
        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.lambdas.inventory_validation_lambda
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.inventory_validation_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": _suppression_reason_iam5,
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
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
            stack_info.lambdas.inventory_validation_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": _suppression_reason_iam5,
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>:*",
                        f"",
                    ],
                }
            ],
        )

        inventory_retrieval_lambda_function_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.inventory_retrieval_lambda_function.node.default_child
        )

        assert isinstance(
            stack_info.lambdas.initiate_inventory_retrieval_lambda.node.default_child,
            CfnElement,
        )
        initiate_inventory_retrieval_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.initiate_inventory_retrieval_lambda.node.default_child
        )

        assert isinstance(
            stack_info.lambdas.archive_naming_override_lambda.node.default_child,
            CfnElement,
        )
        archive_naming_override_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.archive_naming_override_lambda.node.default_child
        )

        assert isinstance(
            stack_info.lambdas.send_anonymized_stats_lambda.node.default_child,
            CfnElement,
        )
        send_anonymized_stats_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.send_anonymized_stats_lambda.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "By default wildcard permission is granted to the state machine from various sources.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        f"Resource::<{inventory_retrieval_lambda_function_logical_id}.Arn>:*",
                        f"Resource::<{initiate_inventory_retrieval_lambda_logical_id}.Arn>:*",
                        f"Resource::<{archive_naming_override_lambda_logical_id}.Arn>:*",
                        f"Resource::<{send_anonymized_stats_lambda_logical_id}.Arn>:*",
                    ],
                }
            ],
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcard permissions needed to allow Inventory Retrieval workflow to list sorted bucket contents to generate partitions list.",
                    "appliesTo": [
                        "Action::s3:DeleteObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                }
            ],
        )
        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for Glacier Object Retrieval table index to allow read/write",
                    "appliesTo": [
                        f"Resource::<{glacier_retrieval_table_logical_id}.Arn>/index/*"
                    ],
                }
            ],
        )

        assert isinstance(
            stack_info.state_machines.inventory_retrieval_state_machine.node.default_child,
            CfnElement,
        )
        inventory_retrieval_state_machine_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.state_machines.inventory_retrieval_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            inventory_retrieval_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{inventory_retrieval_state_machine_logical_id}.Name>/*"
                    ],
                }
            ],
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "By default wildcard permission is granted to the lambda.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        f"Resource::<{inventory_validation_lambda_logical_id}.Arn>:*"
                    ],
                }
            ],
        )

        assert isinstance(
            stack_info.lambdas.inventory_chunk_determination_lambda.node.default_child,
            CfnElement,
        )
        inventory_chunk_determination_lambda_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.inventory_chunk_determination_lambda.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "By default wildcard permission is granted to the lambda.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        f"Resource::<{inventory_chunk_determination_lambda_logical_id}.Arn>:*",
                        "Action::s3:Abort*",
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                    ],
                }
            ],
        )

        assert stack_info.lambdas.send_anonymized_stats_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.send_anonymized_stats_lambda.role.node.find_child(
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
