"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from cdk_nag import NagSuppressions

from solution.application.operational_metrics.anonymized_stats import StatsType
from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.helpers.solutions_function import SolutionsPythonFunction
from solution.infrastructure.helpers.solutions_state_machine import (
    SolutionsStateMachine,
)
from solution.infrastructure.helpers.task_catch import TaskCatch
from solution.infrastructure.helpers.task_retry import TaskRetry
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo):
        if stack_info.eventbridge_rules.extend_download_window_trigger is None:
            raise ResourceNotFound("Extend download window trigger")
        if stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger is None:
            raise ResourceNotFound("CloudWatch dashboard update trigger")
        if stack_info.eventbridge_rules.completion_checker_trigger is None:
            raise ResourceNotFound("Completion checker trigger")
        if stack_info.parameters.enable_lambda_tracing_parameter is None:
            raise ResourceNotFound("Enable Lambda Tracing Parameter")
        if stack_info.interfaces.output_bucket is None:
            raise ResourceNotFound("Output Bucket")
        if stack_info.buckets.inventory_bucket is None:
            raise ResourceNotFound("Inventory Bucket")
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier Retrieval Table")
        if stack_info.tables.metric_table is None:
            raise ResourceNotFound("Metric Table")
        if stack_info.state_machines.cloudwatch_dashboard_update_state_machine is None:
            raise ResourceNotFound("CloudWatch dashboard update state machine")
        if stack_info.lambdas.send_anonymized_stats_lambda is None:
            raise ResourceNotFound("Send Anonymized Stats Lambda")

        state_machine_start_execution_retry = TaskRetry(
            errors=["StepFunctions.AWSStepFunctionsException"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2.0,
        )
        cleanup_task = sfn.Pass(stack_info.scope, "CleanupTask")

        # Each independent task in the cleanup workflow should catch to the next task, to ensure any failures don't block the rest of the cleanup

        stack_info.lambdas.post_workflow_dashboard_update = SolutionsPythonFunction(
            stack_info.scope,
            "PostWorkflowDashboardUpdate",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.post_workflow_dashboard_update",
            code=stack_info.lambda_source,
            memory_size=10240,
            timeout=Duration.minutes(15),
            environment={
                OutputKeys.INVENTORY_BUCKET_NAME: stack_info.buckets.inventory_bucket.bucket_name,
                OutputKeys.GLACIER_RETRIEVAL_INDEX_NAME: "staged_archives_index",
                OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME: stack_info.tables.glacier_retrieval_table.table_name,
                OutputKeys.METRIC_TABLE_NAME: stack_info.tables.metric_table.table_name,
            },
        )
        stack_info.buckets.inventory_bucket.grant_read_write(
            stack_info.lambdas.post_workflow_dashboard_update
        )
        stack_info.tables.glacier_retrieval_table.grant_read_write_data(
            stack_info.lambdas.post_workflow_dashboard_update
        )
        stack_info.tables.metric_table.grant_read_write_data(
            stack_info.lambdas.post_workflow_dashboard_update
        )

        post_workflow_dashboard_update_task = tasks.LambdaInvoke(
            stack_info.scope,
            "PostWorkflowDashboardUpdateTask",
            lambda_function=stack_info.lambdas.post_workflow_dashboard_update,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object(
                {
                    "WorkflowRun.$": "$.workflow_run",
                    "BucketName": stack_info.buckets.inventory_bucket.bucket_name,
                }
            ),
            result_path="$.post_workflow_dashboard_update",
            retry_on_service_exceptions=False,
        )

        send_anonymized_stats_task = tasks.LambdaInvoke(
            stack_info.scope,
            "SendAnonymizedCompletionStatsTask",
            lambda_function=stack_info.lambdas.send_anonymized_stats_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "stats_type": StatsType.COMPLETION,
                    "workflow_run.$": "$.workflow_run",
                }
            ),
            result_path="$.send_anonymized_stats_result",
            retry_on_service_exceptions=False,
        )
        send_anonymized_stats_task_catch = TaskCatch(
            handler=cleanup_task,
            result_path="$.errors",
        )
        send_anonymized_stats_task_catch.apply_to_steps([send_anonymized_stats_task])
        stack_info.default_retry.apply_to_steps([send_anonymized_stats_task])

        remove_cloudwatch_dashboard_update_target = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:removeTargets",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger.rule_name,
                "Ids.$": "States.Array($.workflow_run)",
            },
            "Catch": TaskCatch(
                handler=post_workflow_dashboard_update_task,
                result_path="$.errors",
            ).custom_state_params(),
            "ResultPath": "$.cloudwatch_dashboard_update_target_result",
        }

        remove_cloudwatch_dashboard_update_target_state = sfn.CustomState(
            stack_info.scope,
            "RemoveCloudWatchDashboardUpdateTarget",
            state_json=remove_cloudwatch_dashboard_update_target,
        )

        cloudwatch_dashboard_update_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "StateMachineArn": stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_arn,
                "Input.$": "$$.Execution.Input",
            },
            "ResultPath": "$.cloudwatch_dashboard_update_result",
            "Retry": state_machine_start_execution_retry.custom_state_params(),
            "Catch": TaskCatch(
                handler=send_anonymized_stats_task,
                result_path="$.errors",
            ).custom_state_params(),
        }

        cloudwatch_dashboard_update_state = sfn.CustomState(
            stack_info.scope,
            "CloudwatchDashboardUpdateState",
            state_json=cloudwatch_dashboard_update_state_json,
        )

        remove_extend_download_window_target = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:removeTargets",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.extend_download_window_trigger.rule_name,
                "Ids.$": "States.Array($.workflow_run)",
            },
            "Catch": TaskCatch(
                handler=remove_cloudwatch_dashboard_update_target_state,
                result_path="$.errors",
            ).custom_state_params(),
            "ResultPath": "$.extend_download_window_target_result",
        }

        remove_extend_download_window_target_state = sfn.CustomState(
            stack_info.scope,
            "RemoveExtendDownloadWindowTarget",
            state_json=remove_extend_download_window_target,
        )

        stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda = SolutionsPythonFunction(
            stack_info.scope,
            "CleanupIncompleteMultipartUploads",
            stack_info.parameters.enable_lambda_tracing_parameter.value_as_string,
            handler="solution.application.handlers.cleanup_incomplete_multipart_uploads",
            code=stack_info.lambda_source,
            memory_size=512,
            timeout=Duration.minutes(15),
        )
        stack_info.interfaces.output_bucket.grant_read_write(
            stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda
        )

        cleanup_incomplete_multipart_uploads_task = tasks.LambdaInvoke(
            stack_info.scope,
            "CleanupIncompleteMultipartUploadsTask",
            lambda_function=stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object(
                {
                    "WorkflowRun.$": "$.workflow_run",
                    "S3DestinationBucket": stack_info.interfaces.output_bucket.bucket_name,
                }
            ),
            result_path="$.multipart_upload_cleanup",
            retry_on_service_exceptions=False,
        )

        multipart_upload_cleanup_catch = TaskCatch(
            handler=remove_extend_download_window_target_state,
            result_path="$.errors",
        )
        multipart_upload_cleanup_catch.apply_to_steps(
            [cleanup_incomplete_multipart_uploads_task]
        )
        stack_info.default_retry.apply_to_steps(
            [
                cleanup_incomplete_multipart_uploads_task,
                post_workflow_dashboard_update_task,
            ]
        )

        post_workflow_dashboard_update_catch = TaskCatch(
            handler=cloudwatch_dashboard_update_state,
            result_path="$.errors",
        )
        post_workflow_dashboard_update_catch.apply_to_steps(
            [post_workflow_dashboard_update_task]
        )

        remove_completion_checker_target = {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:removeTargets",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.completion_checker_trigger.rule_name,
                "Ids.$": "States.Array($.workflow_run)",
            },
            "Catch": TaskCatch(
                handler=cleanup_incomplete_multipart_uploads_task,
                result_path="$.errors",
            ).custom_state_params(),
            "ResultPath": "$.remove_completion_checker_target_result",
        }

        remove_completion_checker_target_state = sfn.CustomState(
            stack_info.scope,
            "RemoveCompletionCheckerTarget",
            state_json=remove_completion_checker_target,
        )

        assert isinstance(
            stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda.node.default_child,
            CfnElement,
        )
        assert isinstance(
            stack_info.lambdas.post_workflow_dashboard_update.node.default_child,
            CfnElement,
        )
        assert isinstance(
            stack_info.buckets.inventory_bucket.node.default_child, CfnElement
        )
        inventory_bucket_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.buckets.inventory_bucket.node.default_child
        )
        cleanup_incomplete_multipart_uploads_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda.node.default_child
        )
        post_workflow_dashboard_update_task_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.lambdas.post_workflow_dashboard_update.node.default_child
        )
        assert (
            stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda.role
            is not None
        )
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.cleanup_incomplete_multipart_uploads_lambda.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child(
                "Resource"
            ),
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

        assert stack_info.lambdas.post_workflow_dashboard_update.role is not None
        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.post_workflow_dashboard_update.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying failed archives over to s3 its locations",
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:Abort*",
                        "Action::s3:DeleteObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                }
            ],
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.lambdas.post_workflow_dashboard_update.role.node.find_child(
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

        remove_completion_checker_target_state.next(
            cleanup_incomplete_multipart_uploads_task
        ).next(remove_extend_download_window_target_state).next(
            remove_cloudwatch_dashboard_update_target_state
        ).next(
            post_workflow_dashboard_update_task
        ).next(
            cloudwatch_dashboard_update_state
        ).next(
            send_anonymized_stats_task
        ).next(
            cleanup_task
        )

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.cleanup_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "CleanupStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=remove_completion_checker_target_state,
        )

        executing_sfn_rule_policy = iam.Policy(
            stack_info.scope,
            "ExecutingSFNRulePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[
                        stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_arn
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        executing_sfn_rule_policy.attach_to_role(
            stack_info.state_machines.cleanup_state_machine.role
        )

        # https://docs.aws.amazon.com/step-functions/latest/dg/stepfunctions-iam.html
        step_functions_managed_rule_policy = iam.Policy(
            stack_info.scope,
            "CleanUpStepFunctionManagedRulePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "events:PutTargets",
                        "events:PutRule",
                        "events:DescribeRule",
                    ],
                    resources=[
                        f"arn:aws:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
                    ],
                ),
            ],
        )

        remove_target_policy = iam.Policy(
            stack_info.scope,
            "RemoveTargetPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "events:RemoveTargets",
                    ],
                    resources=[
                        stack_info.eventbridge_rules.extend_download_window_trigger.rule_arn,
                        stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger.rule_arn,
                        stack_info.eventbridge_rules.completion_checker_trigger.rule_arn,
                    ],
                ),
            ],
        )

        remove_target_policy.attach_to_role(
            stack_info.state_machines.cleanup_state_machine.role
        )
        step_functions_managed_rule_policy.attach_to_role(
            stack_info.state_machines.cleanup_state_machine.role
        )

        stack_info.outputs[OutputKeys.CLEANUP_STATE_MACHINE_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.CLEANUP_STATE_MACHINE_ARN,
            value=stack_info.state_machines.cleanup_state_machine.state_machine_arn,
        )

        assert isinstance(
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine.node.default_child,
            CfnElement,
        )
        cloudwatch_dashboard_update_logical_id = Stack.of(
            stack_info.scope
        ).get_logical_id(
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            executing_sfn_rule_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions to make orchestrator workflow to StartExecution for the nested workflows",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{cloudwatch_dashboard_update_logical_id}.Name>/*"
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.lambdas.send_anonymized_stats_lambda.node.default_child,
            CfnElement,
        )
        send_anonymized_stats_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.lambdas.send_anonymized_stats_lambda.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.cleanup_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Wildcard permission is granted to the step function to be able to write to archive download throttling table",
                    "appliesTo": [
                        f"Resource::<{cleanup_incomplete_multipart_uploads_logical_id}.Arn>:*",
                        f"Resource::<{post_workflow_dashboard_update_task_logical_id}.Arn>:*",
                        f"Resource::<{send_anonymized_stats_logical_id}.Arn>:*",
                    ],
                },
            ],
        )
