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
from solution.infrastructure.helpers.distributed_map import DistributedMap
from solution.infrastructure.helpers.solutions_state_machine import (
    SolutionsStateMachine,
)
from solution.infrastructure.helpers.task_catch import TaskCatch
from solution.infrastructure.helpers.task_retry import TaskRetry
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class Workflow:
    def __init__(self, stack_info: StackInfo):
        DAILY_QUOTA = 100 * 2**40

        if stack_info.state_machines.cleanup_state_machine is None:
            raise ResourceNotFound("Cleanup state machine")
        if stack_info.state_machines.cleanup_archives_status_state_machine is None:
            raise ResourceNotFound("Archives status cleanup state machine")
        if stack_info.state_machines.inventory_retrieval_state_machine is None:
            raise ResourceNotFound("Inventory retrieval state machine")
        if stack_info.state_machines.initiate_retrieval_state_machine is None:
            raise ResourceNotFound("Initiate retrieval state machine")
        if stack_info.state_machines.extend_download_window_state_machine is None:
            raise ResourceNotFound("Extend download window state machine")
        if stack_info.eventbridge_rules.extend_download_window_trigger is None:
            raise ResourceNotFound("Extend download window trigger")
        if stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger is None:
            raise ResourceNotFound("CloudWatch dashboard update trigger")
        if stack_info.eventbridge_rules.completion_checker_trigger is None:
            raise ResourceNotFound("Completion checker trigger")
        if stack_info.state_machines.cloudwatch_dashboard_update_state_machine is None:
            raise ResourceNotFound("CloudWatch dashboard update state machine")
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier retrieval table")
        if stack_info.lambdas.completion_checker_lambda is None:
            raise ResourceNotFound("Completion checker lambda")
        if stack_info.tables.async_facilitator_table is None:
            raise ResourceNotFound("Async Facilitator table")

        state_machine_start_execution_retry = TaskRetry(
            errors=["StepFunctions.AWSStepFunctionsException"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2.0,
        )

        cleanup_workflow_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "Input.$": "$$.Execution.Input",
                "StateMachineArn": stack_info.state_machines.cleanup_state_machine.state_machine_arn,
            },
            "Retry": state_machine_start_execution_retry.custom_state_params(),
            "End": True,
        }

        cleanup_workflow_state = sfn.CustomState(
            stack_info.scope, "CleanupWorkflow", state_json=cleanup_workflow_state_json
        )

        cleanup_catch = TaskCatch(
            handler=cleanup_workflow_state, result_path="$.errors"
        )

        put_workflow_input_into_ddb = tasks.DynamoPutItem(
            stack_info.scope,
            "PutWorkflowInputIntoDDB",
            table=stack_info.tables.glacier_retrieval_table,
            item={
                "pk": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.workflow_run")
                ),
                "sk": tasks.DynamoAttributeValue.from_string("meta"),
                "vault_name": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.vault_name")
                ),
                "start_time": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$$.Execution.StartTime")
                ),
                "daily_quota": tasks.DynamoAttributeValue.from_number(DAILY_QUOTA),
                "storage_class": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.s3_storage_class")
                ),
                "retrieval_tier": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("$.tier")
                ),
            },
            result_path="$.put_workflow_input_into_ddb",
        )

        status_cleanup_workflow_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "Input.$": "$$.Execution.Input",
                "StateMachineArn": stack_info.state_machines.cleanup_archives_status_state_machine.state_machine_arn,
            },
            "ResultPath": sfn.JsonPath.DISCARD,
            "Retry": state_machine_start_execution_retry.custom_state_params(),
            "Catch": cleanup_catch.custom_state_params(),
        }

        archives_status_cleanup_workflow_state = sfn.CustomState(
            stack_info.scope,
            "ArchivesStatusCleanupWorkflow",
            state_json=status_cleanup_workflow_state_json,
        )

        inventory_retrieval_workflow_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "StateMachineArn": stack_info.state_machines.inventory_retrieval_state_machine.state_machine_arn,
                "Input.$": "$$.Execution.Input",
            },
            "ResultPath": sfn.JsonPath.DISCARD,
            "Retry": state_machine_start_execution_retry.custom_state_params(),
            "Catch": cleanup_catch.custom_state_params(),
        }

        inventory_retrieval_workflow_state = sfn.CustomState(
            stack_info.scope,
            "InventoryRetrievalWorkflow",
            state_json=inventory_retrieval_workflow_state_json,
        )

        initiate_retrieval_workflow_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync:2",
            "Parameters": {
                "Input.$": "$$.Execution.Input",
                "StateMachineArn": stack_info.state_machines.initiate_retrieval_state_machine.state_machine_arn,
            },
            "ResultPath": sfn.JsonPath.DISCARD,
            "Retry": state_machine_start_execution_retry.custom_state_params(),
            "Catch": cleanup_catch.custom_state_params(),
        }

        initiate_retrieval_workflow_state = sfn.CustomState(
            stack_info.scope,
            "InitiateRetrievalWorkflow",
            state_json=initiate_retrieval_workflow_state_json,
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
                        "S.$": "States.Format('{}|COMPLETION', $.workflow_run)",
                    },
                    "start_timestamp": {
                        "S.$": "$$.Execution.StartTime",
                    },
                },
            },
            "ResultPath": "$.async_ddb_put_result",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken",
            "TimeoutSeconds": 48 * 60 * 60,
            "Retry": [
                {
                    "ErrorEquals": ["States.Timeout"],
                    "MaxAttempts": 0,
                },
                {
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 3,
                    "BackoffRate": 2,
                },
            ],
            "Catch": cleanup_catch.custom_state_params(),
        }

        wait_completion = sfn.CustomState(
            stack_info.scope,
            "WaitWorkflowCompletion",
            state_json=dynamo_db_put_state_json,
        )

        eventbridge_invoke_role = iam.Role(
            stack_info.scope,
            "EventbridgeInvokeRole",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
            inline_policies={
                "EventbrigeInvokePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["states:StartExecution"],
                            resources=[
                                stack_info.state_machines.extend_download_window_state_machine.state_machine_arn,
                                stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_arn,
                            ],
                        )
                    ]
                )
            },
        )

        put_extend_download_window_target = {
            "Type": "Task",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.extend_download_window_trigger.rule_name,
                "Targets": [
                    {
                        "RoleArn": eventbridge_invoke_role.role_arn,
                        "Arn": stack_info.state_machines.extend_download_window_state_machine.state_machine_arn,
                        "Id.$": "$.workflow_run",
                        "Input.$": "$",
                    }
                ],
            },
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:putTargets",
            "ResultPath": "$.put_target_result",
            "Catch": cleanup_catch.custom_state_params(),
        }

        put_extend_download_window_target_state = sfn.CustomState(
            stack_info.scope,
            "PutExtendDownloadWindowTarget",
            state_json=put_extend_download_window_target,
        )

        cloudwatch_dashboard_update_state_machine_target = {
            "Type": "Task",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger.rule_name,
                "Targets": [
                    {
                        "RoleArn": eventbridge_invoke_role.role_arn,
                        "Arn": stack_info.state_machines.cloudwatch_dashboard_update_state_machine.state_machine_arn,
                        "Id.$": "$.workflow_run",
                        "Input.$": "$",
                    }
                ],
            },
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:putTargets",
            "ResultPath": "$.post_metrics_sfn_target_result",
            "Catch": cleanup_catch.custom_state_params(),
        }

        cloudwatch_dashboard_update_state_machine_target_state = sfn.CustomState(
            stack_info.scope,
            "PostMetricStateMachineTarget",
            state_json=cloudwatch_dashboard_update_state_machine_target,
        )

        put_completion_checker_target = {
            "Type": "Task",
            "Parameters": {
                "Rule": stack_info.eventbridge_rules.completion_checker_trigger.rule_name,
                "Targets": [
                    {
                        "Arn": stack_info.lambdas.completion_checker_lambda.function_arn,
                        "Id.$": "$.workflow_run",
                        "Input.$": "$",
                    }
                ],
            },
            "Resource": "arn:aws:states:::aws-sdk:eventbridge:putTargets",
            "ResultPath": "$.put_completion_checker_target_result",
            "Catch": cleanup_catch.custom_state_params(),
        }

        put_completion_checker_target_state = sfn.CustomState(
            stack_info.scope,
            "PutCompletionCheckerTarget",
            state_json=put_completion_checker_target,
        )

        stack_info.lambdas.completion_checker_lambda.grant_invoke(
            iam.ServicePrincipal("events.amazonaws.com")
        )

        # https://docs.aws.amazon.com/step-functions/latest/dg/stepfunctions-iam.html
        step_functions_managed_rule_policy = iam.Policy(
            stack_info.scope,
            "StepFunctionManagedRulePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "events:PutTargets",
                        "events:RemoveTargets",
                        "events:PutRule",
                        "events:DescribeRule",
                    ],
                    resources=[
                        f"arn:aws:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule",
                        stack_info.eventbridge_rules.extend_download_window_trigger.rule_arn,
                        stack_info.eventbridge_rules.cloudwatch_dashboard_update_trigger.rule_arn,
                        stack_info.eventbridge_rules.completion_checker_trigger.rule_arn,
                    ],
                ),
            ],
        )

        assert stack_info.parameters.enable_step_function_logging_parameter is not None
        assert stack_info.parameters.enable_step_function_tracing_parameter is not None
        stack_info.state_machines.orchestrator_state_machine = SolutionsStateMachine(
            stack_info.scope,
            "OrchestratorStateMachine",
            stack_info.parameters.enable_step_function_logging_parameter.value_as_string,
            stack_info.parameters.enable_step_function_tracing_parameter.value_as_string,
            definition=put_workflow_input_into_ddb.next(
                archives_status_cleanup_workflow_state
            )
            .next(inventory_retrieval_workflow_state)
            .next(put_extend_download_window_target_state)
            .next(cloudwatch_dashboard_update_state_machine_target_state)
            .next(initiate_retrieval_workflow_state)
            .next(put_completion_checker_target_state)
            .next(wait_completion)
            .next(cleanup_workflow_state),
        )

        stack_info.tables.async_facilitator_table.grant_read_write_data(
            stack_info.state_machines.orchestrator_state_machine
        )

        stack_info.tables.glacier_retrieval_table.grant_read_data(
            stack_info.state_machines.orchestrator_state_machine
        )

        eventbridge_invoke_role.grant_pass_role(
            stack_info.state_machines.orchestrator_state_machine.role
        )

        step_functions_managed_rule_policy.attach_to_role(
            stack_info.state_machines.orchestrator_state_machine.role
        )

        state_machines_list = [
            stack_info.state_machines.orchestrator_state_machine,
            stack_info.state_machines.inventory_retrieval_state_machine,
            stack_info.state_machines.initiate_retrieval_state_machine,
            stack_info.state_machines.cleanup_state_machine,
            stack_info.state_machines.extend_download_window_state_machine,
            stack_info.state_machines.cloudwatch_dashboard_update_state_machine,
            stack_info.state_machines.cleanup_archives_status_state_machine,
        ]

        step_functions_execution_rule_policy = iam.Policy(
            stack_info.scope,
            "StepFunctionExecutionRulePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[
                        state_machine.state_machine_arn
                        for state_machine in state_machines_list
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{state_machine.state_machine_name}/*"
                        for state_machine in state_machines_list
                    ],
                ),
            ],
        )

        stack_info.outputs[OutputKeys.ORCHESTRATOR_STATE_MACHINE_ARN] = CfnOutput(
            stack_info.scope,
            OutputKeys.ORCHESTRATOR_STATE_MACHINE_ARN,
            value=stack_info.state_machines.orchestrator_state_machine.state_machine_arn,
        )

        step_functions_execution_rule_policy.attach_to_role(
            stack_info.state_machines.orchestrator_state_machine.role
        )

        state_machines_logical_ids_list = []
        for state_machine in state_machines_list:
            assert isinstance(state_machine.node.default_child, CfnElement)
            logical_id = Stack.of(stack_info.scope).get_logical_id(
                state_machine.node.default_child
            )
            state_machines_logical_ids_list.append(logical_id)

        NagSuppressions.add_resource_suppressions(
            step_functions_execution_rule_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions to make orchestrator workflow to StartExecution for the nested workflows",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{state_machine_logical_id}.Name>/*"
                        for state_machine_logical_id in state_machines_logical_ids_list
                    ],
                },
            ],
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            stack_info.state_machines.orchestrator_state_machine.role.node.find_child(
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
