"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


from aws_cdk import Aws, CfnElement, CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_ssm as ssm
from cdk_nag import NagSuppressions

from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.ssm_automation_docs.orchestration_automation_document import (
    LaunchAutomationDocument,
    ResumeAutomationDocument,
)
from solution.infrastructure.workflows.stack_info import StackInfo

SOLUTION_NAME = "Data-Retrieval-for-Glacier-S3"


class SystemManager:
    def __init__(self, stack_info: StackInfo) -> None:
        if stack_info.state_machines.orchestrator_state_machine is None:
            raise ResourceNotFound("Orchestrator State Machine")
        if stack_info.tables.glacier_retrieval_table is None:
            raise ResourceNotFound("Glacier Retrieval Table")

        ssm_role = iam.Role(
            stack_info.scope,
            "SSMRole",
            assumed_by=iam.ServicePrincipal("ssm.amazonaws.com"),
        )
        stack_info.state_machines.orchestrator_state_machine.grant_start_execution(
            ssm_role
        )
        stack_info.tables.glacier_retrieval_table.grant_read_data(ssm_role)

        launch_automation_document = ssm.CfnDocument(
            stack_info.scope,
            "LaunchAutomationRunbook",
            content=LaunchAutomationDocument(
                ssm_role.role_arn,
                stack_info.async_facilitator_topic.topic_arn,
                stack_info.state_machines.orchestrator_state_machine.state_machine_arn,
            ).yaml_document,
            document_format="YAML",
            document_type="Automation",
            name=f"Launch-{SOLUTION_NAME}-{Aws.STACK_NAME}",
            target_type="/AWS::StepFunctions::StateMachine",
            update_method="NewVersion",
        )
        if not launch_automation_document.name:
            raise ResourceNotFound("LaunchAutomationRunbook, has not been created")

        stack_info.outputs[OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME,
            value=launch_automation_document.name,
        )

        resume_automation_document = ssm.CfnDocument(
            stack_info.scope,
            "ResumeAutomationRunbook",
            content=ResumeAutomationDocument(
                ssm_role.role_arn,
                stack_info.async_facilitator_topic.topic_arn,
                stack_info.state_machines.orchestrator_state_machine.state_machine_arn,
                stack_info.tables.glacier_retrieval_table.table_name,
            ).yaml_document,
            document_format="YAML",
            document_type="Automation",
            name=f"Resume-{SOLUTION_NAME}-{Aws.STACK_NAME}",
            target_type="/AWS::StepFunctions::StateMachine",
            update_method="NewVersion",
        )
        if not resume_automation_document.name:
            raise ResourceNotFound("ResumeAutomationRunbook, has not been created")

        stack_info.outputs[OutputKeys.SSM_RESUME_AUTOMATION_RUNBOOK_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.SSM_RESUME_AUTOMATION_RUNBOOK_NAME,
            value=resume_automation_document.name,
        )

        assert isinstance(
            stack_info.tables.glacier_retrieval_table.node.default_child, CfnElement
        )
        glacier_retrieval_table_logical_id = Stack.of(stack_info.scope).get_logical_id(
            stack_info.tables.glacier_retrieval_table.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            ssm_role.node.find_child("DefaultPolicy").node.find_child("Resource"),
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
