"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from dataclasses import fields
from typing import Any, List

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.stack import SolutionStack


@pytest.fixture(autouse=True, scope="module")
def set_cdk_env() -> None:
    os.environ["CDK_DEFAULT_ACCOUNT"] = "111111111111"
    os.environ["CDK_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def solution_name() -> str:
    return "Data-Retrieval-for-Glacier-S3"


def test_cdk_app() -> None:
    import solution.app

    solution.app.main()


def test_cdk_nag(stack: SolutionStack) -> None:
    stack_annotations = assertions.Annotations.from_stack(stack)
    stack_annotations.has_no_error("*", assertions.Match.any_value())
    stack_annotations.has_no_warning("*", assertions.Match.any_value())


def assert_resource_name_has_correct_type_and_props(
    stack: SolutionStack,
    template: assertions.Template,
    resources_list: List[str],
    cfn_type: str,
    props: Any,
) -> None:
    resources = template.find_resources(type=cfn_type, props=props)
    assert 1 == len(resources)
    assert get_logical_id(stack, resources_list) in resources


def get_logical_id(stack: SolutionStack, resources_list: List[str]) -> str:
    node = stack.node
    for resource in resources_list:
        node = node.find_child(resource).node
    cfnElement = node.default_child
    assert isinstance(cfnElement, core.CfnElement)
    return stack.get_logical_id(cfnElement)


def test_job_tracking_table_created_with_cfn_output(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["AsyncFacilitatorTable"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::DynamoDB::Table",
        props={
            "Properties": {
                "KeySchema": [
                    {
                        "AttributeName": "job_id",
                        "KeyType": "HASH",
                    }
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "job_id", "AttributeType": "S"}
                ],
            },
        },
    )

    template.has_output(
        OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
        {"Value": {"Ref": get_logical_id(stack, resources_list)}},
    )


def test_glacier_retrieval_table_created_with_cfn_output(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["GlacierObjectRetrieval"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::DynamoDB::Table",
        props={
            "Properties": {
                "KeySchema": [
                    {
                        "AttributeName": "pk",
                        "KeyType": "HASH",
                    },
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "job_id", "AttributeType": "S"},
                    {"AttributeName": "retrieve_status", "AttributeType": "S"},
                    {"AttributeName": "archive_creation_date", "AttributeType": "S"},
                ],
            },
        },
    )
    template.has_output(
        OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME,
        {"Value": {"Ref": get_logical_id(stack, resources_list)}},
    )


def test_cfn_outputs_logical_id_is_same_as_key(stack: SolutionStack) -> None:
    """
    The outputs are used to build environment variables to pass in to lambdas,
    so we need to ensure the resource name is the same as the resulting logical
    id. Outputs have non-alphanumeric characters removed, like '-', so this
    makes sure they aren't part of the resource name.
    """
    for key, output in stack.outputs.items():
        assert key == stack.get_logical_id(output)


def test_glacier_sns_topic_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["AsyncFacilitatorTopic"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::SNS::Topic",
        props={},
    )

    template.has_output(
        OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN,
        {"Value": {"Ref": logical_id}},
    )

    template.has_resource_properties(
        "AWS::SNS::TopicPolicy",
        {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": "sns:Publish",
                        "Condition": {"Bool": {"aws:SecureTransport": False}},
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Resource": {"Ref": logical_id},
                    },
                    {
                        "Action": "SNS:Publish",
                        "Condition": {
                            "StringEquals": {
                                "AWS:SourceOwner": {"Ref": "AWS::AccountId"}
                            }
                        },
                        "Effect": "Allow",
                        "Principal": {"Service": "glacier.amazonaws.com"},
                        "Resource": {"Ref": logical_id},
                    },
                ],
            },
            "Topics": [
                {
                    "Ref": logical_id,
                }
            ],
        },
    )


def test_buckets_created(stack: SolutionStack, template: assertions.Template) -> None:
    resources_list = ["BucketAccessLogs"]
    access_logs_bucket_logical_id = get_logical_id(stack, resources_list)

    resources = template.find_resources(
        type="AWS::S3::Bucket",
        props={
            "Properties": {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                    ]
                },
                "LoggingConfiguration": {
                    "DestinationBucketName": {"Ref": access_logs_bucket_logical_id},
                    "LogFilePrefix": assertions.Match().any_value(),
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "VersioningConfiguration": {"Status": "Enabled"},
            }
        },
    )
    assert 1 == len(resources)
    assert get_logical_id(stack, ["InventoryBucket"]) in resources


def test_chunk_retrieval_lambda_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["ChunkRetrieval"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "solution.application.handlers.archive_retrieval",
                "Runtime": {"Fn::If": ["GovCnCondition", "python3.11", "python3.12"]},
                "MemorySize": 1536,
                "Timeout": 900,
            },
        },
    )
    template.has_output(
        OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_archive_validation_lambda_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["ArchiveValidation"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "solution.application.handlers.archive_validation",
                "Runtime": {"Fn::If": ["GovCnCondition", "python3.11", "python3.12"]},
                "MemorySize": 256,
                "Timeout": 900,
            },
        },
    )

    template.has_output(
        OutputKeys.ARCHIVE_VALIDATION_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_inventory_chunk_determination_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["InventoryChunkDetermination"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "solution.application.handlers.inventory_chunking",
                "Runtime": {"Fn::If": ["GovCnCondition", "python3.11", "python3.12"]},
            },
        },
    )

    template.has_output(
        OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_facilitator_lambda_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    match = assertions.Match()

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Code": {
                "S3Bucket": {"Fn::Sub": match.any_value()},
                "S3Key": match.any_value(),
            },
            "Role": {
                "Fn::GetAtt": [
                    match.string_like_regexp("AsyncFacilitatorRole*"),
                    "Arn",
                ]
            },
            "Handler": "solution.application.handlers.async_facilitator",
            "MemorySize": 256,
            "Runtime": {"Fn::If": ["GovCnCondition", "python3.11", "python3.12"]},
        },
    )


def test_facilitator_lambda_with_dynamoDb_event_source(
    template: assertions.Template,
) -> None:
    match = assertions.Match()
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "FunctionName": {"Ref": match.any_value()},
        },
    )


def test_facilitator_default_policy(
    stack: SolutionStack, template: assertions.Template
) -> None:
    match = assertions.Match()
    db_resource_name = ["AsyncFacilitatorTable"]
    facilitator_table_logical_id = get_logical_id(stack, db_resource_name)
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": match.array_with(
                    [
                        {
                            "Action": "dynamodb:ListStreams",
                            "Effect": "Allow",
                            "Resource": "*",
                        },
                        {
                            "Action": [
                                "dynamodb:DescribeStream",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                            ],
                            "Effect": "Allow",
                            "Resource": {
                                "Fn::GetAtt": [
                                    facilitator_table_logical_id,
                                    "StreamArn",
                                ]
                            },
                        },
                        {
                            "Action": [
                                "dynamodb:BatchGetItem",
                                "dynamodb:BatchWriteItem",
                                "dynamodb:ConditionCheckItem",
                                "dynamodb:DeleteItem",
                                "dynamodb:DescribeTable",
                                "dynamodb:GetItem",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                                "dynamodb:PutItem",
                                "dynamodb:Query",
                                "dynamodb:Scan",
                                "dynamodb:UpdateItem",
                            ],
                            "Effect": "Allow",
                            "Resource": [
                                {
                                    "Fn::GetAtt": [
                                        facilitator_table_logical_id,
                                        "Arn",
                                    ]
                                },
                                {"Ref": "AWS::NoValue"},
                            ],
                        },
                        {
                            "Action": [
                                "states:DescribeExecution",
                                "states:SendTaskFailure",
                                "states:SendTaskSuccess",
                            ],
                            "Effect": "Allow",
                            "Resource": [
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":execution:InventoryRetrievalStateMachine*",
                                        ],
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":execution:OrchestratorStateMachine*",
                                        ],
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":execution:RetrieveArchiveStateMachine*",
                                        ],
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":stateMachine:InventoryRetrievalStateMachine*",
                                        ],
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":stateMachine:OrchestratorStateMachine*",
                                        ],
                                    ]
                                },
                                {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":states:",
                                            {"Ref": "AWS::Region"},
                                            ":",
                                            {"Ref": "AWS::AccountId"},
                                            ":stateMachine:RetrieveArchiveStateMachine*",
                                        ],
                                    ]
                                },
                            ],
                        },
                    ]
                )
            }
        },
    )


def test_glue_job_created(stack: SolutionStack, template: assertions.Template) -> None:
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    resources = template.find_resources(
        type="AWS::Glue::Job",
        props={
            "Properties": {
                "Command": {
                    "ScriptLocation": {
                        "Fn::Join": [
                            "",
                            [
                                "s3://",
                                {"Ref": inventory_bucket_logical_id},
                                "/workflow_run_id/scripts/inventory_sort_script.py",
                            ],
                        ]
                    }
                },
            }
        },
    )

    assert 1 == len(resources)


def test_glue_job_role_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    match = assertions.Match()
    template.has_resource(
        "AWS::IAM::Role",
        {
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "glue.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                },
            },
        },
    )


def test_glue_job_role_default_policy(
    stack: SolutionStack, template: assertions.Template
) -> None:
    match = assertions.Match()
    bucket_resource_name = ["InventoryBucket"]
    inventory_bucket_logical_id = get_logical_id(stack, bucket_resource_name)
    table_resource_name = ["MetricTable"]
    table_logical_id = get_logical_id(stack, table_resource_name)
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": match.array_with(
                    [
                        {
                            "Action": [
                                "dynamodb:BatchGetItem",
                                "dynamodb:BatchWriteItem",
                                "dynamodb:ConditionCheckItem",
                                "dynamodb:DeleteItem",
                                "dynamodb:DescribeTable",
                                "dynamodb:GetItem",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                                "dynamodb:PutItem",
                                "dynamodb:Query",
                                "dynamodb:Scan",
                                "dynamodb:UpdateItem",
                            ],
                            "Effect": "Allow",
                            "Resource": match.array_with(
                                [
                                    {"Fn::GetAtt": [table_logical_id, "Arn"]},
                                    {"Ref": "AWS::NoValue"},
                                ]
                            ),
                        },
                        {
                            "Action": [
                                "s3:Abort*",
                                "s3:DeleteObject*",
                                "s3:GetBucket*",
                                "s3:GetObject*",
                                "s3:List*",
                                "s3:PutObject",
                                "s3:PutObjectLegalHold",
                                "s3:PutObjectRetention",
                                "s3:PutObjectTagging",
                                "s3:PutObjectVersionTagging",
                            ],
                            "Effect": "Allow",
                            "Resource": match.array_with(
                                [
                                    {
                                        "Fn::GetAtt": [
                                            inventory_bucket_logical_id,
                                            "Arn",
                                        ]
                                    },
                                    {
                                        "Fn::Join": [
                                            "",
                                            [
                                                {
                                                    "Fn::GetAtt": [
                                                        inventory_bucket_logical_id,
                                                        "Arn",
                                                    ]
                                                },
                                                "/*",
                                            ],
                                        ]
                                    },
                                ]
                            ),
                        },
                        {
                            "Action": ["s3:GetBucket*", "s3:GetObject*", "s3:List*"],
                            "Effect": "Allow",
                            "Resource": match.array_with(
                                [
                                    {
                                        "Fn::GetAtt": [
                                            inventory_bucket_logical_id,
                                            "Arn",
                                        ]
                                    },
                                    {
                                        "Fn::Join": [
                                            "",
                                            [
                                                {
                                                    "Fn::GetAtt": [
                                                        inventory_bucket_logical_id,
                                                        "Arn",
                                                    ]
                                                },
                                                "/workflow_run_id/scripts/inventory_sort_script.py",
                                            ],
                                        ]
                                    },
                                ]
                            ),
                        },
                    ]
                )
            }
        },
    )


def test_glue_job_policy(stack: SolutionStack, template: assertions.Template) -> None:
    inventory_retrieval_state_machine_role_logical_id = get_logical_id(
        stack, ["InventoryRetrievalStateMachine", "Role"]
    )
    glue_order_job_logical_id = get_logical_id(stack, ["GlueOrderingJob"])
    resources_list = ["GlueJobStatePolicy"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::IAM::Policy",
        props={
            "Properties": {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": ["glue:StartJobRun", "glue:UpdateJob"],
                            "Effect": "Allow",
                            "Resource": {
                                "Fn::Join": [
                                    "",
                                    [
                                        "arn:",
                                        {"Ref": "AWS::Partition"},
                                        ":glue:",
                                        {"Ref": "AWS::Region"},
                                        ":",
                                        {"Ref": "AWS::AccountId"},
                                        ":job/",
                                        {"Ref": glue_order_job_logical_id},
                                    ],
                                ]
                            },
                        },
                    ]
                },
                "Roles": [
                    {"Ref": inventory_retrieval_state_machine_role_logical_id},
                ],
            },
        },
    )


def test_glue_job_logging_policy(
    stack: SolutionStack, template: assertions.Template
) -> None:
    glue_job_role_logical_id = get_logical_id(stack, ["GlueJobRole"])
    resources_list = ["GlueLoggingPolicy"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::IAM::Policy",
        props={
            "Properties": {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            "Effect": "Allow",
                            "Resource": {
                                "Fn::Join": [
                                    "",
                                    [
                                        "arn:",
                                        {"Ref": "AWS::Partition"},
                                        ":logs:*:*:*:/aws-glue/*",
                                    ],
                                ]
                            },
                        },
                    ]
                },
                "Roles": [
                    {"Ref": glue_job_role_logical_id},
                ],
            },
        },
    )


def test_orchestrator_step_function_created(
    stack: SolutionStack, template: assertions.Template
) -> None:
    resources_list = ["OrchestratorStateMachine"]
    logical_id = get_logical_id(stack, resources_list)
    # TODO add assert_resource_name_has_correct_type_and_props for StepFunctions DefinitionString

    template.has_output(
        OutputKeys.ORCHESTRATOR_STATE_MACHINE_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_cloudwatch_dashboard(
    stack: SolutionStack, template: assertions.Template
) -> None:
    match = assertions.Match()
    template.has_resource_properties(
        "AWS::CloudWatch::Dashboard",
        {
            "DashboardBody": {
                "Fn::Join": [
                    "",
                    match.any_value(),
                ]
            },
            "DashboardName": {
                "Fn::Join": [
                    "",
                    [
                        "Data-transfer-from-Amazon-S3-Glacier-to-Amazon-S3-Dashboard-",
                        {
                            "Fn::Select": [
                                2,
                                {"Fn::Split": ["/", {"Ref": "AWS::StackId"}]},
                            ]
                        },
                    ],
                ]
            },
        },
    )


def put_metric_data_policy(stack: SolutionStack, template: assertions.Template) -> None:
    resources_list = ["PutMetricDataPolicy"]
    put_metric_data_policy_logical_id = get_logical_id(stack, resources_list)
    metric_update_sfn_role_logical_id = get_logical_id(
        stack, ["MetricUpdateStateMachineRole"]
    )

    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::IAM::Policy",
        props={
            "Properties": {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": "cloudwatch:PutMetricData",
                            "Condition": {
                                "StringEquals": {
                                    "cloudwatch:namespace": "DataTransferFromAmazonS3GlacierVaultsToAmazonS3"
                                }
                            },
                            "Effect": "Allow",
                            "Resource": "*",
                        }
                    ],
                    "Version": "2012-10-17",
                },
                "PolicyName": put_metric_data_policy_logical_id,
                "Roles": [{"Ref": metric_update_sfn_role_logical_id}],
            },
        },
    )


@pytest.mark.parametrize(
    "stack_info_resource,cfn_resource_name",
    [
        ("lambdas", "AWS::Lambda::Function"),
        ("tables", "AWS::DynamoDB::Table"),
        ("buckets", "AWS::S3::Bucket"),
        ("state_machines", "AWS::StepFunctions::StateMachine"),
        ("eventbridge_rules", "AWS::Events::Rule"),
    ],
    ids=["lambdas", "tables", "buckets", "state_machines", "eventbridge_rules"],
)
def test_stack_info_contents(
    stack: SolutionStack,
    template: assertions.Template,
    stack_info_resource: str,
    cfn_resource_name: str,
) -> None:
    resource = getattr(stack.stack_info, stack_info_resource)
    assert len(vars(resource)) == len(template.find_resources(type=cfn_resource_name))
    for field in fields(resource):
        assert getattr(resource, field.name) is not None


def test_stack_info_parameters(
    stack: SolutionStack,
    template: assertions.Template,
) -> None:
    parameters = getattr(stack.stack_info, "parameters")
    # BootstrapVersion parameter is added by default
    assert len(vars(parameters)) == len(template.find_parameters("*")) - 1
    for field in fields(parameters):
        assert getattr(parameters, field.name) is not None


def test_ssm_document_created(
    template: assertions.Template, solution_name: str
) -> None:
    template.has_resource_properties(
        type="AWS::SSM::Document",
        props={
            "Content": {
                "Fn::Join": [
                    "",
                    assertions.Match.any_value(),
                ]
            },
            "DocumentFormat": "YAML",
            "DocumentType": "Automation",
            "Name": {
                "Fn::Join": [
                    "",
                    [f"Launch-{solution_name}-", {"Ref": "AWS::StackName"}],
                ]
            },
            "TargetType": "/AWS::StepFunctions::StateMachine",
        },
    )


def test_app_registry_application(
    app: core.App, stack: SolutionStack, template: assertions.Template
) -> None:
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::Application",
        {
            "Name": {
                "Fn::Join": [
                    "-",
                    [
                        app.node.try_get_context("APP_REGISTRY_NAME"),
                        {
                            "Fn::Select": [
                                2,
                                {"Fn::Split": ["/", {"Ref": "AWS::StackId"}]},
                            ]
                        },
                        {"Ref": "AWS::Region"},
                        {"Ref": "AWS::AccountId"},
                    ],
                ]
            },
            "Description": "Service Catalog application to track and manage all your resources for the solution %%SOLUTION_NAME%%",
            "Tags": {
                "Solutions:ApplicationType": "AWS-Solutions",
                "Solutions:SolutionID": "SO0140",
                "Solutions:SolutionName": "%%SOLUTION_NAME%%",
                "Solutions:SolutionVersion": "%%VERSION%%",
            },
        },
    )


def test_app_registry_resource_association(
    stack: SolutionStack, template: assertions.Template
) -> None:
    application_resources_list = ["AppRegistryAspect", "RegistrySetup"]
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::ResourceAssociation",
        {
            "Application": {
                "Fn::GetAtt": [get_logical_id(stack, application_resources_list), "Id"]
            },
            "Resource": {"Ref": "AWS::StackId"},
            "ResourceType": "CFN_STACK",
        },
    )


def test_app_registry_attr_group_association(
    stack: SolutionStack, template: assertions.Template
) -> None:
    application_resources_list = ["AppRegistryAspect", "RegistrySetup"]
    attr_group_resources_list = ["AppRegistryAspect", "AppAttributes"]
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::AttributeGroupAssociation",
        {
            "Application": {
                "Fn::GetAtt": [get_logical_id(stack, application_resources_list), "Id"]
            },
            "AttributeGroup": {
                "Fn::GetAtt": [get_logical_id(stack, attr_group_resources_list), "Id"]
            },
        },
    )


def test_sqs_queues_encrypted(
    stack: SolutionStack, template: assertions.Template
) -> None:
    template.all_resources_properties("AWS::SQS::Queue", {"SqsManagedSseEnabled": True})


def test_logs_insights_query(
    stack: SolutionStack, template: assertions.Template
) -> None:
    lambdas_logical_ids = [
        get_logical_id(stack, ["InitiateArchiveRetrieval"]),
        get_logical_id(stack, ["NotificationsProcessor"]),
        get_logical_id(stack, ["ChunkRetrieval"]),
        get_logical_id(stack, ["ArchiveValidation"]),
        get_logical_id(stack, ["MetricsProcessor"]),
        get_logical_id(stack, ["ArchivesNeedingWindowExtension"]),
        get_logical_id(stack, ["ExtendDownloadInitiateRetrieval"]),
    ]

    Log_group_names = [
        {"Fn::Join": ["", ["/aws/lambda/", {"Ref": lambda_logical_id}]]}
        for lambda_logical_id in lambdas_logical_ids
    ]

    template.has_resource_properties(
        "AWS::Logs::QueryDefinition",
        {
            "LogGroupNames": Log_group_names,
            "Name": {
                "Fn::Join": [
                    "",
                    [
                        "LambdasErrorQuery-",
                        {
                            "Fn::Select": [
                                2,
                                {"Fn::Split": ["/", {"Ref": "AWS::StackId"}]},
                            ]
                        },
                    ],
                ]
            },
            "QueryString": "fields @timestamp, @message, @logStream, @log         | filter @message like /error/ or @message like /Error/ or @message like /ERROR/         | filter @message not like /segmentation fault Runtime/         | filter @message not like /TransactionConflict/         | sort by @timestamp desc",
        },
    )

    for status in (
        GlacierTransferModel.StatusCode.REQUESTED,
        GlacierTransferModel.StatusCode.STAGED,
        GlacierTransferModel.StatusCode.DOWNLOADED,
    ):
        template.has_resource_properties(
            "AWS::Logs::QueryDefinition",
            {
                "LogGroupNames": [
                    {
                        "Fn::Join": [
                            "",
                            [
                                "/aws/lambda/",
                                {"Ref": get_logical_id(stack, ["MetricsProcessor"])},
                            ],
                        ]
                    }
                ],
                "Name": {
                    "Fn::Join": [
                        "",
                        [
                            f"{status}CounterQuery-",
                            {
                                "Fn::Select": [
                                    2,
                                    {"Fn::Split": ["/", {"Ref": "AWS::StackId"}]},
                                ]
                            },
                        ],
                    ]
                },
                "QueryString": f"fields @timestamp, @message, @logStream, @log         | filter @message like /counted_status:{status}/         | parse @message '[INFO]\t*\t*\tArchive:*|* - counted_status:{status}' as parsed_time, parsed_id, parsed_workflow, parsed_archive         | stats count_distinct(parsed_archive) as unique_downloaded_archives by parsed_workflow",
            },
        )
