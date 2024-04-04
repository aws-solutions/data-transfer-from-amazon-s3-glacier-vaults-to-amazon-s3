"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import os
from typing import Any, Dict, List

import boto3
import pytest
from botocore.exceptions import ClientError
from mypy_boto3_cloudformation import CloudFormationClient
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_events.client import EventBridgeClient
from mypy_boto3_iam import IAMClient
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_logs import CloudWatchLogsClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_sns import SNSClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient

from solution.infrastructure.output_keys import OutputKeys

if "AWS_PROFILE" in os.environ and "AWS_REGION" in os.environ:
    session = boto3.Session(
        profile_name=os.environ["AWS_PROFILE"],
        region_name=os.environ["AWS_REGION"],
    )
else:
    session = boto3.Session()

iam_client: IAMClient = session.client("iam")
lambdas_to_update_env_variables = [
    OutputKeys.CHUNK_RETRIEVAL_LAMBDA_NAME,
    OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_NAME,
    OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_NAME,
    OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA,
    OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_NAME,
]
lambdas_to_update_invoke_permissions = [
    OutputKeys.INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_ARN,
    OutputKeys.EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA_ARN,
    OutputKeys.INITIATE_INVENTORY_RETRIEVAL_LAMBDA_ARN,
]


@pytest.fixture(scope="module")
def sfn_client() -> SFNClient:
    client: SFNClient = session.client("stepfunctions")
    return client


@pytest.fixture(scope="module")
def sns_client() -> SNSClient:
    client: SNSClient = session.client("sns")
    return client


@pytest.fixture(scope="module")
def ddb_client() -> DynamoDBClient:
    client: DynamoDBClient = session.client("dynamodb")
    return client


@pytest.fixture(scope="module")
def s3_client() -> S3Client:
    client: S3Client = session.client("s3")
    return client


@pytest.fixture(scope="module")
def ssm_client() -> SSMClient:
    client: SSMClient = session.client("ssm")
    return client


@pytest.fixture(scope="package")
def eventbridge_client() -> EventBridgeClient:
    client: EventBridgeClient = session.client("events")
    return client


@pytest.fixture(scope="package")
def lambda_client() -> LambdaClient:
    client: LambdaClient = session.client("lambda")
    return client


@pytest.fixture(scope="package")
def logs_client() -> CloudWatchLogsClient:
    client: CloudWatchLogsClient = session.client("logs")
    return client


@pytest.fixture(autouse=True, scope="package")
def set_up_environment(
    lambda_client: LambdaClient, eventbridge_client: EventBridgeClient
) -> Any:
    solution_stack_details = _get_stack_outputs(stack_name=os.environ["STACK_NAME"])
    assert "Outputs" in solution_stack_details
    _set_env_from_cfn_outputs(solution_stack_details["Outputs"])

    mock_sns_stack_details = _get_stack_outputs(
        stack_name=os.environ["MOCK_SNS_STACK_NAME"]
    )
    assert "Outputs" in mock_sns_stack_details
    _set_env_from_cfn_outputs(mock_sns_stack_details["Outputs"])
    _setup_mock_lambda_env(lambda_client)
    _setup_lambda_permissions_to_invoke(lambda_client)
    _setup_mock_sns_notify_lambda_permissions(lambda_client)
    yield
    _cleanup_mock_sns_notify_lambda_permissions(lambda_client)
    _cleanup_lambda_permissions_to_invoke(lambda_client)
    _cleanup_mock_lambda_env(lambda_client)
    _cleanup_eventbridge_targets(eventbridge_client)


def _get_stack_outputs(stack_name: str) -> Dict:  # type: ignore
    client: CloudFormationClient = session.client("cloudformation")
    return client.describe_stacks(StackName=stack_name)["Stacks"][0]  # type: ignore


def _set_env_from_cfn_outputs(cfn_outputs: List[Dict]) -> None:  # type: ignore
    for output in cfn_outputs:
        os.environ[output["OutputKey"]] = output["OutputValue"]


def _setup_mock_lambda_env(lambda_client: LambdaClient) -> None:
    for key in lambdas_to_update_env_variables:
        existing_env_variables = {}
        lambda_configuration = lambda_client.get_function_configuration(
            FunctionName=os.environ[key]
        )
        if lambda_configuration and "Environment" in lambda_configuration:
            existing_env_variables = lambda_configuration["Environment"]["Variables"]

        lambda_client.update_function_configuration(
            FunctionName=os.environ[key],
            Environment={
                "Variables": {
                    **existing_env_variables,
                    "MockGlacier": "True",  # MockGlacier variable set to True to indicate integration tests
                    "MOCK_NOTIFY_SNS_LAMBDA_ARN": os.environ[
                        OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN
                    ],
                }
            },
        )


def _cleanup_mock_lambda_env(lambda_client: LambdaClient) -> None:
    for key in lambdas_to_update_env_variables:
        env_variables = {}
        lambda_configuration = lambda_client.get_function_configuration(
            FunctionName=os.environ[key]
        )
        if lambda_configuration and "Environment" in lambda_configuration:
            env_variables = lambda_configuration["Environment"]["Variables"]
            if "MockGlacier" in env_variables:
                env_variables.pop("MockGlacier")
            if "MOCK_NOTIFY_SNS_LAMBDA_ARN" in env_variables:
                env_variables.pop("MOCK_NOTIFY_SNS_LAMBDA_ARN")

        lambda_client.update_function_configuration(
            FunctionName=os.environ[key],
            Environment={
                "Variables": {
                    **env_variables,
                }
            },
        )


def _setup_lambda_permissions_to_invoke(lambda_client: LambdaClient) -> None:
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "Statement1",
                    "Effect": "Allow",
                    "Action": ["lambda:InvokeFunction"],
                    "Resource": [os.environ[OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN]],
                }
            ],
        }
    )
    for key in lambdas_to_update_invoke_permissions:
        _put_policy_on_lambda(
            function_name=os.environ[key],
            policy_name="Invoke-MockNotifySNSLambda",
            policy_document=policy_document,
            lambda_client=lambda_client,
        )


def _cleanup_lambda_permissions_to_invoke(lambda_client: LambdaClient) -> None:
    for key in lambdas_to_update_invoke_permissions:
        _remove_policy_from_lambda(
            function_name=os.environ[key],
            policy_name="Invoke-MockNotifySNSLambda",
            lambda_client=lambda_client,
        )


def _setup_mock_sns_notify_lambda_permissions(lambda_client: LambdaClient) -> None:
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "Statement1",
                    "Effect": "Allow",
                    "Action": ["sns:Publish"],
                    "Resource": [os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]],
                }
            ],
        }
    )
    _put_policy_on_lambda(
        function_name=os.environ[OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN],
        policy_name="Publish-AsyncFacilitatorTopic",
        policy_document=policy_document,
        lambda_client=lambda_client,
    )


def _cleanup_mock_sns_notify_lambda_permissions(lambda_client: LambdaClient) -> None:
    _remove_policy_from_lambda(
        function_name=os.environ[OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN],
        policy_name="Publish-AsyncFacilitatorTopic",
        lambda_client=lambda_client,
    )


def _cleanup_eventbridge_targets(eventbridge_client: EventBridgeClient) -> None:
    for rule in [
        os.environ[OutputKeys.CLOUDWATCH_DASHBOARD_UPDATE_RULE_NAME],
        os.environ[OutputKeys.WORKFLOW_COMPLETION_CHECKER_RULE_NAME],
        os.environ[OutputKeys.EXTEND_DOWNLOAD_WINDOW_RULE_NAME],
    ]:
        response = eventbridge_client.list_targets_by_rule(Rule=rule)
        ids = [target["Id"] for target in response["Targets"]]
        if ids:
            eventbridge_client.remove_targets(
                Rule=rule,
                Ids=[target["Id"] for target in response["Targets"]],
            )


def _put_policy_on_lambda(
    function_name: str,
    policy_name: str,
    policy_document: str,
    lambda_client: LambdaClient,
) -> None:
    function_configuration = lambda_client.get_function(FunctionName=function_name)
    if function_configuration and "Configuration" in function_configuration:
        role_name = function_configuration["Configuration"]["Role"].split("/")[-1]
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=policy_document,
        )


def _remove_policy_from_lambda(
    function_name: str, policy_name: str, lambda_client: LambdaClient
) -> None:
    function_configuration = lambda_client.get_function(FunctionName=function_name)
    try:
        if function_configuration and "Configuration" in function_configuration:
            role_name = function_configuration["Configuration"]["Role"].split("/")[-1]
            iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchEntity":
            pass
        else:
            raise error
