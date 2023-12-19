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
from mypy_boto3_iam import IAMClient
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_sns import SNSClient

from solution.infrastructure.output_keys import OutputKeys

lambda_client: LambdaClient = boto3.client("lambda")
sns_client: SNSClient = boto3.client("sns")
iam_client: IAMClient = boto3.client("iam")
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


@pytest.fixture(autouse=True, scope="package")
def set_up_environment() -> Any:
    solution_stack_details = _get_stack_outputs(stack_name=os.environ["STACK_NAME"])
    assert "Outputs" in solution_stack_details
    _set_env_from_cfn_outputs(solution_stack_details["Outputs"])

    mock_sns_stack_details = _get_stack_outputs(
        stack_name=os.environ["MOCK_SNS_STACK_NAME"]
    )
    assert "Outputs" in mock_sns_stack_details
    _set_env_from_cfn_outputs(mock_sns_stack_details["Outputs"])
    _setup_mock_lambda_env()
    _setup_lambda_permissions_to_invoke()
    _setup_mock_sns_notify_lambda_permissions()
    yield
    _cleanup_mock_sns_notify_lambda_permissions()
    _cleanup_lambda_permissions_to_invoke()
    _cleanup_mock_lambda_env()


def _get_stack_outputs(stack_name: str) -> Dict:  # type: ignore
    client: CloudFormationClient = boto3.client("cloudformation")
    return client.describe_stacks(StackName=stack_name)["Stacks"][0]  # type: ignore


def _set_env_from_cfn_outputs(cfn_outputs: List[Dict]) -> None:  # type: ignore
    for output in cfn_outputs:
        os.environ[output["OutputKey"]] = output["OutputValue"]


def _setup_mock_lambda_env() -> None:
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


def _cleanup_mock_lambda_env() -> None:
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


def _setup_lambda_permissions_to_invoke() -> None:
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
        )


def _cleanup_lambda_permissions_to_invoke() -> None:
    for key in lambdas_to_update_invoke_permissions:
        _remove_policy_from_lambda(
            function_name=os.environ[key], policy_name="Invoke-MockNotifySNSLambda"
        )


def _setup_mock_sns_notify_lambda_permissions() -> None:
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
    )


def _cleanup_mock_sns_notify_lambda_permissions() -> None:
    _remove_policy_from_lambda(
        function_name=os.environ[OutputKeys.MOCK_NOTIFY_SNS_LAMBDA_ARN],
        policy_name="Publish-AsyncFacilitatorTopic",
    )


def _put_policy_on_lambda(
    function_name: str, policy_name: str, policy_document: str
) -> None:
    function_configuration = lambda_client.get_function(FunctionName=function_name)
    if function_configuration and "Configuration" in function_configuration:
        role_name = function_configuration["Configuration"]["Role"].split("/")[-1]
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=policy_document,
        )


def _remove_policy_from_lambda(function_name: str, policy_name: str) -> None:
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
