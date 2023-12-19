"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import functools
import os
import re
from typing import Any, Optional

import aws_cdk as cdk
from aws_cdk import Aws, Stack, Stage
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codecommit as codecommit
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import pipelines
from constructs import Construct

from solution.infrastructure.stack import SolutionStack
from solution.mocking.mock_glacier_stack import MockGlacierStack

RESOURCE_NAME_LENGTH_LIMIT = 30
SCANS = ["pip-audit", "bandit"]


class PipelineStack(Stack):
    """
    This stack establishes a pipeline that builds, deploys, and tests the solution
    in a specified account. It uses a CodeCommit repo specified by context input
    to trigger the pipeline.

    The repo is configured using context parameters, specifically the following:

       - repository_name: CodeCommit repository name
       - branch: Branch to trigger the pipeline from
       - skip_integration_tests: true or false
    """

    repository_name: str = "DataTransferFromAmazonS3GlacierVaultsToAmazonS3"
    branch: str = "main"
    skip_integration_tests: bool = False

    def __init__(self, scope: Construct, construct_id: str) -> None:
        context_repo = scope.node.try_get_context("repository_name")
        if context_repo:
            self.repository_name = context_repo
        context_branch = scope.node.try_get_context("branch")
        if context_branch:
            self.branch = context_branch
        skip_integration_tests = scope.node.try_get_context("skip_integration_tests")
        if skip_integration_tests:
            self.skip_integration_tests = skip_integration_tests.lower() == "true"

        super().__init__(
            scope,
            construct_id,
            stack_name=self.get_resource_name(f"pipeline-stack"),
            env=cdk.Environment(
                account=os.environ["CDK_DEFAULT_ACCOUNT"],
                region=os.environ["CDK_DEFAULT_REGION"],
            ),
            description=f"({scope.node.try_get_context('SOLUTION_ID')}-pipeline) - "
            f"{scope.node.try_get_context('SOLUTION_NAME')}: Pipeline Stack to create developer pipeline "
            f"for {self.repository_name} repository on {self.branch} branch - "
            f"Version: {scope.node.try_get_context('SOLUTION_VERSION')}",
        )

        cache_bucket = s3.Bucket(
            self,
            "CacheBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
        )

        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            pipeline_name=self.get_resource_name("pipeline"),
            synth=self.get_synth_step(cache_bucket),
            code_build_defaults=pipelines.CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                    compute_type=codebuild.ComputeType.LARGE,
                )
            ),
        )

        audit_wave = pipeline.add_wave("SecurityAudit")
        audit_wave.add_post(*self.get_steps_from_tox(SCANS))

        self.deploy_stage = DeployStage(
            self,
            self.get_resource_name("deploy"),
        )
        pipeline.add_stage(self.deploy_stage)

        if not self.skip_integration_tests:
            self.integration_test_stage = IntegrationTestStage(
                self, self.get_resource_name("test")
            )
            pipeline.add_stage(
                self.integration_test_stage,
                post=[self.get_integration_test_step()],
            )

    def get_resource_name(self, name: str) -> str:
        """
        Returns a name with the repo and branch appended to differentiate pipelines between branches
        """
        concatenated = re.sub(r"[^a-zA-Z0-9-]+", "", f"{name}-{self.branch}")
        checksum = functools.reduce(
            lambda a, b: a ^ b,
            bytes(f"{self.repository_name}{self.branch}", "utf-8"),
        )
        return f"{concatenated[:RESOURCE_NAME_LENGTH_LIMIT - 2]}{checksum:02x}"

    def get_connection(self) -> pipelines.CodePipelineSource:
        return pipelines.CodePipelineSource.code_commit(
            repository=codecommit.Repository.from_repository_name(
                scope=self,
                id="CodeCommitSource",
                repository_name=self.repository_name,
            ),
            branch=self.branch,
        )

    def get_synth_step(self, cache_bucket: s3.IBucket) -> pipelines.CodeBuildStep:
        return pipelines.CodeBuildStep(
            "Synth",
            input=self.get_connection(),
            env=dict(
                REPOSITORY_NAME=self.repository_name,
                BRANCH=self.branch,
                SKIP_INTEGRATION_TESTS=f"{self.skip_integration_tests}",
            ),
            install_commands=[
                'pip install ".[dev]"',
            ],
            commands=[
                "tox --recreate --parallel-no-spinner -- --junitxml=pytest-report.xml",
                "npx cdk synth -c repository_name=$REPOSITORY_NAME -c branch=$BRANCH -c skip_integration_tests=$SKIP_INTEGRATION_TESTS",
            ],
            partial_build_spec=self.get_partial_build_spec(
                dict(
                    reports=self.get_reports_build_spec_mapping("pytest-report.xml"),
                    cache=dict(
                        paths=[
                            ".mypy_cache/**/*",
                            ".tox/**/*",
                            "/root/.cache/pip/**/*",
                        ]
                    ),
                )
            ),
            cache=codebuild.Cache.bucket(cache_bucket),
        )

    def get_integration_test_step(self) -> pipelines.CodeBuildStep:
        stack_name = self.deploy_stage.stack_name
        iam_stack_name = stack_name[:25]
        mock_sns_stack_name = self.integration_test_stage.stack_name
        return pipelines.CodeBuildStep(
            "integration-test",
            install_commands=[
                "pip install tox",
            ],
            env=dict(STACK_NAME=stack_name, MOCK_SNS_STACK_NAME=mock_sns_stack_name),
            commands=["tox -e integration -- --junitxml=pytest-integration-report.xml"],
            role_policy_statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["cloudformation:DescribeStacks"],
                    resources=[
                        f"arn:aws:cloudformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:stack/{iam_stack_name}*",
                        f"arn:aws:cloudformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:stack/{mock_sns_stack_name}*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:UpdateItem",
                    ],
                    resources=[
                        f"arn:aws:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{iam_stack_name}*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "sns:Publish",
                        "sns:ListSubscriptionsByTopic",
                        "sns:AddPermission",
                        "sns:RemovePermission",
                    ],
                    resources=[
                        f"arn:aws:sns:{Aws.REGION}:{Aws.ACCOUNT_ID}:{iam_stack_name}*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:GetObject*",
                        "s3:List*",
                    ],
                    resources=[
                        "arn:aws:s3:::*-outputbucket*",
                        "arn:aws:s3:::*-inventorybucket*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:GetExecutionHistory",
                        "states:ListExecutions",
                        "states:StopExecution",
                    ],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InventoryRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InventoryRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InitiateRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InitiateRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InitiateMetadataStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InitiateMetadataStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:mapRun:InitiateMetadataStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:RetrieveArchiveStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:RetrieveArchiveStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:OrchestratorStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:OrchestratorStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:mapRun:RetrieveArchiveStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:mapRun:InitiateRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:ExtendDownloadWindowStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:ExtendDownloadWindowStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:mapRun:ExtendDownloadWindowStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:CloudWatchDashboardUpdateStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:CloudWatchDashboardUpdateStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:CleanupStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:CleanupStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:ArchivesStatusCleanupStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:ArchivesStatusCleanupStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:mapRun:ArchivesStatusCleanupStateMachine*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction",
                        "lambda:GetFunction",
                        "lambda:UpdateFunctionConfiguration",
                        "lambda:GetFunctionConfiguration",
                    ],
                    resources=[
                        f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "iam:PutRolePolicy",
                        "iam:DeleteRolePolicy",
                    ],
                    resources=[
                        f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/{mock_sns_stack_name}*",
                        f"arn:aws:iam::{Aws.ACCOUNT_ID}:role/{iam_stack_name}*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ssm:StartAutomationExecution",
                    ],
                    resources=[
                        f"arn:aws:ssm:{Aws.REGION}:{Aws.ACCOUNT_ID}:automation-definition/*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ssm:GetAutomationExecution",
                    ],
                    resources=[
                        f"arn:aws:ssm:{Aws.REGION}:{Aws.ACCOUNT_ID}:automation-execution/*"
                    ],
                ),
            ],
            partial_build_spec=self.get_partial_build_spec(
                dict(
                    reports=self.get_reports_build_spec_mapping(
                        "pytest-integration-report.xml"
                    ),
                )
            ),
        )

    def get_partial_build_spec(self, mapping: dict[str, Any]) -> codebuild.BuildSpec:
        return codebuild.BuildSpec.from_object(mapping)

    def get_reports_build_spec_mapping(self, filename: str) -> dict[str, Any]:
        return {
            "pytest_reports": {
                "files": [filename],
                "file-format": "JUNITXML",
            }
        }

    @staticmethod
    def get_steps_from_tox(tox_env: list[str]) -> list[pipelines.CodeBuildStep]:
        steps: list[pipelines.CodeBuildStep] = []
        for _env in tox_env:
            _step = pipelines.CodeBuildStep(
                _env,
                install_commands=["pip install tox"],
                commands=[
                    f"tox -e {_env}",
                ],
            )
            steps.append(_step)
        return steps


class IntegrationTestStage(Stage):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
    ) -> None:
        super().__init__(scope, construct_id)
        mock_glacier_stack = MockGlacierStack(self, "mock-glacier")
        self.stack_name = mock_glacier_stack.stack_name


class DeployStage(Stage):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Optional[cdk.Environment] = None,
    ) -> None:
        super().__init__(scope, construct_id, env=env)
        solution_stack = SolutionStack(
            self, "data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3"
        )
        self.stack_name = solution_stack.stack_name
