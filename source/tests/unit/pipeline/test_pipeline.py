"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from solution.pipeline.stack import RESOURCE_NAME_LENGTH_LIMIT, PipelineStack


@pytest.fixture(autouse=True, scope="module")
def set_cdk_env() -> None:
    os.environ["CDK_DEFAULT_ACCOUNT"] = "111111111111"
    os.environ["CDK_DEFAULT_REGION"] = "us-east-1"


def test_pipeline_is_created() -> None:
    app = core.App()
    stack = PipelineStack(app, "pipeline")
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 1)


@pytest.mark.parametrize(
    "branch,name",
    [
        ("develop", "test-resource"),
        ("main", "test-resource"),
        ("branch-name-that-is-really-long", "test-resource"),
        ("branch_name_that_includes_underscores", "test-feature"),
        ("a/slash/name", "a/b/c"),
    ],
)
def test_pipeline_name_generation_is_not_too_long(branch: str, name: str) -> None:
    app = core.App(context=dict(branch=branch))
    stack = PipelineStack(app, "pipeline")
    assert len(stack.stack_name) <= RESOURCE_NAME_LENGTH_LIMIT
    resource_name = stack.get_resource_name(name)
    assert len(resource_name) <= RESOURCE_NAME_LENGTH_LIMIT
