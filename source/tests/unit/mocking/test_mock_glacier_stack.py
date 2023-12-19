"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING

import aws_cdk as core
import aws_cdk.assertions as assertions
import cdk_nag
import pytest

from solution.mocking.mock_glacier_stack import MockGlacierStack


@pytest.fixture
def stack() -> MockGlacierStack:
    app = core.App()
    stack = MockGlacierStack(app, "mockGlacierStack")
    core.Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True)
    )
    return stack


@pytest.fixture
def template(stack: MockGlacierStack) -> assertions.Template:
    return assertions.Template.from_stack(stack)
