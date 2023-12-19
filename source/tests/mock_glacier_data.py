"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os

import pytest

from solution.application.mocking.mock_glacier_generator import (
    generate_mock_glacier_data,
)


def test_glacier_mock_data_not_modified(aws_regions: None) -> None:
    generated_lines = generate_mock_glacier_data().splitlines()
    with open("source/solution/application/mocking/mock_glacier_data.py") as f:
        for line_number, line in enumerate(f):
            assert line.rstrip("\n") == generated_lines[line_number]


@pytest.fixture
def aws_regions() -> None:
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"
