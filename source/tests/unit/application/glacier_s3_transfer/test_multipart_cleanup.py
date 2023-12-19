"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from typing import TYPE_CHECKING, Generator

import pytest

from solution.application.glacier_s3_transfer.multipart_cleanup import MultipartCleanup

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


@pytest.fixture
def open_multipart_uploads(
    s3_client: S3Client, num_uploads: int, workflow_run: str, bucket_name: str
) -> Generator[None, None, None]:
    """
    Fixture to set up and tear down test resources.
    """
    s3_client.create_bucket(Bucket=bucket_name)
    for i in range(num_uploads):
        s3_client.create_multipart_upload(Bucket=bucket_name, Key=f"{workflow_run}/{i}")
    s3_client.create_multipart_upload(
        Bucket=bucket_name, Key=f"Other{workflow_run}/test"
    )
    yield
    s3_client.delete_bucket(Bucket=bucket_name)


@pytest.mark.parametrize(
    "num_uploads, workflow_run, bucket_name, expected_deletions",
    [
        (10, "test_workflow_run", "test_bucket", 10),
        (1005, "test_workflow_run", "test_bucket", 1005),
        (
            1,
            "",
            "test_bucket",
            2,
        ),  # No workflow run filter, expect to receive all multipart uploads, regardless of prefix
    ],
)
def test_cleanup(
    open_multipart_uploads: None,
    workflow_run: str,
    bucket_name: str,
    expected_deletions: int,
) -> None:
    """
    Test the MultipartCleanup class.
    """
    cleaner = MultipartCleanup(workflow_run=workflow_run, bucket_name=bucket_name)
    assert cleaner.cleanup() == expected_deletions
