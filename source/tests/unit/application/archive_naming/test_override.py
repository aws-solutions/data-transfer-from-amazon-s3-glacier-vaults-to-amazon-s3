"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
import urllib.error
import urllib.request
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


from solution.application.archive_naming.override import upload_provided_file
from solution.infrastructure.output_keys import OutputKeys


@pytest.fixture(autouse=True)
def mock_inventory_bucket(s3_client: S3Client) -> None:
    os.environ[
        OutputKeys.INVENTORY_BUCKET_NAME
    ] = "test_mock_upload_provided_file_bucket"
    s3_client.create_bucket(Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME])


def test_upload_provided_file_successful(s3_client: S3Client) -> None:
    workflow_run = "test-workflow_run"
    presigned_url = "https://test_url.test/mock-presigned-url"

    content = b"Mock content"
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__().getcode.return_value = 200
        mock_urlopen.return_value.__enter__().read.return_value = content

        upload_provided_file(workflow_run, presigned_url)

    obj = s3_client.get_object(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Key=f"{workflow_run}/naming_overrides/name_overrides.csv",
    )
    assert content == obj["Body"].read()


def test_upload_provided_file_expired_url(s3_client: S3Client) -> None:
    workflow_run = "test-workflow_run"
    presigned_url = "https://test_url.test/mock-presigned-url"

    mock_exception = urllib.error.HTTPError(
        presigned_url, 403, "Forbidden", {}, None  # type:ignore
    )

    with patch("urllib.request.urlopen", side_effect=mock_exception) as mock_urlopen:
        with pytest.raises(Exception):
            upload_provided_file(workflow_run, presigned_url)
