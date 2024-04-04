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
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from solution.application import __boto_config__
from solution.application.archive_naming.override import (
    create_header_file,
    upload_provided_file,
)
from solution.infrastructure.output_keys import OutputKeys


@pytest.fixture(autouse=True)
def mock_inventory_bucket(s3_client: S3Client) -> None:
    os.environ[
        OutputKeys.INVENTORY_BUCKET_NAME
    ] = "test_mock_upload_provided_file_bucket"
    s3_client.create_bucket(Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME])


@pytest.fixture()
def workflow_run() -> str:
    return "test-workflow_run"


@pytest.fixture()
def pre_signed_url() -> str:
    return "https://test_url.test/mock-presigned-url"


def test_user_agent_on_s3_client(
    solution_user_agent: str,
    workflow_run: str,
    pre_signed_url: str,
) -> None:
    with patch("boto3.client") as mock_client:
        create_header_file(workflow_run)
        _config = mock_client.call_args[1]["config"]
        assert type(_config) is Config

        _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
        assert _config_user_agent_extra == __boto_config__.__getattribute__(
            "user_agent_extra"
        )
        assert _config_user_agent_extra == solution_user_agent


def test_upload_provided_file_successful(
    s3_client: S3Client, workflow_run: str, pre_signed_url: str
) -> None:
    content = b"Mock content"
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__().getcode.return_value = 200
        mock_urlopen.return_value.__enter__().read.return_value = content

        upload_provided_file(workflow_run, pre_signed_url)

    obj = s3_client.get_object(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Key=f"{workflow_run}/naming_overrides/name_overrides.csv",
    )
    assert content == obj["Body"].read()


def test_upload_provided_file_expired_url(
    s3_client: S3Client, workflow_run: str, pre_signed_url: str
) -> None:
    mock_exception = urllib.error.HTTPError(
        pre_signed_url, 403, "Forbidden", {}, None  # type:ignore
    )

    with patch("urllib.request.urlopen", side_effect=mock_exception) as mock_urlopen:
        with pytest.raises(Exception):
            upload_provided_file(workflow_run, pre_signed_url)
