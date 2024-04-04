"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import io
import os
import urllib.request
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from solution.application import __boto_config__
from solution.infrastructure.output_keys import OutputKeys


def create_header_file(workflow_run: str) -> None:
    s3_client: S3Client = boto3.client("s3", config=__boto_config__)
    file_name = f"{workflow_run}/naming_overrides/override_headers.csv"

    s3_client.put_object(
        Body=b"GlacierArchiveID,FileName\n,",
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Key=file_name,
        ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
    )


def upload_provided_file(workflow_run: str, presigned_url: str) -> None:
    s3_client: S3Client = boto3.client("s3", config=__boto_config__)
    with urllib.request.urlopen(presigned_url) as response:
        content = response.read()
        s3_client.upload_fileobj(
            io.BytesIO(content),
            os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
            f"{workflow_run}/naming_overrides/name_overrides.csv",
        )
