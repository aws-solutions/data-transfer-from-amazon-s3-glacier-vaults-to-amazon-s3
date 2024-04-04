"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING

import boto3

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object


def assert_bucket_put_get_object(bucket_name: str, s3_client: S3Client) -> None:
    value = "test data".encode("utf-8")
    key = "test_object.txt"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=value)

    assert value == s3_client.get_object(Bucket=bucket_name, Key=key)["Body"].read()

    s3_client.delete_object(Bucket=bucket_name, Key=key)


def test_output_bucket_put_get_object(s3_client: S3Client) -> None:
    bucket_name = os.environ[OutputKeys.OUTPUT_BUCKET_NAME]
    assert_bucket_put_get_object(bucket_name, s3_client)


def test_inventory_bucket_put_get_object(s3_client: S3Client) -> None:
    bucket_name = os.environ[OutputKeys.INVENTORY_BUCKET_NAME]
    assert_bucket_put_get_object(bucket_name, s3_client)
