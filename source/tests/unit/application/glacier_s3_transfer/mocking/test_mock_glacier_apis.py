"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import re
import typing
from importlib import resources

from solution.application import mocking
from solution.application.glacier_service.glacier_apis_factory import GlacierAPIsFactory
from solution.application.glacier_service.glacier_typing import GlacierJobType

if typing.TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import GetJobOutputOutputTypeDef
    from mypy_boto3_s3.client import S3Client
else:
    GlacierClient = object
    S3Client = object
    GetJobOutputOutputTypeDef = object
import pytest


@pytest.fixture(autouse=True)
def mock_glacier_asset(s3_client: S3Client) -> None:
    os.environ["MOCK_GLACIER_DATA_BUCKET_NAME"] = "test_mock_glacier_apis_bucket"
    os.environ["MOCK_GLACIER_DATA_KEY"] = "test.py"
    s3_client.create_bucket(
        Bucket=os.environ["MOCK_GLACIER_DATA_BUCKET_NAME"],
    )
    mock_data_path = resources.files(mocking.__name__) / "mock_glacier_data.py"
    with mock_data_path.open("rb") as f:
        s3_client.put_object(
            Bucket=os.environ["MOCK_GLACIER_DATA_BUCKET_NAME"],
            Key=os.environ["MOCK_GLACIER_DATA_KEY"],
            Body=f,
        )


@pytest.fixture
def glacier_apis() -> GlacierClient:
    return GlacierAPIsFactory.create_instance(True)


def test_inventory_retrieval(glacier_apis: GlacierClient) -> None:
    initiate_job_response = glacier_apis.initiate_job(
        vaultName="test_mock_glacier_apis_vault",
        accountId="",
        jobParameters={"Type": GlacierJobType.INVENTORY_RETRIEVAL},
    )
    assert (
        initiate_job_response["location"]
        == "//vaults/test_mock_glacier_apis_vault/jobs/IEQH524YNG5BY1A2ROGUBBB8AYN1B7O259OWOO3SB09GLSHV616MTS56ZC4PZ0LX9XF26GK7ZX5B4CTZKK6OAM89OZ6W"
    )
    assert (
        initiate_job_response["jobId"]
        == "IEQH524YNG5BY1A2ROGUBBB8AYN1B7O259OWOO3SB09GLSHV616MTS56ZC4PZ0LX9XF26GK7ZX5B4CTZKK6OAM89OZ6W"
    )
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="test_mock_glacier_apis_vault",
        jobId=initiate_job_response["jobId"],
        accountId="",
    )
    pattern = rb"^ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\r\ncf2e306ff9a72790b152fb4af93a1a1d,test.txt,\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z,8,b9f9644670e5fcd37a4c54a478d636fc37c41282d161e3e50cb3fb962aa04285\r\n$"
    assert re.match(pattern, get_job_output_response["body"].read()) is not None


def test_get_job_output_with_range(glacier_apis: GlacierClient) -> None:
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="test_mock_glacier_apis_vault",
        jobId="W3R9AY6I79N1D4X9M605W0WA88V3BOL9LF9QCEFB2ARPRHLWSEKKQ7KRS3U54HBTYV0MQGQ6N1BOBZJCK2618O72O7BZ",
        accountId="",
        range="bytes=3-5",
    )
    assert get_job_output_response["body"].read() == b"TBO"
