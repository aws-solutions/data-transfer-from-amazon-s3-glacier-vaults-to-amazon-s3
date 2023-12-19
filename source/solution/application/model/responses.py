"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from mypy_boto3_glacier.type_defs import InitiateJobOutputTypeDef
from mypy_boto3_s3.type_defs import CompletedPartTypeDef


class GlacierRetrieval(CompletedPartTypeDef, total=False):
    TreeChecksum: str


class InitiateArchiveRetrieval(InitiateJobOutputTypeDef, total=False):
    FileName: str
