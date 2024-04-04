"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import logging
import os
from typing import TYPE_CHECKING

import boto3

from solution.application import __boto_config__

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import (
        ListMultipartUploadsOutputTypeDef,
        MultipartUploadTypeDef,
    )
else:
    ListMultipartUploadsOutputTypeDef = object
    MultipartUploadTypeDef = object

logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


class MultipartCleanup:
    def __init__(self, workflow_run: str, bucket_name: str):
        self.workflow_run = workflow_run
        self.bucket_name = bucket_name
        self.client = boto3.client("s3", config=__boto_config__)

    def cleanup(self) -> int:
        open_uploads: ListMultipartUploadsOutputTypeDef = (
            self.client.list_multipart_uploads(
                Bucket=self.bucket_name,
                Prefix=self.workflow_run,
                ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
            )
        )
        if "Uploads" not in open_uploads:
            logger.info("No uploads to cleanup")
            return 0
        for upload in open_uploads["Uploads"]:
            self.abort_upload(upload)
        return len(open_uploads["Uploads"]) + self.cleanup()

    def abort_upload(self, upload: MultipartUploadTypeDef) -> None:
        logger.info(f"Aborting upload: {upload['UploadId']}")
        self.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=upload["Key"],
            UploadId=upload["UploadId"],
            ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
        )
