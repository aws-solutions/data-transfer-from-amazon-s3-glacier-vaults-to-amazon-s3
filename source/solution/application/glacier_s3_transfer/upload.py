"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from base64 import b64decode, b64encode
from typing import TYPE_CHECKING

import boto3

from solution.application import __boto_config__
from solution.application.hashing.s3_hash import S3Hash
from solution.application.model.glacier_transfer_part_model import GlacierTransferPart

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import (
        CompleteMultipartUploadOutputTypeDef,
        UploadPartOutputTypeDef,
    )

    from solution.application.model.responses import (
        GlacierRetrieval as GlacierRetrievalResponse,
    )
else:
    S3Client = object
    UploadPartOutputTypeDef = object
    CompleteMultipartUploadOutputTypeDef = object
    GlacierRetrievalResponse = object


class S3Upload:
    def __init__(
        self,
        bucket_name: str,
        key: str,
        upload_id: str,
    ) -> None:
        self.s3: S3Client = boto3.client("s3", config=__boto_config__)

        self.bucket_name = bucket_name
        self.key = key
        self.parts: list[GlacierRetrievalResponse] = []
        self.upload_id = upload_id

    def upload_part(self, chunk: bytes, part_number: int) -> GlacierRetrievalResponse:
        checksum = b64encode(S3Hash.hash(chunk)).decode("ascii")
        response: UploadPartOutputTypeDef = self.s3.upload_part(
            Body=chunk,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=part_number,
            UploadId=self.upload_id,
            ChecksumAlgorithm="SHA256",
            ChecksumSHA256=checksum,
            ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
        )
        return S3Upload._build_part(part_number, response["ETag"], checksum)

    def include_part(self, part: GlacierTransferPart) -> None:
        self.parts.append(
            S3Upload._build_part(part.part_number, part.e_tag, part.checksum_sha_256)
        )

    def complete_upload(self) -> CompleteMultipartUploadOutputTypeDef:
        s3_hash = S3Hash()
        for part in self.parts:
            s3_hash.include(
                b64decode(part["ChecksumSHA256"].encode("ascii")),
                part["PartNumber"] - 1,
            )

        return self.s3.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts},
            ChecksumSHA256=b64encode(s3_hash.digest()).decode("ascii"),
            ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
        )

    def get_file_size(self) -> int:
        return self.s3.head_object(
            Bucket=self.bucket_name,
            Key=self.key,
            ExpectedBucketOwner=os.environ["AWS_ACCOUNT_ID"],
        )["ContentLength"]

    @staticmethod
    def _build_part(
        part_number: int, etag: str, checksum: str
    ) -> GlacierRetrievalResponse:
        return {
            "PartNumber": part_number,
            "ETag": etag,
            "ChecksumSHA256": checksum,
        }
