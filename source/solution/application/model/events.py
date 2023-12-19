"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Any, Dict, List, TypedDict

if TYPE_CHECKING:
    from mypy_boto3_glacier.type_defs import InitiateJobInputRequestTypeDef
else:
    InitiateJobInputRequestTypeDef = TypedDict("MockType")


class GlacierRetrieval(TypedDict):
    JobId: str
    VaultName: str
    ByteRange: str
    S3DestinationBucket: str
    S3DestinationKey: str
    GlacierObjectId: str
    UploadId: str
    PartNumber: int
    WorkflowRun: str


class InventoryChunkOverlap(TypedDict):
    InventorySize: int
    MaximumInventoryRecordSize: int
    ChunkSize: int


class Chunk(TypedDict):
    TotalSize: int
    ChunkSize: int


class MultipartUploadValidation(TypedDict):
    WorkflowRun: str
    GlacierObjectId: str


class ArchiveNaming(TypedDict):
    ArchiveDescription: str
    ArchiveId: str


class ArchiveNamingOverride(TypedDict):
    WorkflowRun: str
    NameOverridePresignedURL: str


class InitiateArchiveRetrieval(InitiateJobInputRequestTypeDef, total=False):
    ArchiveDescription: str


class RemoveMultipartUploads(TypedDict):
    WorkflowRun: str
    S3DestinationBucket: str


class ArchiveItem(TypedDict):
    CreationDate: str
    Size: str
    ArchiveDescription: str
    ArchiveId: str
    SHA256TreeHash: str
    Filename: str


class InitiateArchiveRetrievalItem(TypedDict):
    vault_name: str
    workflow_run: str
    s3_storage_class: str
    tier: str
    item: ArchiveItem


class InitiateArchiveRetrievalBatch(TypedDict):
    AccountId: str
    SNSTopic: str
    Items: List[InitiateArchiveRetrievalItem]


class ExtendArchiveRetrievalItem(TypedDict):
    vault_name: str
    workflow_run: str
    s3_storage_class: str
    tier: str
    item: Dict[str, Any]


class ExtendArchiveRetrievalBatch(TypedDict):
    AccountId: str
    SNSTopic: str
    Items: List[ExtendArchiveRetrievalItem]
