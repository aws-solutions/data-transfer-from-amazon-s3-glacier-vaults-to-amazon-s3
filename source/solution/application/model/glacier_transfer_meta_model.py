"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from dataclasses import dataclass
from typing import Optional

from solution.application.model.base import Model
from solution.application.model.glacier_transfer_model import GlacierTransferModel


@dataclass
class GlacierTransferMetadataRead(GlacierTransferModel):
    sk: str = Model.field(["sk", "S"], default="meta")


@dataclass
class GlacierTransferMetadata(GlacierTransferMetadataRead):
    job_id: str = Model.field(["job_id", "S"])
    staged_job_id: str = Model.field(["staged_job_id", "S"], optional=True)
    start_time: str = Model.field(["start_time", "S"])
    vault_name: str = Model.field(["vault_name", "S"])
    retrieval_type: str = Model.field(["retrieval_type", "S"])
    file_name: str = Model.field(["file_name", "S"])
    s3_storage_class: str = Model.field(["s3_storage_class", "S"])
    retrieve_status: str = Model.field(["retrieve_status", "S"])
    description: str = Model.field(["description", "S"])
    size: int | None = Model.field(["size", "N"], marshal_as=str)
    chunk_size: int | None = Model.field(
        ["chunk_size", "N"], marshal_as=str, optional=True
    )
    chunks_count: int | None = Model.field(
        ["chunks_count", "N"], marshal_as=str, optional=True
    )
    upload_id: str | None = Model.field(["upload_id", "S"], optional=True)
    download_window: str | None = Model.field(["download_window", "S"], optional=True)
    archive_id: str | None = Model.field(["archive_id", "S"], optional=True)
    archive_creation_date: str | None = Model.field(
        ["archive_creation_date", "S"], optional=True
    )
    sha256_tree_hash: str | None = Model.field(["sha256_tree_hash", "S"], optional=True)
    s3_destination_bucket: str | None = Model.field(
        ["s3_destination_bucket", "S"], optional=True
    )
    s3_destination_key: str | None = Model.field(
        ["s3_destination_key", "S"], optional=True
    )
