"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
    GlacierTransferMetadataRead,
)


def test_glacier_transfer_metadata_model() -> None:
    metadata_model = GlacierTransferMetadata(
        workflow_run="run_id",
        glacier_object_id="object_id",
        job_id="job_id",
        staged_job_id="staged_job_id",
        start_time="start_time",
        vault_name="vault_name",
        retrieval_type="archive-retrieval",
        file_name="inventory",
        s3_storage_class="GLACIER",
        retrieve_status="status",
        description="test_description",
        size=2023,
    )
    marshaled = metadata_model.marshal()
    assert marshaled["job_id"]["S"] == "job_id"
    assert marshaled["staged_job_id"]["S"] == "staged_job_id"
    assert marshaled["start_time"]["S"] == "start_time"
    assert marshaled["vault_name"]["S"] == "vault_name"
    assert marshaled["retrieval_type"]["S"] == "archive-retrieval"
    assert marshaled["file_name"]["S"] == "inventory"
    assert marshaled["s3_storage_class"]["S"] == "GLACIER"
    assert marshaled["retrieve_status"]["S"] == "status"
    assert marshaled["description"]["S"] == "test_description"
    assert marshaled["pk"]["S"] == "run_id|object_id"
    assert marshaled["sk"]["S"] == "meta"
    assert marshaled["size"]["N"] == "2023"
    assert len(marshaled.items()) == 12
    assert GlacierTransferMetadata.parse(marshaled).size == 2023
    assert GlacierTransferMetadata.parse(marshaled).marshal() == marshaled


def test_glacier_transfer_metadata_read_model() -> None:
    metadata_model = GlacierTransferMetadataRead(
        workflow_run="run_id",
        glacier_object_id="object_id",
    )
    marshaled = metadata_model.marshal()
    assert marshaled["pk"]["S"] == "run_id|object_id"
    assert marshaled["sk"]["S"] == "meta"
    assert len(marshaled) == 2
    assert GlacierTransferMetadataRead.parse(marshaled).marshal() == marshaled
