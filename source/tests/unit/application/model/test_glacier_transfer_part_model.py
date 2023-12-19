"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest

from solution.application.model.glacier_transfer_part_model import (
    GlacierTransferPart,
    GlacierTransferPartRead,
)


@pytest.mark.parametrize(
    "tree_checksum,pk,sk",
    [
        ("tree_checksum", "run_id|object_id", "p|00001"),
        (None, "run_id|object_id", "p|00001"),
    ],
    ids=["forms_keys_properly", "ignores_missing_optional_fields"],
)
def test_glacier_transfer_part_model(
    part_model: GlacierTransferPart, tree_checksum: str | None, pk: str, sk: str
) -> None:
    marshaled = part_model.marshal()
    assert marshaled.get("tree_checksum", {}).get("S") == tree_checksum
    assert marshaled["pk"]["S"] == pk
    assert marshaled["sk"]["S"] == sk
    assert marshaled["checksum_sha_256"]["S"] == "shachecksum"
    assert marshaled["e_tag"]["S"] == "etag"
    assert marshaled["part_number"]["N"] == "1"
    assert GlacierTransferPart.parse(marshaled).marshal() == marshaled
    assert GlacierTransferPart.parse(marshaled).part_number == 1


@pytest.fixture
def part_model(tree_checksum: str | None) -> GlacierTransferPart:
    return GlacierTransferPart(
        workflow_run="run_id",
        glacier_object_id="object_id",
        checksum_sha_256="shachecksum",
        e_tag="etag",
        part_number=1,
        tree_checksum=tree_checksum,
    )


@pytest.mark.parametrize(
    "part_number,pk,sk",
    [
        (1, "run_id|object_id", "p|00001"),
        (9999, "run_id|object_id", "p|09999"),
    ],
    ids=["forms_keys_properly", "pads_part_number_properly"],
)
def test_glacier_transfer_part_model_read(
    part_model_read: GlacierTransferPartRead,
    pk: str,
    sk: str,
) -> None:
    marshaled = part_model_read.marshal()
    assert marshaled["pk"]["S"] == pk
    assert marshaled["sk"]["S"] == sk
    assert len(marshaled.items()) == 3
    assert GlacierTransferPartRead.parse(marshaled).marshal() == marshaled
    keys = part_model_read.key
    assert keys["pk"]["S"] == pk
    assert keys["sk"]["S"] == sk
    assert len(keys.items()) == 2


@pytest.fixture
def part_model_read(part_number: int) -> GlacierTransferPartRead:
    return GlacierTransferPartRead(
        workflow_run="run_id",
        glacier_object_id="object_id",
        part_number=part_number,
    )
