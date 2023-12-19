"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
import random
from datetime import datetime, timedelta
from importlib import resources
from typing import Any, Callable, Dict, Optional
from unittest.mock import patch

import black
import requests  # type: ignore
from moto import mock_glacier  # type: ignore
from moto.glacier import models  # type: ignore

from solution.application import mocking
from solution.application.chunking.chunk_generator import (
    calculate_chunk_size,
    generate_chunk_array,
)
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.mocking.mock_glacier_vault import MockGlacierVault

mock_vault_list = []


def mock_vault(func: Callable[[MockGlacierVault], Dict[str, Any]]) -> None:
    """Decorator to mark a function as a mock vault.

    When applied to a function, this decorator generates a mock Glacier vault with the same
    name as the function in a file named mock_glacier_data.py. This can be useful for testing
    code that interacts with the AWS Glacier service without actually making network requests.
    The decorator also seeds the moto API with a set value to ensure that UUIDs match on
    consecutive runs of the script, and mocks the datetime.timedelta function to return 0 to
    prevent an artificial sleep call in the moto initiate-job API.

    Args:
        func (Callable): The function to be decorated.

    Returns:
        None

    Raises:
        TypeError: If the input argument is not a callable object.

    """

    def wrapper() -> Dict[str, Any]:
        random.seed(1)
        with mock_glacier():
            # Sonarqube ruleKey python:S5332
            # This is safe. The http request is getting used locally for seeding moto for test setup.
            # https://docs.getmoto.org/en/latest/docs/configuration/recorder/index.html#deterministic-identifiers
            requests.post("http://motoapi.amazonaws.com/moto-api/seed?a=1")  # NOSONAR
            return func(MockGlacierVault(func.__name__))

    mock_vault_list.append(wrapper)


@mock_vault
def test_mock_glacier_apis_vault(vault: MockGlacierVault) -> Dict[str, Any]:
    archive_id = vault.upload_archive(body="TESTBODY", archive_description="test.txt")
    inventory_job_id = vault.initiate_job({"Type": GlacierJobType.INVENTORY_RETRIEVAL})
    vault.get_job_output(inventory_job_id)

    archive_job_id = vault.initiate_job(
        {"Type": GlacierJobType.ARCHIVE_RETRIEVAL, "ArchiveId": archive_id}
    )
    vault.get_job_output(archive_job_id, range="bytes=0-2")
    vault.get_job_output(archive_job_id, range="bytes=3-5")
    vault.get_job_output(archive_job_id, range="bytes=6-8")
    return vault.mock_data()


@mock_vault
def test_vault_generation_vault(vault: MockGlacierVault) -> Dict[str, Any]:
    archive_id = vault.upload_archive(body="TESTBODY", archive_description="test.txt")
    archive_id_2 = vault.upload_archive(
        body="TESTBODY2", archive_description='my archive description,1"2'
    )
    inventory_job_id = vault.initiate_job({"Type": GlacierJobType.INVENTORY_RETRIEVAL})
    vault.get_job_output(inventory_job_id)

    archive_job_id = vault.initiate_job(
        {"Type": GlacierJobType.ARCHIVE_RETRIEVAL, "ArchiveId": archive_id}
    )

    archive_2_job_id = vault.initiate_job(
        {"Type": GlacierJobType.ARCHIVE_RETRIEVAL, "ArchiveId": archive_id_2}
    )
    vault.get_job_output(archive_job_id, range="bytes=0-2")
    vault.get_job_output(archive_job_id, range="bytes=3-5")
    vault.get_job_output(archive_job_id, range="bytes=6-8")
    vault.get_job_output(archive_2_job_id)
    return vault.mock_data()


def _vault_generator(
    vault: MockGlacierVault,
    num_archives: int,
    mock_archive: bool,
) -> Dict[str, Any]:
    for i in range(num_archives):
        archive_body = f"TESTBODY{i}"
        archive_id = vault.upload_archive(
            body=archive_body, archive_description=f"test{i}.txt"
        )
        chunk_size = calculate_chunk_size(len(archive_body))
        if mock_archive:
            archive_job_id = vault.initiate_job(
                {"Type": GlacierJobType.ARCHIVE_RETRIEVAL, "ArchiveId": archive_id}
            )
            chunk_array = generate_chunk_array(len(archive_body), chunk_size, False)
            for chunk in chunk_array:
                vault.get_job_output(archive_job_id, range=f"bytes={chunk}")

    inventory_job_id = vault.initiate_job({"Type": GlacierJobType.INVENTORY_RETRIEVAL})

    vault.set_inventory_metadata(inventory_job_id, chunk_size)

    if chunk_size:
        chunk_array = generate_chunk_array(vault.inventory_size, chunk_size, False)
        for chunk in chunk_array:
            vault.get_job_output(inventory_job_id, range=f"bytes={chunk}")
    else:
        vault.get_job_output(inventory_job_id)

    return vault.mock_data()


@mock_vault
def test_vault_chunk_generation_vault(vault: MockGlacierVault) -> Dict[str, Any]:
    return _vault_generator(vault, 45000, False)


@mock_vault
def test_medium_vault(vault: MockGlacierVault) -> Dict[str, Any]:
    return _vault_generator(vault, 45, True)


@mock_vault
def test_small_vault(vault: MockGlacierVault) -> Dict[str, Any]:
    return _vault_generator(vault, 5, True)


@mock_vault
def test_small_vault_archive_range(vault: MockGlacierVault) -> Dict[str, Any]:
    return _vault_generator(vault, 10, True)


def generate_mock_glacier_data() -> str:
    models.datetime.datetime = RandomDatetime
    models.Vault.job_ready = job_always_ready
    mock_vault_dict: Dict[str, Any] = {}
    for mock_vault in mock_vault_list:
        mock_vault_dict = mock_vault_dict | mock_vault()
    data = []
    data.append(
        '"""\nCopyright Amazon.com, Inc. or its affiliates. All Rights Reserved.\nSPDX-License-Identifier: Apache-2.0\n"""\n'
    )
    data.append(
        "# This file is auto-generated by mock_glacier_generator.py and formatted with black\n"
    )
    data.append(f"MOCK_DATA = {json.dumps(mock_vault_dict, indent=4)}")
    return black.format_str("".join(data), mode=black.FileMode())


def write_mock_glacier_data() -> None:
    mock_data_path = resources.files(mocking.__name__) / "mock_glacier_data.py"
    with mock_data_path.open("w") as f:
        f.write(generate_mock_glacier_data())


class RandomDatetime(datetime):
    @classmethod
    def now(cls: Any, tz: Any = None) -> Any:
        start = datetime(2000, 1, 1)
        now = start + timedelta(days=365)
        random_date = start + timedelta(
            seconds=random.randint(0, int((now - start).total_seconds()))  # NOSONAR
        )
        # Sonarqube ruleKey: python:S2245
        # Applying NOSONAR since This is safe. This is just to randomize date, it is safe here

        return random_date


def job_always_ready(self: models.Vault, job_id: str) -> Any:
    job = self.describe_job(job_id)
    job.et = datetime(1, 1, 1)
    jobj = job.to_dict()
    return jobj["Completed"]
