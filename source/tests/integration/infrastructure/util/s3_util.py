"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import csv
import io
import os
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

import boto3
from tests.integration.infrastructure.util.ddb_util import get_inventory_csv

from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import ObjectIdentifierTypeDef
else:
    S3Client = object


def put_inventory_file_in_s3(
    file_name_prefix: str,
    vault_name: str,
) -> None:
    client: S3Client = boto3.client("s3")
    for i, csv_buffer in enumerate(inventory_file_generator(vault_name)):
        client.put_object(
            Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
            Key=f"{file_name_prefix}_{i}.csv",
            Body=csv_buffer.getvalue().encode("utf-8"),
        )


def delete_all_inventory_files_from_s3(prefix: str) -> None:
    client: S3Client = boto3.client("s3")
    objects_to_delete = client.list_objects_v2(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME], Prefix=prefix
    )
    if "Contents" not in objects_to_delete:
        return
    inventory_files_map: Sequence[ObjectIdentifierTypeDef] = [
        {"Key": obj["Key"]} for obj in objects_to_delete["Contents"]
    ]
    if inventory_files_map:
        client.delete_objects(
            Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
            Delete={"Objects": inventory_files_map},
        )


def delete_archives_from_s3(prefix: str) -> None:
    client: S3Client = boto3.client("s3")
    objects_to_delete = client.list_objects_v2(
        Bucket=os.environ[OutputKeys.OUTPUT_BUCKET_NAME], Prefix=prefix
    )
    if "Contents" not in objects_to_delete:
        return
    archive_map: Sequence[ObjectIdentifierTypeDef] = [
        {"Key": obj["Key"]} for obj in objects_to_delete["Contents"]
    ]
    if archive_map:
        client.delete_objects(
            Bucket=os.environ[OutputKeys.OUTPUT_BUCKET_NAME],
            Delete={"Objects": archive_map},
        )


def delete_inventory_file_from_s3(file_name_prefix: str, num_archives: int) -> None:
    client: S3Client = boto3.client("s3")
    inventories_keys = [f"{file_name_prefix}_{n}.csv" for n in range(num_archives)]
    client.delete_objects(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Delete={"Objects": [{"Key": key} for key in inventories_keys]},
    )


def inventory_file_generator(vault_name: str, bytes: Optional[str] = None) -> Any:
    csv_str = get_inventory_csv(vault_name)
    csv_str_list = csv_str.splitlines()
    for i in range(1, len(csv_str_list), 10000):
        csv_buffer = io.StringIO()
        writer = csv.writer(
            csv_buffer,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
            escapechar="\\",
            doublequote=False,
        )
        writer.writerow(
            [
                "ArchiveId",
                "ArchiveDescription",
                "CreationDate",
                "Size",
                "SHA256TreeHash",
                "Filename",
            ]
        )
        for row in csv_str_list[i : i + 10000]:
            row + f",filename_{i}"
            writer.writerow(row.split(","))
        yield csv_buffer
