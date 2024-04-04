"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


import csv
import io
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

import boto3

from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.mocking.mock_glacier_data import MOCK_DATA
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = object


def ddb_client() -> DynamoDBClient:
    if "AWS_PROFILE" in os.environ and "AWS_REGION" in os.environ:
        session = boto3.Session(
            profile_name=os.environ["AWS_PROFILE"],
            region_name=os.environ["AWS_REGION"],
        )
    else:
        session = boto3.Session()

    client: DynamoDBClient = session.client("dynamodb")
    return client


def convert_csv_to_dict(csv_text: str) -> Dict[str, Any]:
    """
    Having a csv to dict conventor for easier parsing of inventory/csv files
    """
    csv_file = io.StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    csv_dict = {str(index): rows for index, rows in enumerate(reader)}
    return csv_dict


def get_inventory_csv(vault_name: str) -> str:
    csv_str = ""
    job_id = MOCK_DATA[vault_name]["initiate-job"]["inventory-retrieval"]["jobId"]  # type: ignore
    if "body" in MOCK_DATA[vault_name]["get-job-output"][job_id]:  # type: ignore
        csv_str = MOCK_DATA[vault_name]["get-job-output"][job_id]["body"]  # type: ignore
    else:
        for key in MOCK_DATA[vault_name]["get-job-output"][job_id]:  # type: ignore
            if key.startswith("bytes"):
                csv_str = MOCK_DATA[vault_name]["get-job-output"][job_id][key]["body"]  # type: ignore
                break

    return csv_str


def glacier_retrieval_table_update_item(
    workflow_run_id: str,
    vault_name: str,
    workflow_type: str = GlacierJobType.ARCHIVE_RETRIEVAL,
) -> None:
    csv_str = get_inventory_csv(vault_name)

    body_dict = convert_csv_to_dict(csv_str)

    client: Any = ddb_client()
    for _, v in body_dict.items():
        archive_id = v["ArchiveId"]
        archive_job_id = MOCK_DATA[vault_name]["initiate-job"][f"archive-retrieval:{archive_id}"]["jobId"]  # type: ignore
        item = GlacierTransferMetadata(
            workflow_run=workflow_run_id,
            glacier_object_id=archive_id,
            job_id=archive_job_id,
            start_time="2021-01-01T00:00:00Z",
            vault_name=vault_name,
            retrieval_type=workflow_type,
            size=int(v["Size"]),
            chunk_size=64 * 2**20,
            description=v["ArchiveDescription"],
            retrieve_status=f"workflow1/{GlacierTransferModel.StatusCode.REQUESTED}",
            download_window=datetime.now().isoformat(),
            file_name=archive_id,
            sha256_tree_hash=v["SHA256TreeHash"],
            s3_storage_class="GLACIER",
        )
        client.put_item(
            TableName=os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME],
            Item=item.marshal(),
        )


def delete_all_table_items(
    table_name: str, primary_key: str, sort_key: Optional[str] = None
) -> None:
    client: Any = ddb_client()
    response = client.scan(TableName=table_name, Select="ALL_ATTRIBUTES")

    while response:
        delete_request_list = []
        for item in response["Items"]:
            delete_request = {
                "DeleteRequest": {"Key": {primary_key: item[primary_key]}}
            }
            if sort_key:
                delete_request["DeleteRequest"]["Key"].update(
                    {sort_key: item[sort_key]}
                )
            delete_request_list.append(delete_request)

        for i in range(0, (len(delete_request_list) // 25 + 1) * 25, 25):
            if len(delete_request_list[i : i + 25]) > 0:
                client.batch_write_item(
                    RequestItems={table_name: delete_request_list[i : i + 25]}
                )
        if not response.get("LastEvaluatedKey"):
            break

        response = client.scan(
            TableName=table_name,
            Select="ALL_ATTRIBUTES",
            ExclusiveStartKey=response.get("LastEvaluatedKey"),
        )
