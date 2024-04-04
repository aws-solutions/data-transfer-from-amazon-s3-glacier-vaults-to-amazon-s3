"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any, Dict, List

import boto3
from botocore.config import Config

__boto_config__ = Config(user_agent_extra="AwsSolution/SO0293/v1.1.0")


def update_metric_table(
    pk: str, table_name: str, migration_type: str, dfc: Dict[str, Any]
) -> None:
    node_inputs = list(dfc.values())
    client = boto3.client("dynamodb", config=__boto_config__)
    if migration_type == "LAUNCH":
        _update_metric_table_for_new_run(node_inputs, pk, client, table_name)
    else:
        _update_metric_table_to_resume_existing_run(pk, client, table_name)


def _update_metric_table_to_resume_existing_run(
    pk: str, ddb_client: Any, table_name: str
) -> None:
    response = ddb_client.get_item(TableName=table_name, Key={"pk": {"S": pk}})
    if "Item" not in response:
        raise ValueError(f"Item with pk {pk} not found in Metric Table {table_name}")
    try:
        count_downloaded = response["Item"]["count_downloaded"]
        size_downloaded = response["Item"]["size_downloaded"]
        ddb_client.update_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET count_requested = :ucr, size_requested = :usr, count_staged = :ucs, size_staged = :uss, count_failed = :ucf, size_failed =:usf",
            ExpressionAttributeValues={
                ":ucr": count_downloaded,
                ":usr": size_downloaded,
                ":ucs": count_downloaded,
                ":uss": size_downloaded,
                ":ucf": {"N": "0"},
                ":usf": {"N": "0"},
            },
        )
    except Exception as e:
        raise ValueError(f"Failed to update metric table for resume with error: {e}")


def _update_metric_table_for_new_run(
    node_inputs: List[Any], pk: str, ddb_client: Any, table_name: str
) -> None:
    total_archives_number = node_inputs[0].toDF().collect()[0].TotalArchivesNumber
    total_archives_size = node_inputs[0].toDF().collect()[0].TotalArchivesSize
    total_skipped_number = node_inputs[1].toDF().collect()[0].TotalArchivesNumber
    total_skipped_size = node_inputs[1].toDF().collect()[0].TotalArchivesSize

    item = {
        "pk": {"S": pk},
        "count_total": {"N": str(total_archives_number)},
        "size_total": {"N": str(total_archives_size)},
        "count_skipped": {"N": str(total_skipped_number)},
        "size_skipped": {"N": str(total_skipped_size)},
    }
    for value in [
        "count_requested",
        "size_requested",
        "count_staged",
        "size_staged",
        "count_downloaded",
        "size_downloaded",
        "count_failed",
        "size_failed",
    ]:
        item[value] = {"N": "0"}
    ddb_client.put_item(TableName=table_name, Item=item)
