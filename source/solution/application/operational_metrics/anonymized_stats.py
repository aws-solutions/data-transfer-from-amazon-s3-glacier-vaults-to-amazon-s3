"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import datetime
import json
import logging
import os
import urllib.request
import uuid
from typing import Any, Dict

from solution.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from solution.application.model.metric_record import MetricRecord
from solution.application.model.workflow_metadata_model import WorkflowMetadataRecord
from solution.infrastructure.output_keys import OutputKeys

logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))

SOLUTION_BUILDERS_ENDPOINT = "https://metrics.awssolutionsbuilder.com/generic"


class StatsType:
    SUBMISSION = "submission"
    COMPLETION = "completion"


def send_job_stats(stats_type: str, workflow_run: str) -> None:
    metric_record = query_metric(workflow_run)
    workflow_metadata_record = query_workflow_metadata(workflow_run)

    if not workflow_metadata_record:
        logger.info(f"Failed to retrieve workflow metadata record for {workflow_run}")
        return

    if not metric_record:
        logger.info(f"Failed to retrieve metric record for {workflow_run}")
        return

    UUID = uuid.uuid5(uuid.NAMESPACE_OID, os.environ["ACCOUNT_ID"])
    anonymized_data: Dict[str, Any] = {
        "Solution": os.environ["SOLUTION_ID"],
        "UUID": str(UUID),
        "TimeStamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "Data": {
            "Region": os.environ["REGION"],
            "Version": os.environ["VERSION"],
            "StorageClass": workflow_metadata_record.storage_class,
            "RetrievalTier": workflow_metadata_record.retrieval_tier,
            "StartTime": workflow_metadata_record.start_time,
            "DailyQuota": workflow_metadata_record.daily_quota,
            "ProvidedInventory": workflow_metadata_record.provided_inventory,
            "TransferType": workflow_metadata_record.transfer_type,
            "CrossRegionTransfer": workflow_metadata_record.cross_region_transfer,
            "NamingOverrideFile": "NO"
            if workflow_metadata_record.naming_override_file == ""
            else "YES",
            "VaultSize": metric_record.size_total,
            "ArchiveCount": metric_record.count_total,
        },
    }

    if stats_type == StatsType.COMPLETION:
        anonymized_data["Data"]["FinishTime"] = datetime.datetime.now().strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        anonymized_data["Data"]["DownloadedSize"] = metric_record.size_downloaded
        anonymized_data["Data"]["DownloadedCount"] = metric_record.count_downloaded

    send_stats(anonymized_data)


def query_metric(workflow_run: str) -> MetricRecord | None:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.METRIC_TABLE_NAME])
    metric = ddb_accessor.get_item(key={"pk": {"S": workflow_run}})

    return MetricRecord.parse(metric) if metric else None


def query_workflow_metadata(workflow_run: str) -> WorkflowMetadataRecord | None:
    ddb_accessor = DynamoDBAccessor(os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME])
    workflow_metadata = ddb_accessor.get_item(
        key={
            "pk": {"S": workflow_run},
            "sk": {"S": "meta"},
        },
    )
    return (
        WorkflowMetadataRecord.parse(workflow_metadata) if workflow_metadata else None
    )


def send_stats(anonymized_data: Dict[str, Any]) -> None:
    if os.environ["SEND_ANONYMIZED_STATISTICS"] != "Yes":
        logging.info(
            f"Sending anonymized data has been disabled. Anonymized_data: {anonymized_data}"
        )
        return

    logger.debug(f"Stats anonymized data: {anonymized_data}")

    request = json.dumps(anonymized_data).encode("utf-8")
    headers = {"Content-Type": "application/json", "Content-Length": str(len(request))}

    try:
        req = urllib.request.Request(
            SOLUTION_BUILDERS_ENDPOINT, data=request, headers=headers, method="POST"
        )
        urllib.request.urlopen(req)
    except urllib.error.URLError as e:
        logging.error(f"Failed to send data: {str(e)}")
