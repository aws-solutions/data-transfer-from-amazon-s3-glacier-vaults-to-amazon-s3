"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any, List, Optional

import boto3

from solution.application import __boto_config__
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.model.glacier_transfer_meta_model import (
    GlacierTransferMetadata,
)
from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.util.retry import retry
from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import TransactWriteItemTypeDef
else:
    DynamoDBClient = object
    TransactWriteItemTypeDef = object


logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


class StatusMetricController:
    def __init__(self, records: List[dict[str, Any]]) -> None:
        self.counted_logs: List[str] = []
        self.records = records
        self.requested_count = 0
        self.staged_count = 0
        self.downloaded_count = 0
        self.requested_size = 0
        self.staged_size = 0
        self.downloaded_size = 0
        self.client_request_token = self._generate_client_request_token(records)

    def _generate_client_request_token(self, records: List[dict[str, Any]]) -> str:
        token = hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()
        # Slice to extract every other hexadecimal character and concatenate the last 4 characters to generate a 36 character long token
        return token[::2] + token[-4:]

    def handle_archive_status_changed(self) -> None:
        for record in self.records:
            if record.get("eventSource") == "aws:dynamodb":
                if record.get("eventName") == "INSERT":
                    self.increase_archive_status_metric_counter(
                        record["dynamodb"]["NewImage"]
                    )
                elif record.get("eventName") == "MODIFY":
                    self.increase_archive_status_metric_counter(
                        record["dynamodb"]["NewImage"], record["dynamodb"]["OldImage"]
                    )
        self.update_metric_query()

    @retry(max_retries=10, raise_exception=True)
    def update_metric_query(self) -> None:
        if self.records:
            ddb_client: DynamoDBClient = boto3.client(
                "dynamodb", config=__boto_config__
            )

            update_expression = "ADD"
            expression_attribute_values = {}
            delimiter = " "

            for attribute_status in (
                GlacierTransferModel.StatusCode.REQUESTED,
                GlacierTransferModel.StatusCode.STAGED,
                GlacierTransferModel.StatusCode.DOWNLOADED,
            ):
                for attribute_type in ("count", "size"):
                    update_expression += f"{delimiter}{attribute_type}_{attribute_status} :update_{attribute_type}_{attribute_status}"
                    delimiter = ", "

                    attribute_key = f":update_{attribute_type}_{attribute_status}"
                    attribute_value = str(
                        getattr(self, f"{attribute_status}_{attribute_type}")
                    )
                    expression_attribute_values[attribute_key] = {"N": attribute_value}

            transact_items: List[TransactWriteItemTypeDef] = [
                {
                    "Update": {
                        "TableName": os.environ[OutputKeys.METRIC_TABLE_NAME],
                        "Key": {"pk": {"S": self.workflow_run}},
                        "UpdateExpression": update_expression,
                        "ExpressionAttributeValues": expression_attribute_values,
                    },
                }
            ]
            ddb_client.transact_write_items(
                TransactItems=transact_items,
                ClientRequestToken=self.client_request_token,
            )

            for entry in self.counted_logs:
                logger.info(entry)

    def increase_archive_status_metric_counter(
        self, new_image: dict[str, Any], old_image: Optional[dict[str, Any]] = None
    ) -> None:
        new_metadata = GlacierTransferMetadata.parse(new_image)
        self.workflow_run = new_metadata.workflow_run

        if new_metadata.retrieval_type != GlacierJobType.ARCHIVE_RETRIEVAL:
            return

        if not new_metadata.size or not new_metadata.archive_id:
            logger.error(f"Failed to read archive's metadata from {new_metadata}")
            return

        new_status = new_metadata.retrieve_status.split("/")[-1]
        old_status = (
            GlacierTransferMetadata.parse(old_image).retrieve_status.split("/")[-1]
            if old_image
            else None
        )

        status_mapping = {
            (
                None,
                GlacierTransferModel.StatusCode.REQUESTED,
            ): GlacierTransferModel.StatusCode.REQUESTED,
            (
                GlacierTransferModel.StatusCode.REQUESTED,
                GlacierTransferModel.StatusCode.STAGED,
            ): GlacierTransferModel.StatusCode.STAGED,
            (
                GlacierTransferModel.StatusCode.STAGED,
                GlacierTransferModel.StatusCode.DOWNLOADED,
            ): GlacierTransferModel.StatusCode.DOWNLOADED,
        }
        result_status = status_mapping.get((old_status, new_status), None)

        archive_id = GlacierTransferModel(
            workflow_run=new_metadata.workflow_run,
            glacier_object_id=new_metadata.archive_id,
        ).key["pk"]
        if result_status:
            logger.debug(f"Archive:{archive_id} - handled_status:{new_status}")
            self.counted_logs.append(
                f"Archive:{archive_id} - counted_status:{new_status}"
            )
            setattr(
                self,
                f"{result_status}_count",
                getattr(self, f"{result_status}_count") + 1,
            )
            setattr(
                self,
                f"{result_status}_size",
                getattr(self, f"{result_status}_size") + new_metadata.size,
            )
        else:
            logger.info(f"Archive:{archive_id} - unhandled_status:{new_status}")
