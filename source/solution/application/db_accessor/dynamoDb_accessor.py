"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import boto3

from solution.application import __boto_config__

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    DynamoDBClient = object

logger = logging.getLogger()
logger.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


class DynamoDBAccessor:
    def __init__(
        self, table_name: str, client: Optional[DynamoDBClient] = None
    ) -> None:
        self.dynamodb: DynamoDBClient = client or boto3.client(
            "dynamodb", config=__boto_config__
        )
        self.table_name = table_name

    def insert_item(self, item: Dict[str, Any]) -> None:
        self.dynamodb.put_item(TableName=self.table_name, Item=item)
        logger.info("Successfully inserted item into the database")

    def get_item(
        self, key: Dict[str, Any], consistent_read: bool = False
    ) -> Optional[Dict[str, Any]]:
        response = self.dynamodb.get_item(
            TableName=self.table_name, Key=key, ConsistentRead=consistent_read
        )
        return response.get("Item")

    def update_item(
        self,
        key: Dict[str, Any],
        update_expression: str,
        expression_attribute_values: Dict[str, Any],
    ) -> None:
        self.dynamodb.update_item(
            TableName=self.table_name,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        logger.info("Successfully updated the database")

    def delete_item(self, key: Dict[str, Any]) -> None:
        self.dynamodb.delete_item(TableName=self.table_name, Key=key)
        logger.info("Successfully deleted item from the database")

    def query_items(
        self,
        primary_key_expression: str,
        key_mapping: Dict[str, Any],
        sort_key_expression: Optional[str] = None,
        consistent_read: bool = False,
    ) -> List[Dict[str, Any]]:
        key_expression = primary_key_expression
        if sort_key_expression:
            key_expression = f"{primary_key_expression} AND {sort_key_expression}"

        results: List[Dict[str, Any]] = []
        response = self.dynamodb.query(
            TableName=self.table_name,
            ExpressionAttributeValues=key_mapping,
            KeyConditionExpression=key_expression,
            ConsistentRead=consistent_read,
        )
        results.extend(response.get("Items", []))
        while key := response.get("LastEvaluatedKey"):
            response = self.dynamodb.query(
                TableName=self.table_name,
                ExpressionAttributeValues=key_mapping,
                KeyConditionExpression=key_expression,
                ExclusiveStartKey=key,
            )
            results.extend(response.get("Items", []))
        return results
