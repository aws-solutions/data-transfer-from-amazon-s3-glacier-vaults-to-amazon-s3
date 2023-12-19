"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import json
from typing import Any, Iterator, Type
from unittest.mock import Mock

import pytest
from mypy_boto3_glacier import GlacierClient

from solution.application.archive_retrieval import (
    notification_processor as archive_notification_processor,
)
from solution.application.facilitator import processor
from solution.application.handlers import (
    archive_retrieval,
    async_facilitator,
    notifications_processor,
)

mocked_sqs_event: dict[str, Any] = {
    "Records": [
        {
            "messageId": "1",
            "receiptHandle": "1",
            "body": json.dumps({"Message": json.dumps({"Action": "ArchiveRetrieval"})}),
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1",
                "SenderId": "1",
                "ApproximateFirstReceiveTimestamp": "1",
            },
            "messageAttributes": {},
            "md5OfBody": "1",
            "eventSource": "aws:sqs",
            "eventSourceARN": "1",
            "awsRegion": "1",
        }
    ]
}

mocked_dynamodb_stream_event: dict[str, Any] = {
    "Records": [
        {
            "eventID": "1",
            "eventName": "INSERT",
            "eventVersion": "1.0",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-west-2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1577836800,
                "Keys": {"id": {"S": "1"}},
                "NewImage": {
                    "id": {"S": "1"},
                    "name": {"S": "test_name"},
                    "description": {"S": "test_description"},
                    "created_at": {"S": "2020-01-01T00:00:00"},
                    "updated_at": {"S": "2020-01-01T00:00:00"},
                },
                "SequenceNumber": "1",
                "SizeBytes": 26,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }
    ]
}


class TestNotificationProcessor:
    @pytest.fixture
    def mock_handle_archive_job_notification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> Mock:
        mock_client = Mock()
        monkeypatch.setattr(
            archive_notification_processor,
            "handle_archive_job_notification",
            mock_client,
        )
        return mock_client

    @pytest.fixture
    def mock_handle_job_notification(self, monkeypatch: pytest.MonkeyPatch) -> Mock:
        mock_client = Mock()
        monkeypatch.setattr(
            processor,
            "handle_job_notification",
            mock_client,
        )
        return mock_client

    @pytest.mark.parametrize("invalid_event, error", [("", TypeError), ({}, KeyError)])
    def test_for_invalid_event(
        self, invalid_event: str | dict[str, Any], error: Type[Exception]
    ) -> None:
        with pytest.raises(error):
            notifications_processor(invalid_event, None)

    def test_for_archive_retrieval_event(
        self, mock_handle_archive_job_notification: Mock
    ) -> None:
        assert notifications_processor(mocked_sqs_event, None) is None
        mock_handle_archive_job_notification.assert_called_once_with(
            json.loads(str(mocked_sqs_event["Records"][0]["body"]))["Message"]
        )

    def test_for_inventory_retrieval_event(
        self,
        mock_handle_archive_job_notification: Mock,
        mock_handle_job_notification: Mock,
    ) -> None:
        # Arrange
        mocked_sqs_event["Records"][0]["body"] = json.dumps(
            {"Message": json.dumps({"Action": "InventoryRetrieval"})}
        )

        # Act/Assert
        assert notifications_processor(mocked_sqs_event, None) is None
        mock_handle_archive_job_notification.assert_not_called()
        mock_handle_job_notification.assert_called_once_with(
            json.loads(str(mocked_sqs_event["Records"][0]["body"]))["Message"]
        )


class TestAsyncFacilitator:
    @pytest.fixture
    def mock_handle_record_changed(self, monkeypatch: pytest.MonkeyPatch) -> Mock:
        mock_client = Mock()
        monkeypatch.setattr(
            processor,
            "handle_record_changed",
            mock_client,
        )
        return mock_client

    @pytest.mark.parametrize("invalid_event, error", [("", TypeError), ({}, KeyError)])
    def test_for_invalid_event(
        self, invalid_event: str | dict[str, Any], error: Type[Exception]
    ) -> None:
        with pytest.raises(error):
            async_facilitator(invalid_event, None)

    def test_for_insert_record_event(self, mock_handle_record_changed: Mock) -> None:
        async_facilitator(mocked_dynamodb_stream_event, None)
        mock_handle_record_changed.assert_called_once_with(
            mocked_dynamodb_stream_event["Records"][0]["dynamodb"]["NewImage"]
        )

    def test_for_remove_record_event(self, mock_handle_record_changed: Mock) -> None:
        mocked_dynamodb_stream_event["Records"][0]["eventName"] = "REMOVE"
        async_facilitator(mocked_dynamodb_stream_event, None)
        mock_handle_record_changed.assert_not_called()


class TestArchiveRetrieval:
    @pytest.fixture
    def mock_glacier_to_s3_facilitator(self, monkeypatch: pytest.MonkeyPatch) -> Mock:
        mocked_sqs_event["Records"][0]["body"] = json.dumps(
            {
                "VaultName": "test_vault",
                "WorkflowRun": "test_workflow_run",
                "ByteRange": "test_byte_range",
                "GlacierObjectId": "test_glacier_object_id",
                "S3DestinationBucket": "XXXXXXXXXXXXXXXXXXXXXXXXXX",
                "S3DestinationKey": "XXXXXXXXXXXXXXXXXXXXXXXXXX",
                "UploadId": "XXXXXXXXXXXXXXXXXXXXXXXXXX",
                "PartNumber": "XXXXXXXXXXXXXXXXXXXXXXXXXX",
            }
        )
        mock_client = Mock()
        monkeypatch.setattr(
            "solution.application.handlers.GlacierToS3Facilitator",
            mock_client,
        )
        return mock_client

    @pytest.mark.parametrize("invalid_event, error", [("", TypeError), ({}, KeyError)])
    def test_for_invalid_event(
        self, invalid_event: str | dict[str, Any], error: Type[Exception]
    ) -> None:
        with pytest.raises(error):
            archive_retrieval(invalid_event, None)

    def test_for_valid_event(
        self,
        mock_glacier_to_s3_facilitator: Mock,
        glacier_client: Iterator[GlacierClient],
    ) -> None:
        archive_retrieval(mocked_sqs_event, None)
        mock_glacier_to_s3_facilitator.assert_called_once()
        mock_glacier_to_s3_facilitator.return_value.transfer.assert_called_once_with()
        mock_glacier_to_s3_facilitator.return_value.send_validation_event.assert_called_once_with()

        # check for send_validation_event() not called
        mock_glacier_to_s3_facilitator.reset_mock()
        mock_glacier_to_s3_facilitator.return_value.transfer.return_value = None
        archive_retrieval(mocked_sqs_event, None)
        mock_glacier_to_s3_facilitator.return_value.transfer.assert_called_once_with()
        mock_glacier_to_s3_facilitator.return_value.send_validation_event.assert_not_called()
