"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import io
import json
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

import boto3
from botocore.response import StreamingBody

from solution.application import __boto_config__
from solution.application.glacier_service.glacier_typing import GlacierJobType
from solution.application.mocking.mock_glacier_data import MOCK_DATA

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import JobParametersTypeDef
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object
    GlacierClient = object
    JobParametersTypeDef = object


class MockGlacierAPIs(GlacierClient):
    def __init__(self) -> None:
        self.MOCK_DATA = MOCK_DATA
        self.output_mapping: Dict[str, Any] = self.MOCK_DATA

    def get_job_output(
        self, *, vaultName: str, jobId: str, accountId: str = "-", range: str = ""
    ) -> Any:
        output = json.loads(
            json.dumps(self.output_mapping[vaultName]["get-job-output"][jobId])
        )
        if range != "":
            output = output[range]
        body = output["body"]
        output["body"] = StreamingBody(io.BytesIO(bytes(body, "utf-8")), len(body))
        return output

    def initiate_job(
        self,
        *,
        vaultName: str,
        accountId: str = "-",
        jobParameters: JobParametersTypeDef = {},
    ) -> Any:
        access_string = jobParameters["Type"]
        if archive_id := jobParameters.get("ArchiveId"):
            access_string = f"{access_string}:{archive_id}"
        result = self.output_mapping[vaultName]["initiate-job"][access_string]
        if sns_topic := jobParameters.get("SNSTopic"):
            client: LambdaClient = boto3.client("lambda", config=__boto_config__)
            function_params = {
                "account_id": accountId,
                "vault_name": vaultName,
                "sns_topic": sns_topic,
                "job_id": result.get("jobId", ""),
                "retrieval_type": GlacierJobType.INVENTORY_RETRIEVAL
                if archive_id is None
                else GlacierJobType.ARCHIVE_RETRIEVAL,
                "archive_id": jobParameters.get("ArchiveId"),
            }
            client.invoke(
                FunctionName=os.environ["MOCK_NOTIFY_SNS_LAMBDA_ARN"],
                InvocationType="Event",
                Payload=json.dumps(function_params),
            )
        return result
