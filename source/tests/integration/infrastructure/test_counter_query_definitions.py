"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List

from solution.infrastructure.output_keys import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_logs import CloudWatchLogsClient
    from mypy_boto3_logs.type_defs import InputLogEventTypeDef
else:
    CloudWatchLogsClient = object
    InputLogEventTypeDef = object
    LambdaClient = object


def create_log_stream_and_add_logs_entry(
    logs_client: CloudWatchLogsClient,
    workflow_id: str,
    archives_count: int,
    log_group_name: str,
    log_stream_name: str,
) -> None:
    event_time = int(datetime.now().timestamp()) * 1000

    response = logs_client.create_log_stream(
        logGroupName=log_group_name, logStreamName=log_stream_name
    )

    log_entries: List[InputLogEventTypeDef] = []
    for i in range(archives_count):
        log_entries.append(
            {
                "timestamp": event_time,
                "message": f"[INFO]	2020-01-01T01:01:01.001Z	7d95ff3b-23ac-408e-b215-98f903b28df{i}	Archive:{workflow_id}|test_archive_id_{i} - counted_status:downloaded",
            }
        )

    response = logs_client.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=log_entries,
    )


def test_downloaded_counter_query(
    lambda_client: LambdaClient, logs_client: CloudWatchLogsClient
) -> None:
    lambda_client.invoke(
        FunctionName=os.environ[OutputKeys.METRIC_UPDATE_LAMBDA_NAME],
        InvocationType="RequestResponse",
        Payload="",
    )
    # Add 20 secs delay to allow the creation of the logGroup
    time.sleep(20)

    response = logs_client.describe_query_definitions()
    for query_definition in response["queryDefinitions"]:
        if (
            query_definition["name"]
            == os.environ[OutputKeys.LOGS_INSIGHTS_DOWNLOADED_COUNTER_QUERY_NAME]
        ):
            timestamp = int(datetime.now().timestamp()) * 1000
            log_group_name = str(query_definition["logGroupNames"][0])
            log_stream_name = f"test_log_stream_{timestamp}"
            workflow_id = f"test_workflow_id_{timestamp}"

            archives_count = 5
            create_log_stream_and_add_logs_entry(
                logs_client,
                workflow_id,
                archives_count,
                log_group_name,
                log_stream_name,
            )

            # Add 3 mins delay to allow the logs events to be ingested into CloudWatch Logs
            time.sleep(180)

            query_response = logs_client.start_query(
                logGroupNames=query_definition["logGroupNames"],
                startTime=int((datetime.now() - timedelta(minutes=10)).timestamp())
                * 1000,
                endTime=int(datetime.now().timestamp()) * 1000,
                queryString=query_definition["queryString"],
                limit=100,
            )

            query_id = query_response["queryId"]

            start_time = time.time()
            while time.time() - start_time < 30:
                query_status = logs_client.get_query_results(queryId=query_id)
                if query_status["status"] == "Complete":
                    break
                time.sleep(1)

            results = query_status["results"]

            assert any(
                result[0]["value"] == workflow_id
                and result[1]["value"] == str(archives_count)
                for result in results
            )
