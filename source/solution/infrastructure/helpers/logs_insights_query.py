"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List

from aws_cdk import Aws, CfnOutput, Fn
from aws_cdk import aws_logs as logs

from solution.application.model.glacier_transfer_model import GlacierTransferModel
from solution.application.util.exceptions import ResourceNotFound
from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.workflows.stack_info import StackInfo


class LogsInsightsQuery:
    def __init__(self, stack_info: StackInfo) -> None:
        if stack_info.lambdas.initiate_archive_retrieval_lambda is None:
            raise ResourceNotFound("Initiate archive retrievalLambda")
        if stack_info.lambdas.notifications_processor_lambda is None:
            raise ResourceNotFound("Notifications processor Lambda")
        if stack_info.lambdas.chunk_retrieval_lambda is None:
            raise ResourceNotFound("Chunk retrieval Lambda")
        if stack_info.lambdas.archive_validation_lambda is None:
            raise ResourceNotFound("Archive validation Lambda")
        if stack_info.lambdas.metric_update_on_status_change_lambda is None:
            raise ResourceNotFound("Metric update Lambda")
        if stack_info.lambdas.archives_needing_window_extension_lambda is None:
            raise ResourceNotFound("Archives needing extension Lambda")
        if stack_info.lambdas.extend_download_window_initiate_retrieval_lambda is None:
            raise ResourceNotFound("Extend download window Lambda")

        log_group_names = [
            f"/aws/lambda/{stack_info.lambdas.initiate_archive_retrieval_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.notifications_processor_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.chunk_retrieval_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.archive_validation_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.metric_update_on_status_change_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.archives_needing_window_extension_lambda.function_name}",
            f"/aws/lambda/{stack_info.lambdas.extend_download_window_initiate_retrieval_lambda.function_name}",
        ]

        query_string = "fields @timestamp, @message, @logStream, @log \
        | filter @message like /error/ or @message like /Error/ or @message like /ERROR/ \
        | filter @message not like /segmentation fault Runtime/ \
        | filter @message not like /TransactionConflict/ \
        | sort by @timestamp desc"

        stack_id = Fn.select(2, Fn.split("/", Aws.STACK_ID))

        query_definition = logs.CfnQueryDefinition(
            stack_info.scope,
            "LambdaErrorQueryDefinition",
            name=f"LambdasErrorQuery-{stack_id}",
            query_string=query_string,
            log_group_names=log_group_names,
        )

        stack_info.outputs[OutputKeys.LOGS_INSIGHTS_QUERY_NAME] = CfnOutput(
            stack_info.scope,
            OutputKeys.LOGS_INSIGHTS_QUERY_NAME,
            value=query_definition.name,
        )

        log_group_names = [
            f"/aws/lambda/{stack_info.lambdas.metric_update_on_status_change_lambda.function_name}"
        ]
        for status in (
            GlacierTransferModel.StatusCode.REQUESTED,
            GlacierTransferModel.StatusCode.STAGED,
            GlacierTransferModel.StatusCode.DOWNLOADED,
        ):
            self.add_counter_query(stack_info, log_group_names, status)

    def add_counter_query(
        self, stack_info: StackInfo, log_group_names: List[str], status: str
    ) -> None:
        query_string = f"fields @timestamp, @message, @logStream, @log \
        | filter @message like /counted_status:{status}/ \
        | parse @message '[INFO]	*	*	Archive:*|* - counted_status:{status}' as parsed_time, parsed_id, parsed_workflow, parsed_archive \
        | stats count_distinct(parsed_archive) as unique_downloaded_archives by parsed_workflow"

        stack_id = Fn.select(2, Fn.split("/", Aws.STACK_ID))

        query_definition = logs.CfnQueryDefinition(
            stack_info.scope,
            f"{status}CounterQueryDefinition",
            name=f"{status}CounterQuery-{stack_id}",
            query_string=query_string,
            log_group_names=log_group_names,
        )

        stack_info.outputs[
            f"LogsInsights{status.capitalize()}CounterQueryName"
        ] = CfnOutput(
            stack_info.scope,
            f"LogsInsights{status.capitalize()}CounterQueryName",
            value=query_definition.name,
        )
