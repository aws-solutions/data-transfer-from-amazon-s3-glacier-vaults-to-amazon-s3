"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


class OutputKeys:
    ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN = "ArchiveChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    NOTIFICATIONS_SQS_URL = "NotificationsSQSUrl"
    CHUNKS_SQS_URL = "ChunksSQSUrl"
    VALIDATION_SQS_URL = "ValidationSQSUrl"
    NOTIFICATIONS_SQS_ARN = "NotificationsSQSArn"
    CHUNKS_SQS_ARN = "ChunksSQSArn"
    VALIDATION_SQS_ARN = "ValidationSQSArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"
    CHUNK_RETRIEVAL_LAMBDA_ARN = "ChunkRetrievalLambdaArn"
    CHUNK_RETRIEVAL_LAMBDA_NAME = "ChunkRetrievalLambdaName"
    INITIATE_INVENTORY_RETRIEVAL_LAMBDA_NAME = "InitiateInventoryRetrievalLambdaName"
    INITIATE_INVENTORY_RETRIEVAL_LAMBDA_ARN = "InitiateInventoryRetrievalLambdaArn"
    INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_ARN = "InitiateArchiveRetrievalLambdaArn"
    INITIATE_ARCHIVE_RETRIEVAL_LAMBDA_NAME = "InitiateArchiveRetrievalLambdaName"
    ARCHIVE_VALIDATION_LAMBDA_ARN = "ArchiveValidationLambdaArn"
    INVENTORY_CHUNK_RETRIEVAL_LAMBDA_NAME = "InventoryChunkRetrievalLambdaName"
    INVENTORY_RETRIEVAL_STATE_MACHINE_ARN = "InventoryRetrievalStateMachineArn"
    INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN = "InventoryChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_LAMBDA_NAME = "AsyncFacilitatorLambdaName"
    INITIATE_RETRIEVAL_STATE_MACHINE_ARN = "InitiateRetrievalStateMachineArn"
    RETRIEVE_ARCHIVE_STATE_MACHINE_ARN = "RetrieveArchiveStateMachineArn"
    GLACIER_RETRIEVAL_TABLE_NAME = "GlacierRetrievalTableName"
    INVENTORY_VALIDATION_LAMBDA_ARN = "InventoryValidationLambdaArn"
    CLEANUP_STATE_MACHINE_ARN = "CleanupStateMachineArn"
    ORCHESTRATOR_STATE_MACHINE_ARN = "OrchestratorStateMachineArn"
    GLACIER_RETRIEVAL_INDEX_NAME = "GlacierRetrievalIndexName"
    EXTEND_DOWNLOAD_WINDOW_STATE_MACHINE_ARN = "ExtendDownloadWindowStateMachineArn"
    EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA = (
        "ExtendDownloadWindowInitiateRetrievalLambdaName"
    )
    EXTEND_DOWNLOAD_WINDOW_INITIATE_RETRIEVAL_LAMBDA_ARN = (
        "ExtendDownloadWindowInitiateRetrievalLambdaArn"
    )
    METRIC_TABLE_NAME = "MetricTableName"
    ORPHANED_GATES_CLEANUP_LAMBDA_ARN = "OrphanedGatesCleanupLambdaArn"
    CLOUDWATCH_DASHBOARD_UPDATE_STATE_MACHINE_ARN = (
        "CloudwatchDashboardUpdateStateMachineArn"
    )
    SSM_AUTOMATION_RUNBOOK_NAME = "SSMAutomationRunbookName"
    SSM_LAUNCH_AUTOMATION_RUNBOOK_NAME = "SSMLaunchAutomationRunbookName"
    SSM_RESUME_AUTOMATION_RUNBOOK_NAME = "SSMResumeAutomationRunbookName"
    GLACIER_RETRIEVAL_JOB_INDEX_NAME = "GlacierRetrievalJobIndexName"
    ARCHIVES_STATUS_CLEANUP_STATE_MACHINE_ARN = "ArchivesStatusCleanupStateMachineArn"
    MOCK_NOTIFY_SNS_LAMBDA_ARN = "MockNotifySnsLambdaArn"
    CLOUDWATCH_DASHBOARD_UPDATE_RULE_NAME = "CloudWatchDashboardUpdateRuleName"
    WORKFLOW_COMPLETION_CHECKER_RULE_NAME = "WorkflowCompletionCheckerRuleName"
    EXTEND_DOWNLOAD_WINDOW_RULE_NAME = "ExtendDownloadWindowRuleName"
    LOGS_INSIGHTS_QUERY_NAME = "LogsInsightsQueryName"
    LOGS_INSIGHTS_REQUESTED_COUNTER_QUERY_NAME = "LogsInsightsRequestedCounterQueryName"
    LOGS_INSIGHTS_STAGED_COUNTER_QUERY_NAME = "LogsInsightsStagedCounterQueryName"
    LOGS_INSIGHTS_DOWNLOADED_COUNTER_QUERY_NAME = (
        "LogsInsightsDownloadedCounterQueryName"
    )
    METRIC_UPDATE_LAMBDA_NAME = "MetricUpdateLambdaName"
