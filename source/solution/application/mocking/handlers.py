"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any, Dict

from solution.application.mocking.notify_sns import notify_sns_job_completion


def mock_notify_sns_handler(event: Dict[str, Any], _context: Any) -> None:
    account_id = event.get("account_id", "testing_account_id")
    vault_name = event.get("vault_name", "testing_vault_name")
    sns_topic = event.get("sns_topic", "testing_sns_topic")
    job_id = event.get("job_id", "testing_job_id")
    retrieval_type = event.get("retrieval_type", "testing_retrieval_type")
    archive_id = event.get("archive_id", "testing_archive_id")
    notify_sns_job_completion(
        account_id, vault_name, job_id, sns_topic, retrieval_type, archive_id
    )
