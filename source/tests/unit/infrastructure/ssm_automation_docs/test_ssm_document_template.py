"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest

from solution.infrastructure.ssm_automation_docs.ssm_document_template import (
    ExecuteScriptAutomationDocument,
)


def test_execute_script_automation_document_init() -> None:
    doc = ExecuteScriptAutomationDocument("ssm_role_arn", "description")

    assert doc.document["schemaVersion"] == "0.3"
    assert doc.document["assumeRole"] == "ssm_role_arn"
    assert doc.document["parameters"] == None
    assert doc.document["mainSteps"][0]["name"] == "runScript"
    assert doc.document["mainSteps"][0]["action"] == "aws:executeScript"
    assert doc.document["mainSteps"][0]["inputs"]["Runtime"] == "python3.8"
    assert doc.document["mainSteps"][0]["inputs"]["Handler"] == None
    assert doc.document["mainSteps"][0]["inputs"]["Script"] == None
    assert doc.document["mainSteps"][0]["inputs"]["InputPayload"] == None
