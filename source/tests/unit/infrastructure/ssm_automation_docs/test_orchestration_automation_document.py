"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from unittest import mock

import pytest
import yaml

from solution.infrastructure.ssm_automation_docs.orchestration_automation_document import (
    LaunchAutomationDocument,
    ResumeAutomationDocument,
)


def test_launch_yaml_doc() -> None:
    with mock.patch(
        "builtins.open", mock.mock_open(read_data="def func_name(): pass")
    ) as mock_open:
        doc = LaunchAutomationDocument("ssm_role", "topic_arn", "state_machine_arn")
        # create an instance of the class

        # assert that the _extract_func_name method was called
        mock_open.assert_called()
        # get the yaml document
        yaml_doc = yaml.safe_load(doc.yaml_document)
        # assert that the yaml document is correctly formed
        assert "VaultName" in yaml_doc["parameters"]
        assert "(Optional)" in yaml_doc["parameters"]["WorkflowRun"]["description"]


def test_resume_yaml_doc() -> None:
    with mock.patch(
        "builtins.open", mock.mock_open(read_data="def func_name(): pass")
    ) as mock_open:
        doc = ResumeAutomationDocument(
            "ssm_role", "topic_arn", "state_machine_arn", "table_name"
        )
        # create an instance of the class

        # assert that the _extract_func_name method was called
        mock_open.assert_called()
        # get the yaml document
        yaml_doc = yaml.safe_load(doc.yaml_document)
        # assert that the yaml document is correctly formed
        assert "WorkflowRun" in yaml_doc["parameters"]
