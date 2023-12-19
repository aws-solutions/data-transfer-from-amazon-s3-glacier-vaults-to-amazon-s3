"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from typing import Any, Dict


class ExecuteScriptAutomationDocument:
    def __init__(self, ssm_role_arn: str, description: str) -> None:
        # A template provided by this class to all the child classes
        self.document: Dict[str, Any] = {
            "description": description,
            "schemaVersion": "0.3",
            "assumeRole": ssm_role_arn,
            "parameters": None,
            "mainSteps": [
                {
                    "name": "runScript",
                    "action": "aws:executeScript",
                    "inputs": {
                        "Runtime": "python3.8",
                        "Handler": None,
                        "Script": None,
                        "InputPayload": None,
                    },
                }
            ],
        }
