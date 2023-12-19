"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import os
import re
from typing import Any, Dict, Optional

import yaml

from solution.infrastructure.output_keys import OutputKeys
from solution.infrastructure.ssm_automation_docs.scripts.orchestration_doc_script import (
    s3_storage_class_mapping,
)
from solution.infrastructure.ssm_automation_docs.ssm_document_template import (
    ExecuteScriptAutomationDocument,
)


class BaseAutomationDocument(ExecuteScriptAutomationDocument):
    def __init__(
        self,
        ssm_role: str,
        topic_arn: str,
        state_machine_arn: str,
        document_description: str,
    ) -> None:
        super().__init__(ssm_role, document_description)
        self.script = self._read_script()
        self.parameters = self.base_parameters
        self.input_payload: Dict[str, Any] = {
            "provided_inventory": "{{ ProvidedInventory }}",  # Reusing User inputs
            "description": "{{ Description }}",  # Reusing Possible User inputs
            "name_override_presigned_url": "{{ NamingOverrideFile }}",  # Reusing Possible User inputs
            "s3_storage_class": "{{ S3StorageClass }}",
            "sns_topic_arn": topic_arn,
            "state_machine_arn": state_machine_arn,
        }

    @property
    def base_parameters(self) -> Dict[str, Any]:
        # User Input through SSM Automation Document Execution
        return {
            "ProvidedInventory": {
                "type": "String",
                "allowedValues": ["NO", "YES"],
                "description": "(Required) Input with two options [YES, NO] to indicate if the inventory is provided.",
            },
            "Description": {
                "type": "String",
                "default": "",
                "description": "(Optional) Input can be used to provide an extended description of the document.",
                "allowedPattern": "^(?:[\S\s]*)$",
            },
            "NamingOverrideFile": {
                "type": "String",
                "default": "",
                "description": "(Optional) Input can be used to provide a pre-signed URL for naming override file.",
                "allowedPattern": "^(?:https://[a-zA-Z0-9.-]+\.s3\.[a-zA-Z0-9.-]+\.amazonaws\.com(?:/[^\s]*)?)?$",
            },
            "S3StorageClass": {
                "type": "String",
                "default": next(
                    iter(s3_storage_class_mapping)
                ),  # default storage class
                "allowedValues": list(s3_storage_class_mapping.keys()),
                "description": "(Required) Input to specify the S3 storage class",
            },
        }

    @staticmethod
    def _read_script() -> str:
        with open(
            f"{os.path.dirname(os.path.abspath(__file__))}/scripts/orchestration_doc_script.py"
        ) as f:
            return f.read()

    @property
    def yaml_document(self) -> str:
        self._create_document()
        return yaml.dump(self.document)

    def _extract_func_name(self) -> str:
        match = re.search(r"def\s+(\w+)", self.script)
        if match is None:
            raise ValueError("Could not extract function name")
        return match.group(1)

    def _create_document(self) -> None:
        self.document["parameters"] = self.parameters
        self.document["mainSteps"][0]["inputs"].update(
            {
                "Handler": self._extract_func_name(),
                "Script": self.script,
                "InputPayload": self.input_payload,
            }
        )


class LaunchAutomationDocument(BaseAutomationDocument):
    document_description = "Document that launches an orchestrator workflow to copy a Glacier Vault to an S3 Bucket"

    def __init__(
        self,
        ssm_role: str,
        topic_arn: str,
        state_machine_arn: str,
    ) -> None:
        super().__init__(
            ssm_role, topic_arn, state_machine_arn, self.document_description
        )
        self.parameters.update(
            {
                "VaultName": {
                    "type": "String",
                    "description": "(Required) Input to specify the name of the vault that needs to be retrieved from Glacier and copied to S3",
                    "allowedPattern": "^[a-zA-Z0-9_.-]{1,255}$",
                },
                "WorkflowRun": {
                    "type": "String",
                    "default": "",
                    "description": "(Optional) Input can be used to provide a workflow identifier. If 'ProvidedInventory' is set to 'YES,' this field becomes Required.",
                    "allowedPattern": "^(?!\s).*",
                },
            }
        )
        self.input_payload.update(
            {
                "vault_name": "{{ VaultName }}",  # Reusing User inputs
                "workflow_run": "{{ WorkflowRun }}",  # Reusing User inputs
            }
        )


class ResumeAutomationDocument(BaseAutomationDocument):
    document_description = "Document that resumes an orchestrator workflow to copy a Glacier Vault to an S3 Bucket"

    def __init__(
        self,
        ssm_role: str,
        topic_arn: str,
        state_machine_arn: str,
        table_name: str,
    ) -> None:
        super().__init__(
            ssm_role, topic_arn, state_machine_arn, self.document_description
        )
        self.parameters.update(
            {
                "WorkflowRun": {
                    "type": "String",
                    "description": "(Required) Input to specify the workflow identifier of the workflow that needs to be resumed.",
                    "allowedPattern": "[\S]+",
                },
            }
        )
        self.input_payload.update(
            {
                "workflow_run": "{{ WorkflowRun }}",  # Reusing User inputs
                "table_name": table_name,
            }
        )
