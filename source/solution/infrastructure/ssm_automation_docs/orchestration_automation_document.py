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
        region: str,
        bucket_name: str,
        allow_cross_region_data_transfer: bool,
    ) -> None:
        super().__init__(ssm_role, document_description)
        self.script = self._read_script()
        self.parameters = self.base_parameters
        self.input_payload: Dict[str, Any] = {
            "provided_inventory": "{{ ProvidedInventory }}",  # Reusing User inputs
            "description": "{{ Description }}",  # Reusing Possible User inputs
            "name_override_presigned_url": "{{ NamingOverrideFile }}",  # Reusing Possible User inputs
            "s3_storage_class": "{{ S3StorageClass }}",
            "acknowledge_cross_region": "{{ AcknowledgeAdditionalCostForCrossRegionTransfer }}",
            "bucket_name": bucket_name,
            "region": region,
            "sns_topic_arn": topic_arn,
            "state_machine_arn": state_machine_arn,
            "allow_cross_region_data_transfer": allow_cross_region_data_transfer,
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
                "allowedPattern": r"^(?:[\S\s]*)$",
            },
            "NamingOverrideFile": {
                "type": "String",
                "default": "",
                "description": "(Optional) Input can be used to provide a pre-signed URL for the naming override file.",
                "allowedPattern": r"^(?:https://[a-zA-Z0-9.-]+\.s3\.[a-zA-Z0-9.-]+\.amazonaws\.com(?:/[^\s]*)?)?$",
            },
            "S3StorageClass": {
                "type": "String",
                "default": next(
                    iter(s3_storage_class_mapping)
                ),  # default storage class
                "allowedValues": list(s3_storage_class_mapping.keys()),
                "description": "(Required) Input to specify the S3 storage class for the transfered archives. See Amazon S3 pricing: https://aws.amazon.com/s3/pricing",
            },
            "AcknowledgeAdditionalCostForCrossRegionTransfer": {
                "type": "String",
                "default": "NO",
                "allowedValues": ["NO", "YES"],
                "description": "(Required) Select 'YES' only if you are aware of the excessive additional cost when selecting a destination bucket in a different region than the S3 Glacier vault. See Amazon S3 Glacier data transfer pricing: https://aws.amazon.com/s3/glacier/pricing/#Data_transfer_pricing",
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
    document_description = (
        "# Document that launches an orchestrator workflow to copy a Glacier vault to an S3 bucket.\n"
        "## (Optional) Download the vault inventory file\n"
        "If you want to download the vault inventory file as part of this migration, follow these [steps](https://docs.aws.amazon.com/solutions/latest/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/step-2-launch-the-transfer-workflow.html#optional-download-the-vault-inventory-file)\n"
        "### Input Parameters:\n"
        " * **AcknowledgeAdditionalCostForCrossRegionTransfer**: (Required) Select 'YES' only if you are aware of the excessive additional cost when selecting a destination bucket in a different region than the S3 Glacier vault. See [Amazon S3 Glacier data transfer pricing](https://aws.amazon.com/s3/glacier/pricing/#Data_transfer_pricing)\n"
        " * **Description**: (Optional) Input can be used to provide an extended description of the document.\n"
        " * **ProvidedInventory**: **No**\n"
        " * **VaultName**: (Required) Input to specify the name of the vault that needs to be retrieved from Glacier and copied to S3.\n"
        " * **NamingOverrideFile**: (Optional) Input can be used to provide a pre-signed URL for the naming override file.\n"
        " * **S3StorageClass**: (Required) Input to specify the S3 storage class for the transfered archives. [Learn more](https://docs.aws.amazon.com/console/s3/storageclasses) or see [Amazon S3 pricing](https://aws.amazon.com/s3/pricing).\n"
        " * **WorkflowRun**: (Optional) Input can be used to provide a workflow identifier. If this field remains empty, the solution assigns a value.\n"
        "  \n"
        "## (Optional) Provide the vault inventory file\n"
        "If you want to provide the vault inventory file, follow these [steps](https://docs.aws.amazon.com/solutions/latest/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/step-2-launch-the-transfer-workflow.html)\n"
        "### Input Parameters:\n"
        " * **AcknowledgeAdditionalCostForCrossRegionTransfer**: (Required) Select 'YES' only if you are aware of the excessive additional cost when selecting a destination bucket in a different region than the S3 Glacier vault. See [Amazon S3 Glacier data transfer pricing](https://aws.amazon.com/s3/glacier/pricing/#Data_transfer_pricing)\n"
        " * **Description**: (Optional) Input can be used to provide an extended description of the document.\n"
        " * **ProvidedInventory**: **Yes**\n"
        " * **VaultName**: (Required) Input to specify the name of the vault that needs to be retrieved from Glacier and copied to S3.\n"
        " * **NamingOverrideFile**: (Optional) Input can be used to provide a pre-signed URL for the naming override file.\n"
        " * **S3StorageClass**: (Required) Input to specify the S3 storage class for the transfered archives. [Learn more](https://docs.aws.amazon.com/console/s3/storageclasses) or see [Amazon S3 pricing](https://aws.amazon.com/s3/pricing).\n"
        " * **WorkflowRun**: (Required) Input to specify the name of your workflow run.\n"
    )

    def __init__(
        self,
        ssm_role: str,
        topic_arn: str,
        state_machine_arn: str,
        region: str,
        bucket_name: str,
        allow_cross_region_data_transfer: bool,
    ) -> None:
        super().__init__(
            ssm_role,
            topic_arn,
            state_machine_arn,
            self.document_description,
            region,
            bucket_name,
            allow_cross_region_data_transfer,
        )
        self.parameters.update(
            {
                "VaultName": {
                    "type": "String",
                    "description": "(Required) Input to specify the name of the vault that needs to be retrieved from Glacier and copied to S3.",
                    "allowedPattern": r"^[a-zA-Z0-9_.-]{1,255}$",
                },
                "WorkflowRun": {
                    "type": "String",
                    "default": "",
                    "description": "(Optional) Input can be used to provide a workflow identifier. If 'ProvidedInventory' is set to 'YES', this field becomes Required.",
                    "allowedPattern": r"^(?!\s).*",
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
    document_description = (
        "# Document that resumes an orchestrator workflow to copy a Glacier vault to an S3 bucket.\n"
        "To resume the transfer workflow, follow these [steps](https://docs.aws.amazon.com/solutions/latest/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/step-3-resume-the-transfer-workflow.html)\n"
        "### Input Parameters:\n"
        " * **AcknowledgeAdditionalCostForCrossRegionTransfer**: (Required) Select 'YES' only if you are aware of the excessive additional cost when selecting a destination bucket in a different region than the S3 Glacier vault. See [Amazon S3 Glacier data transfer pricing](https://aws.amazon.com/s3/glacier/pricing/#Data_transfer_pricing)\n"
        " * **Description**: (Optional) Input can be used to provide an extended description of the document.\n"
        " * **ProvidedInventory**: (Required) Input with two options [YES, NO] to indicate if the inventory is provided.\n"
        " * **WorkflowRun**: (Required) Input to specify the workflow identifier of the workflow that needs to be resumed.\n"
        " * **NamingOverrideFile**: (Optional) Input can be used to provide a pre-signed URL for the naming override file.\n"
        " * **S3StorageClass**: (Required) Input to specify the S3 storage class for the transfered archives. [Learn more](https://docs.aws.amazon.com/console/s3/storageclasses) or see [Amazon S3 pricing](https://aws.amazon.com/s3/pricing).\n"
    )

    def __init__(
        self,
        ssm_role: str,
        topic_arn: str,
        state_machine_arn: str,
        table_name: str,
        region: str,
        bucket_name: str,
        allow_cross_region_data_transfer: bool,
    ) -> None:
        super().__init__(
            ssm_role,
            topic_arn,
            state_machine_arn,
            self.document_description,
            region,
            bucket_name,
            allow_cross_region_data_transfer,
        )
        self.parameters.update(
            {
                "WorkflowRun": {
                    "type": "String",
                    "description": "(Required) Input to specify the workflow identifier of the workflow that needs to be resumed.",
                    "allowedPattern": r"[\S]+",
                },
            }
        )
        self.input_payload.update(
            {
                "workflow_run": "{{ WorkflowRun }}",  # Reusing User inputs
                "table_name": table_name,
            }
        )
