"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from dataclasses import dataclass
from typing import Any, Dict

from solution.application.model.base import Model


@dataclass
class GlacierTransferModel(Model):
    class StatusCode:
        REQUESTED = "requested"
        EXTENDED = "extended"
        STAGED = "staged"
        DOWNLOADED = "downloaded"
        STOPPED = "stopped"

    workflow_run: str = Model.field(
        ["pk", "S"],
        composite_index=0,
    )
    glacier_object_id: str = Model.field(
        ["pk", "S"],
        composite_index=1,
    )

    _pk: str = Model.view(["pk", "S"], ["workflow_run", "glacier_object_id"])

    @property
    def key(self) -> Dict[str, Any]:
        return {k: v for k, v in self.marshal().items() if k in ("pk", "sk")}

    @staticmethod
    def format_partition_keys_for_step_function(
        run_id_path: str, glacier_object_id_path: str
    ) -> str:
        return f"States.Format('{{}}|{{}}', {run_id_path}, {glacier_object_id_path})"
