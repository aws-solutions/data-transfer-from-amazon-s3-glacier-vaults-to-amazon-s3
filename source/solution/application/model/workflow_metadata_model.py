"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from dataclasses import dataclass
from typing import Any, ClassVar, Dict

from solution.application.model.base import Model


@dataclass
class WorkflowMetadataRecord(Model):
    workflow_run: str = Model.field(
        ["pk", "S"],
        composite_index=0,
    )

    vault_name: str = Model.field(["vault_name", "S"])
    start_time: str = Model.field(["start_time", "S"])
    daily_quota: int | None = Model.field(
        ["daily_quota", "N"], optional=True, marshal_as=str
    )
    storage_class: str = Model.field(["storage_class", "S"])
    retrieval_tier: str = Model.field(["retrieval_tier", "S"])

    sk: ClassVar[str] = "meta"

    _pk: str = Model.view(["pk", "S"], "workflow_run")
    _sk: str = Model.view(["sk", "S"], "sk")

    @property
    def key(self) -> Dict[str, Any]:
        return {k: v for k, v in self.marshal().items() if k in ("pk", "sk")}
