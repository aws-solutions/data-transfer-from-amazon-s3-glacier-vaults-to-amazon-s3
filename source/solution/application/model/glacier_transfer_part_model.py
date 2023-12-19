"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from dataclasses import dataclass
from typing import ClassVar

from solution.application.model.base import Model
from solution.application.model.glacier_transfer_model import GlacierTransferModel


@dataclass
class GlacierTransferPartRead(GlacierTransferModel):
    part_number: int = Model.field(["part_number", "N"], composite_index=0)
    _part_number: str = Model.view(["part_number", "N"], parts="part_number")
    prefix: ClassVar[str] = "p"

    _sk: str = Model.view(["sk", "S"], ["prefix", "padded_part_number"])

    @property
    def padded_part_number(self) -> str:
        return str(self.part_number).zfill(5)


@dataclass
class GlacierTransferPart(GlacierTransferPartRead):
    checksum_sha_256: str = Model.field(["checksum_sha_256", "S"])
    e_tag: str = Model.field(["e_tag", "S"])
    tree_checksum: str | None = Model.field(["tree_checksum", "S"], optional=True)
