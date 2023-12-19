"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any

from aws_cdk.aws_dynamodb import BillingMode, CfnTable, Table
from cdk_nag import NagSuppressions
from constructs import Construct

DEFAULT_BILLING_MODE = BillingMode.PAY_PER_REQUEST
DEFAULT_POINT_IN_TIME_RECOVERY = True


class SolutionsTable(Table):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        enable_ddb_backup: str,
        **kwargs: Any,
    ):
        self.scope = scope
        self.construct_id = construct_id

        # set billing_mode to PAY_PER_REQUEST unless a runtime is passed
        if not kwargs.get("billing_mode"):
            kwargs["billing_mode"] = DEFAULT_BILLING_MODE

        # set point_in_time_recovery to to True unless another configuration is specified.
        # This value will be overridden based on the value of EnableDDBBackupParameter input.
        if not kwargs.get("point_in_time_recovery"):
            kwargs["point_in_time_recovery"] = DEFAULT_POINT_IN_TIME_RECOVERY

        # initialize the parent Function
        super().__init__(scope, construct_id, **kwargs)

        self._set_point_in_time_recovery_enabled(enable_ddb_backup)

    def _set_point_in_time_recovery_enabled(self, enable_ddb_backup: str) -> None:
        assert isinstance(self.node.default_child, CfnTable)

        cfn_table = self.node.default_child
        cfn_table.add_override(
            "Properties.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled",
            enable_ddb_backup,
        )
