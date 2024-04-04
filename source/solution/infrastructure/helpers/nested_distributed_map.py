"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any, Dict, List, Optional

from aws_cdk import Aws, CfnElement, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from cdk_nag import NagSuppressions
from constructs import Construct

from solution.infrastructure.helpers.distributed_map import (
    DistributedMap,
    ItemReaderConfig,
    ResultConfig,
)


class NestedDistributedMap:
    def __init__(
        self,
        scope: Construct,
        nested_distributed_map_id: str,
        definition: sfn.IChainable,
        item_selector: Dict[str, Any],
        inventory_bucket: s3.Bucket,
        max_concurrency: Optional[int] = None,
        inner_max_concurrency: Optional[int] = None,
        retry: Optional[List[Dict[str, Any]]] = None,
        max_items_per_batch: Optional[int] = None,
    ) -> None:
        inner_item_reader_config = ItemReaderConfig(
            item_reader_resource="arn:aws:states:::s3:getObject",
            reader_config={
                "InputType": "CSV",
                "CSVHeaderLocation": "FIRST_ROW",
            },
            item_reader_parameters={"Bucket.$": "$.bucket", "Key.$": "$.item.Key"},
        )
        inner_result_config = ResultConfig(
            result_writer={
                "Resource": "arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": inventory_bucket.bucket_name,
                    "Prefix.$": f"States.Format('{{}}/{nested_distributed_map_id}InnerDistributedMapOutput', $.workflow_run)",
                },
            },
            result_path="$.map_result",
        )
        inner_distributed_map_state = DistributedMap(
            scope,
            f"{nested_distributed_map_id}InnerDistributedMap",
            definition=definition,
            item_reader_config=inner_item_reader_config,
            result_config=inner_result_config,
            max_concurrency=inner_max_concurrency,
            item_selector={
                **item_selector,
                "bucket.$": "$.bucket",
                "key.$": "$.item.Key",
                "item.$": "$$.Map.Item.Value",
            },
            retry=retry,
            max_items_per_batch=max_items_per_batch,
        )

        item_reader_config = ItemReaderConfig(
            item_reader_resource="arn:aws:states:::s3:listObjectsV2",
            item_reader_parameters={
                "Bucket": inventory_bucket.bucket_name,
                "Prefix.$": "$.prefix",
            },
        )
        result_config = ResultConfig(
            result_writer={
                "Resource": "arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": inventory_bucket.bucket_name,
                    "Prefix.$": f"States.Format('{{}}/{nested_distributed_map_id}DistributedMapOutput', $.workflow_run)",
                },
            },
            result_path="$.map_result",
        )

        self.distributed_map_state = DistributedMap(
            scope,
            f"{nested_distributed_map_id}DistributedMap",
            definition=inner_distributed_map_state,
            item_reader_config=item_reader_config,
            result_config=result_config,
            max_concurrency=max_concurrency,
            item_selector={
                **item_selector,
                "bucket": inventory_bucket.bucket_name,
                "item.$": "$$.Map.Item.Value",
            },
        )

    def configure_step_function(
        self,
        scope: Construct,
        id: str,
        state_machine: sfn.StateMachine,
        bucket: s3.Bucket,
    ) -> None:
        bucket.grant_read_write(state_machine)

        state_machine_policy = iam.Policy(
            scope,
            f"{id}StateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[state_machine.state_machine_arn],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        state_machine_policy.attach_to_role(state_machine.role)

        assert isinstance(bucket.node.default_child, CfnElement)
        bucket_logical_id = Stack.of(scope).get_logical_id(bucket.node.default_child)

        NagSuppressions.add_resource_suppressions(
            state_machine.role.node.find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required for Step Function logging",
                    "appliesTo": ["Resource::*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required to export the results of the Distributed Map state to S3 bucket",
                    "appliesTo": ["Action::s3:Abort*", "Action::s3:DeleteObject*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for reading a file as dataset in a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::<{bucket_logical_id}.Arn>/*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )

        assert isinstance(state_machine.node.default_child, CfnElement)
        state_machine_logical_id = Stack.of(scope).get_logical_id(
            state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{state_machine_logical_id}.Name>/*"
                    ],
                }
            ],
        )
