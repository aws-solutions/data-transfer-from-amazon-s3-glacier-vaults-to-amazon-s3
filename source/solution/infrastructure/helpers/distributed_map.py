"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any, Dict, List, Optional

from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct


class DistributedMap(sfn.CustomState):
    def __init__(
        self,
        scope: Construct,
        distributed_map_id: str,
        definition: sfn.IChainable,
        execution_type: Optional[sfn.StateMachineType] = sfn.StateMachineType.STANDARD,
        max_concurrency: Optional[int] = None,
        item_reader_resource: Optional[str] = None,
        items_path: Optional[str] = None,
        reader_config: Optional[Dict[str, Any]] = None,
        item_reader_parameters: Optional[Dict[str, Any]] = None,
        item_selector: Optional[Dict[str, Any]] = None,
        result_selector: Optional[Dict[str, Any]] = None,
        result_writer: Optional[Dict[str, Any]] = None,
        result_path: Optional[str] = None,
        max_items_per_batch: Optional[int] = None,
        catch: Optional[List[Dict[str, Any]]] = None,
        retry: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        inline_map = sfn.Map(scope, f"{distributed_map_id}InlineMap")
        inline_map.iterator(definition)
        map_iterator = inline_map.to_state_json()["Iterator"]

        state_json: Dict[str, Any]
        state_json = {
            "Type": "Map",
            "ItemProcessor": {
                "ProcessorConfig": {
                    "Mode": "DISTRIBUTED",
                    "ExecutionType": execution_type,
                },
            },
        }

        for key, value in {
            "MaxConcurrency": max_concurrency,
            "ItemSelector": item_selector,
            "ItemsPath": items_path,
            "ResultSelector": result_selector,
            "ResultWriter": result_writer,
            "ResultPath": result_path,
            "Catch": catch,
            "Retry": retry,
        }.items():
            if value is not None:
                state_json[key] = value

        if max_items_per_batch is not None:
            state_json["ItemBatcher"] = {
                "MaxItemsPerBatch": max_items_per_batch,
            }

        if item_reader_resource is not None:
            state_json["ItemReader"] = {
                "Resource": item_reader_resource,
                "ReaderConfig": reader_config,
                "Parameters": item_reader_parameters,
            }

        state_json["ItemProcessor"].update(map_iterator)

        super().__init__(scope, distributed_map_id, state_json=state_json)
