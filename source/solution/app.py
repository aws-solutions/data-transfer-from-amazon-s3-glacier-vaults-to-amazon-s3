"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import os
from pathlib import Path
from typing import Any

import aws_cdk as cdk

from solution.infrastructure.aspects.app_registry import (
    AppRegistry,
    AppRegistryCondition,
)
from solution.infrastructure.stack import SolutionStack
from solution.mocking.mock_glacier_stack import MockGlacierStack
from solution.pipeline.stack import PipelineStack

dist_output_bucket = os.getenv("DIST_OUTPUT_BUCKET")
solution_name = os.getenv("SOLUTION_NAME")
dist_version = os.getenv("VERSION")

if not (dist_output_bucket and solution_name and dist_version):
    synthesizer = cdk.DefaultStackSynthesizer(
        generate_bootstrap_version_rule=False,
    )
else:
    synthesizer = cdk.DefaultStackSynthesizer(
        file_assets_bucket_name=f"{dist_output_bucket}-${{AWS::Region}}",
        bucket_prefix=f"{solution_name}/{dist_version}/",
        generate_bootstrap_version_rule=False,
    )


def _load_cdk_context() -> Any:
    try:
        cdk_json_path = Path(__file__).parent.parent.parent.absolute() / "cdk.json"
        with open(cdk_json_path, "r") as f:
            config = json.loads(f.read())
    except FileNotFoundError:
        print(f"{cdk_json_path} not found, using empty context!")
        return {}
    return config.get("context", {})


def _setup_app_registry(app: cdk.App, stack: SolutionStack) -> None:
    app_registry = AppRegistry(stack, "AppRegistryAspect")
    app_registry_condition = AppRegistryCondition(stack, "AppRegistryConditionAspect")
    cdk.Aspects.of(app_registry).add(app_registry_condition)
    cdk.Aspects.of(app).add(app_registry)


def main() -> None:
    app = cdk.App(context=_load_cdk_context())
    solution_stack = SolutionStack(
        app,
        "data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3",
        synthesizer=synthesizer,
    )
    _setup_app_registry(app, solution_stack)
    PipelineStack(app, "pipeline")
    MockGlacierStack(app, "mock-glacier")
    app.synth()
