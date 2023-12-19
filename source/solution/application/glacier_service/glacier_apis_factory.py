"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
else:
    GlacierClient = object


class GlacierAPIsFactory:
    """
    This class is used to create an instance from either the actual Glacier
    or the mock APIs, depending on the passed parameter 'mock'

    Usage example:
    - For real Glacier APIs
        glacier = GlacierAPIsFactory.create_instance()
        glacier.get_job_output(params)
    - For Mock Glacier APIs
        mockGlacier = GlacierAPIsFactory.create_instance(mock=True)
        mockGlacier.get_job_output(params)
    """

    @staticmethod
    def create_instance(mock: bool = False) -> GlacierClient:
        if mock:
            from solution.application.mocking.mock_glacier_apis import MockGlacierAPIs

            return MockGlacierAPIs()
        config: Config = Config(user_agent_extra=f"AwsSolution/SO0293/v1.0.0")
        client: GlacierClient = boto3.client("glacier", config=config)
        return client
