"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Iterator

from botocore.config import Config
from mypy_boto3_glacier import GlacierClient

from solution.application import __boto_config__
from solution.application.glacier_service.glacier_apis_factory import GlacierAPIsFactory


def test_user_agent_on_glacier_client(
    glacier_client: Iterator[GlacierClient], solution_user_agent: str
) -> None:
    _client = GlacierAPIsFactory.create_instance()
    _config = _client.meta.config
    assert type(_config) is Config
    _config_user_agent_extra = _config.__getattribute__("user_agent_extra")
    assert _config_user_agent_extra == __boto_config__.__getattribute__(
        "user_agent_extra"
    )
    assert _config_user_agent_extra == solution_user_agent
