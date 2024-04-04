"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from importlib import metadata

from botocore.config import Config

__version__ = metadata.version("solution")
__solution_id__ = "SO0293"
__boto_config__ = Config(
    user_agent_extra=f"AwsSolution/{__solution_id__}/v{__version__}"
)
