"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple

from solution.application.util.exceptions import MaximumRetryLimitExceeded

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def retry(max_retries: int = 3, raise_exception: bool = False) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
            for retry in range(max_retries):
                try:
                    func(*args, **kwargs)
                    break
                except Exception as e:
                    logger.error(f"Exception occurred: {str(e)}")
                    logger.error(f"Retrying... (Attempt {retry + 1}/{max_retries})")
                    last_exception = e
            else:
                logger.info(f"Maximum retry limit {max_retries} exceeded. Exiting...")
                if raise_exception:
                    raise MaximumRetryLimitExceeded(max_retries, str(last_exception))

        return wrapper

    return decorator
