"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List


def generate_chunk_array(
    size: int, chunk_size: int, check_power_of_two: bool = True
) -> List[str]:
    """
    The range to archive download must be megabyte and treehash aligned
    in order to receive checksum values when downloading.
    https://docs.aws.amazon.com/amazonglacier/latest/dev/checksum-calculations-range.html#tree-hash-algorithm
    """

    if check_power_of_two and not is_power_of_two(chunk_size):
        raise ValueError(
            "Archive chunk size should be a power of 2 to be megabyte and treehash aligned."
        )

    chunks = []
    start_index = 0
    end_index = min(size, chunk_size) - 1
    while end_index < size - 1:
        chunks.append(f"{start_index}-{end_index}")
        start_index = end_index + 1
        end_index = min(start_index + chunk_size - 1, size - 1)
    chunks.append(f"{start_index}-{end_index}")
    return chunks


def is_power_of_two(n: int) -> bool:
    return (n != 0) and (n & (n - 1) == 0)


def calculate_chunk_size(total_size: int) -> int:
    min_size_per_chunk = (total_size // 10000) + 1
    chunk_size = 1024 * 2**20  # 1024 MB default

    while chunk_size < min_size_per_chunk:
        chunk_size = chunk_size * 2
    return chunk_size
