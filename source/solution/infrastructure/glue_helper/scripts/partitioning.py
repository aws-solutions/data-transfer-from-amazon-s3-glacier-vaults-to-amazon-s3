"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PARTITION_SIZE = 40 * pow(2, 40)


def assign_partition(cumulative_sum: int) -> str:
    partition = int(cumulative_sum // PARTITION_SIZE)
    return f"{partition:010d}"


def add_partitions(df: DataFrame) -> DataFrame:
    window_spec = Window.orderBy("CreationDate")
    df_cumulative_sum = df.withColumn(
        "CumulativeSum", F.sum(F.col("Size")).over(window_spec)
    )
    assign_partition_udf = F.udf(assign_partition)

    df_result = df_cumulative_sum.withColumn(
        "PartitionId", assign_partition_udf(df_cumulative_sum["CumulativeSum"])
    )
    return df_result
