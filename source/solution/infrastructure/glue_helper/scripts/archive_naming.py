"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import json
import logging
import re
from base64 import b64decode
from xml.dom.minidom import parseString

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_filename(archive_id: str, archive_description: str) -> str:
    filename: str = parse_description(archive_description.strip()).strip()
    if filename == "":
        filename = f"00undefined/{archive_id}"
    if "\\" in filename:
        filename = filename.replace(r"\\", "/")

    return filename


def parse_description(archive_description: str) -> str:
    """
    Parses the archive description and returns the filename.
    """
    # FastGlacier v2/3/4
    if re.search(r"<m>.*<\/m>", archive_description):
        return parse_fast_glacier(archive_description, "m", "p")

    # FastGlacier v1
    if re.search(r"<ArchiveMetadata>.*</ArchiveMetadata>", archive_description):
        return parse_fast_glacier(archive_description, "ArchiveMetadata", "Path")

    # JSON
    if re.search(r"{\s*\\*\".*}\s*", archive_description):
        try:
            json_object = json.loads(archive_description)
            path = json_object.get("Path")
            if isinstance(path, str):
                return path
        except Exception as _:
            logger.info(f"Failed to parse JSON: {archive_description}")
    return archive_description


def parse_fast_glacier(archive_description: str, metadata: str, path: str) -> str:
    """
    Parse the Fast Glacier XML archive description and return the filename.
    """
    json_obj = parseString(archive_description.strip("'").strip('"'))
    return b64decode(
        json_obj.getElementsByTagName(metadata)[0]
        .getElementsByTagName(path)[0]
        .firstChild.data  # type: ignore
    ).decode("ascii")


def parse_description_df(df: DataFrame) -> DataFrame:
    parse_description_udf = F.udf(parse_filename)
    df_parsed_description = df.withColumn(
        "Filename", parse_description_udf("ArchiveId", "ArchiveDescription")
    )
    return df_parsed_description


def remove_duplicates_df(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("Filename").orderBy("CreationDate")
    df_with_row_number = df.withColumn("row_number", F.row_number().over(window_spec))
    df_removed_duplicates = df_with_row_number.withColumn(
        "Filename",
        F.when(df_with_row_number["row_number"] == 1, F.col("Filename")).otherwise(
            F.concat_ws("-", F.col("Filename"), F.col("CreationDate"))
        ),
    )
    return df_removed_duplicates


def parse_and_remove_duplicates(df: DataFrame) -> DataFrame:
    df_parsed_description = parse_description_df(df)
    df_removed_duplicates = remove_duplicates_df(df_parsed_description)
    return df_removed_duplicates
