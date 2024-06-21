"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import datetime
from unittest.mock import patch

from pyspark.sql import SparkSession

from solution.infrastructure.glue_helper.scripts.archive_naming import (
    parse_description,
    parse_description_df,
    parse_filename,
    remove_duplicates_df,
)


def test_quoted_fastglacier_v1_format() -> None:
    assert (
        parse_description(
            '"<ArchiveMetadata>           <Path>       UkVOREVSRkFSTTUvQjovb25lL3R3by90aHJlZS9jaDAxX3NlcTAwNF9zaDAwMTAuMjA0NC5kcHg=   </Path></ArchiveMetadata>"'
        )
        == "RENDERFARM5/B:/one/two/three/ch01_seq004_sh0010.2044.dpx"
    )


def test_unquoted_fastglacier_v1_format() -> None:
    assert (
        parse_description(
            "<ArchiveMetadata>           <Path>       UkVOREVSRkFSTTUvQjovb25lL3R3by90aHJlZS9jaDAxX3NlcTAwNF9zaDAwMTAuMjA0NC5kcHg=   </Path></ArchiveMetadata>"
        )
        == "RENDERFARM5/B:/one/two/three/ch01_seq004_sh0010.2044.dpx"
    )


def test_fastglacier_v2_v3_v4_format() -> None:
    assert (
        parse_description(
            "           <m><v>4</v><p>RlgxNTAvQ0JCX2ZpZ3VyZS9lZTRjM2MxOS00OTBkLTQwN2UtOGJlNy04ODliZTQxYjQ4YmIuY2Ji </p><lm>20190121T085242Z       </lm>        </m>"
        )
        == "FX150/CBB_figure/ee4c3c19-490d-407e-8be7-889be41b48bb.cbb"
    )


def test_fastglacier_v2_v3_v4_format_utf8() -> None:
    assert (
        parse_description(
            "           <m><v>4</v><p>RlgxNTAvQ0JCX2ZpZ3VyZS90ZXN0X3B5dGjDtm5fZmlsZS5jYmI=</p><lm>20190121T085242Z       </lm>        </m>"
        )
        == "FX150/CBB_figure/test_pythön_file.cbb"
    )


def test_cloudberry_json_format() -> None:
    assert (
        parse_description(
            '{"Path":"CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx","UTCDateModified":"20100310T110623Z"}'
        )
        == "CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx"
    )


def test_empty_file_name_from_json() -> None:
    assert (
        parse_filename("AAAQQQ", '{"Path":"   ","UTCDateModified":"20100310T110623Z"}')
        == "00undefined/AAAQQQ"
    )


def test_cloudberry_json_format_lowercase() -> None:
    assert (
        parse_description(
            '{"path":"CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx","UTCDateModified":"20100310T110623Z"}'
        )
        == "CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx"
    )


def test_empty_file_name_from_json_lowercase() -> None:
    assert (
        parse_filename("AAAQQQ", '{"path":"   ","UTCDateModified":"20100310T110623Z"}')
        == "00undefined/AAAQQQ"
    )


def test_empty_archive_description() -> None:
    assert parse_filename("AAAQQQ", "             ") == "00undefined/AAAQQQ"


def test_backward_slashes_as_forward() -> None:
    assert parse_filename("AAAQQQ", "aaa\\\\bbb\\\\ccc") == "aaa/bbb/ccc"


def test_failed_parse_description() -> None:
    with patch(
        "solution.infrastructure.glue_helper.scripts.archive_naming.parse_description"
    ) as mock_parse_description:
        mock_parse_description.side_effect = Exception()
        assert parse_filename("AAAQQQ", "test_archive_description") == "00error/AAAQQQ"


def test_parse_description_df() -> None:
    columns = [
        "ArchiveId",
        "ArchiveDescription",
        "CreationDate",
        "Size",
        "SHA256TreeHash",
    ]
    data = []
    for i in range(3):
        entry = (
            f"archive_id_{i}",
            f"archive_description_{i}",
            datetime.datetime.now().isoformat(),
            i,
            f"SHA256TreeHash_{i}",
        )
        data.append(entry)
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    df = spark.createDataFrame(data, columns)
    result_df = parse_description_df(df)

    filename_values = [row.Filename for row in result_df.select("Filename").collect()]

    expected_filename_values = [
        "archive_description_0",
        "archive_description_1",
        "archive_description_2",
    ]
    assert filename_values == expected_filename_values


def test_remove_duplicates_df() -> None:
    columns = [
        "ArchiveId",
        "ArchiveDescription",
        "CreationDate",
        "Size",
        "SHA256TreeHash",
        "Filename",
    ]
    data = []
    for i in range(3):
        entry = (
            f"archive_id_{i}",
            f"archive_description_{i}",
            f"2023-08-15T13:28:0{i}",
            i,
            f"SHA256TreeHash_{i}",
            "duplication.txt",
        )
        data.append(entry)
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    df = spark.createDataFrame(data, columns)
    result_df = remove_duplicates_df(df)

    filename_values = [row.Filename for row in result_df.select("Filename").collect()]

    expected_filename_values = [
        "duplication.txt",
        "duplication.txt-2023-08-15T13:28:01-2",
        "duplication.txt-2023-08-15T13:28:02-3",
    ]
    assert filename_values == expected_filename_values

    assert len(filename_values) == len(set(filename_values))


def test_remove_duplicates_same_creation_date_df() -> None:
    columns = [
        "ArchiveId",
        "ArchiveDescription",
        "CreationDate",
        "Size",
        "SHA256TreeHash",
        "Filename",
    ]
    data = []
    for i in range(3):
        entry = (
            f"archive_id_{i}",
            f"archive_description_{i}",
            "2023-08-15T13:28:00",
            i,
            f"SHA256TreeHash_{i}",
            "duplication.txt",
        )
        data.append(entry)
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    df = spark.createDataFrame(data, columns)
    result_df = remove_duplicates_df(df)

    filename_values = [row.Filename for row in result_df.select("Filename").collect()]

    expected_filename_values = [
        "duplication.txt",
        "duplication.txt-2023-08-15T13:28:00-2",
        "duplication.txt-2023-08-15T13:28:00-3",
    ]
    assert filename_values == expected_filename_values

    assert len(filename_values) == len(set(filename_values))
