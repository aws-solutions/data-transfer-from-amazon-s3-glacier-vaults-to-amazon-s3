"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from pathlib import Path
from typing import Optional

from aws_cdk import Duration, Stack
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks

PYTHON_VERSION = "3"
GLUE_VERSION = "3.0"
JOB_COMMAND_NAME = "glueetl"
SORTING_SQL_QUERY = """
SELECT myDataSource.ArchiveId,
       myDataSource.ArchiveDescription,
       myDataSource.CreationDate,
       myDataSource.Size,
       myDataSource.SHA256TreeHash,
       COALESCE(namingOverrides.FileName, myDataSource.Filename) AS Filename,
       myDataSource.PartitionId
FROM myDataSource
LEFT JOIN namingOverrides ON myDataSource.ArchiveId = namingOverrides.GlacierArchiveID
ORDER BY myDataSource.CreationDate;"""
VALIDATION_CODE = """
node_inputs = list(dfc.values())
assert node_inputs[0].toDF().count() == node_inputs[1].toDF().count()
"""
VALIDATION_CUSTOM_CODE_NAME = "Validation"

METRIC_COLLECTION_SQL_QUERY = """
SELECT 
    COALESCE(SUM(myDataSource.Size), 0) as TotalArchivesSize,
    COALESCE(COUNT(*), 0) as TotalArchivesNumber
FROM myDataSource;
"""

METRIC_COLLECTION_CUSTOM_CODE_NAME = "MetricCollection"
ARCHIVE_NAMING_CODE_NAME = "ArchiveNaming"

# This will call the function within the glue job custom transform script
METRIC_COLLECTION_SCRIPT_APPENDIX = """
args = getResolvedOptions(sys.argv, ["workflow_run_name", "metric_table_name", "migration_type"])
update_metric_table(args["workflow_run_name"], args["metric_table_name"], args["migration_type"], dfc)
"""

PURGE_SORTED_INVENTORY = """ 
args = getResolvedOptions(sys.argv, ["inventory_bucket", "workflow_run_name"])
inventory_bucket = args["inventory_bucket"]
workflow_run = args["workflow_run_name"]
glueContext.purge_s3_path(f"s3://{inventory_bucket}/{workflow_run}/sorted_inventory/", options={"retentionPeriod": 0}, transformation_ctx="")

"""

NAMING_SCRIPT_APPENDIX = """
df = dfc.select(list(dfc.keys())[0]).toDF()
df_result = parse_and_remove_duplicates(df)
df_result = add_partitions(df_result)
dyf = DynamicFrame.fromDF(df_result, glueContext, "parsed_description")
return DynamicFrameCollection({"NamingTransform": dyf}, glueContext)
"""

VALIDATE_SOURCE_SCRIPT = """
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
df = dfc.select(list(dfc.keys())[0]).toDF()
if df.rdd.isEmpty():
    spark = SparkSession.builder.appName("empty_dataframe_with_headers").getOrCreate()
    schema = StructType([
        StructField("ArchiveId", StringType(), True),
        StructField("ArchiveDescription", StringType(), True),
        StructField("CreationDate", StringType(), True),
        StructField("Size", StringType(), True),
        StructField("SHA256TreeHash", StringType(), True),
        StructField("Filename", StringType(), True),
        StructField("PartitionId", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)
dyf = DynamicFrame.fromDF(df, glueContext, "skipped_archives")
return DynamicFrameCollection({"SkippedArchives": dyf}, glueContext)
"""


class GlueSfnUpdate(object):
    csv_file_columns = [
        {"Name": "ArchiveId", "Type": "string"},
        {"Name": "ArchiveDescription", "Type": "string"},
        {"Name": "CreationDate", "Type": "string"},
        {"Name": "Size", "Type": "string"},
        {"Name": "SHA256TreeHash", "Type": "string"},
    ]

    csv_file_columns_output = csv_file_columns + [
        {"Name": "PartitionId", "Type": "string"},
        {"Name": "Filename", "Type": "string"},
    ]

    archive_naming_override_columns = [
        {"Name": "GlacierArchiveID", "Type": "string"},
        {"Name": "FileName", "Type": "string"},
    ]

    def __init__(
        self,
        stack: Stack,
        s3_bucket_name: str,
        s3_bucket_arn: str,
        glue_job_name: str,
        glue_job_arn: str,
        metric_table: str,
        glue_max_concurent_runs: int = 1,
    ) -> None:
        self.stack = stack
        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_arn = s3_bucket_arn
        self.glue_job_name = glue_job_name
        self.glue_job_arn = glue_job_arn
        self.glue_max_concurent_runs = glue_max_concurent_runs
        self.metric_table = metric_table
        self.metric_collection_code = self.metric_collection_script
        self.custom_code = self.archive_naming_partitioning_script
        self.validate_source_input_custom_code = self.validate_source_input_script

    def _load_script(self, script_name: str) -> str:
        script_path = Path(__file__).resolve().parent / f"scripts/{script_name}.py"
        with script_path.open() as f:
            return f.read()

    @property
    def metric_collection_script(self) -> str:
        return (
            self._load_script("metric_collection_script")
            + METRIC_COLLECTION_SCRIPT_APPENDIX
        )

    @property
    def archive_naming_partitioning_script(self) -> str:
        return (
            self._load_script("archive_naming")
            + self._load_script("partitioning")
            + PURGE_SORTED_INVENTORY
            + NAMING_SCRIPT_APPENDIX
        )

    @property
    def validate_source_input_script(self) -> str:
        return VALIDATE_SOURCE_SCRIPT

    def autogenerate_etl_script(self, id: Optional[str] = None) -> tasks.CallAwsService:
        FIVE_TERABYTES = str(5 * 2**40)
        graph_workflow = {
            "node-1": {
                "S3CsvSource": {
                    "Name": "S3 bucket - Original Inventory",
                    "Paths.$": f"States.Array(States.Format('s3://{self.s3_bucket_name}/{{}}/original_inventory/', $.workflow_run))",
                    "QuoteChar": "quote",
                    "Separator": "comma",
                    "Escaper": "\\",
                    "Recurse": True,
                    "WithHeader": True,
                    "OutputSchemas": [{"Columns": self.csv_file_columns}],
                },
            },
            "node-2": {
                "ApplyMapping": {
                    "Name": "ConvertSizeToInt",
                    "Inputs": ["node-1"],
                    "Mapping": [
                        {
                            "ToKey": "ArchiveId",
                            "FromPath": ["ArchiveId"],
                            "FromType": "string",
                            "ToType": "string",
                        },
                        {
                            "ToKey": "ArchiveDescription",
                            "FromPath": ["ArchiveDescription"],
                            "FromType": "string",
                            "ToType": "string",
                        },
                        {
                            "ToKey": "CreationDate",
                            "FromPath": ["CreationDate"],
                            "FromType": "string",
                            "ToType": "string",
                        },
                        {
                            "ToKey": "Size",
                            "FromPath": ["Size"],
                            "FromType": "string",
                            "ToType": "bigint",
                        },
                        {
                            "ToKey": "SHA256TreeHash",
                            "FromPath": ["SHA256TreeHash"],
                            "FromType": "string",
                            "ToType": "string",
                        },
                    ],
                }
            },
            "node-3": {
                "Filter": {
                    "Name": "Filter<=5TB",
                    "Inputs": ["node-2"],
                    "LogicalOperator": "AND",
                    "Filters": [
                        {
                            "Operation": "LTE",
                            "Negated": False,
                            "Values": [
                                {"Type": "COLUMNEXTRACTED", "Value": ["Size"]},
                                {
                                    "Type": "CONSTANT",
                                    "Value": [
                                        FIVE_TERABYTES,
                                    ],
                                },
                            ],
                        },
                    ],
                }
            },
            "node-4": {
                "Filter": {
                    "Name": "Filter>5TB",
                    "Inputs": ["node-2"],
                    "LogicalOperator": "AND",
                    "Filters": [
                        {
                            "Operation": "GT",
                            "Negated": False,
                            "Values": [
                                {"Type": "COLUMNEXTRACTED", "Value": ["Size"]},
                                {
                                    "Type": "CONSTANT",
                                    "Value": [
                                        FIVE_TERABYTES,
                                    ],
                                },
                            ],
                        },
                    ],
                }
            },
            "node-5": {
                "CustomCode": {
                    "Name": "ValidateInput<=5TB",
                    "Inputs": ["node-3"],
                    "ClassName": "validate_source_table",
                    "Code": self.validate_source_input_custom_code,
                }
            },
            "node-6": {
                "CustomCode": {
                    "Name": "ValidateInput>5TB",
                    "Inputs": ["node-4"],
                    "ClassName": "validate_source_table",
                    "Code": self.validate_source_input_custom_code,
                }
            },
            "node-7": {
                "SelectFromCollection": {
                    "Name": "SelectFromCollection<=5TB",
                    "Inputs": ["node-5"],
                    "Index": 0,
                }
            },
            "node-8": {
                "SelectFromCollection": {
                    "Name": "SelectFromCollection>5TB",
                    "Inputs": ["node-6"],
                    "Index": 0,
                }
            },
            "node-9": {
                "CustomCode": {
                    "Name": ARCHIVE_NAMING_CODE_NAME,
                    "Inputs": ["node-7"],
                    "ClassName": ARCHIVE_NAMING_CODE_NAME,
                    "Code": self.custom_code,
                }
            },
            "node-10": {
                "SparkSQL": {
                    "Name": "SQL Metric",
                    "Inputs": ["node-7"],
                    "SqlQuery": METRIC_COLLECTION_SQL_QUERY,
                    "SqlAliases": [{"From": "node-7", "Alias": "myDataSource"}],
                    "OutputSchemas": [
                        {
                            "Columns": [
                                {"Name": "TotalArchivesNumber", "Type": "bigint"},
                                {"Name": "TotalArchivesSize", "Type": "bigint"},
                            ]
                        }
                    ],
                }
            },
            "node-11": {
                "SparkSQL": {
                    "Name": "SQL Skipped Metric",
                    "Inputs": ["node-8"],
                    "SqlQuery": METRIC_COLLECTION_SQL_QUERY,
                    "SqlAliases": [{"From": "node-8", "Alias": "myDataSource"}],
                    "OutputSchemas": [
                        {
                            "Columns": [
                                {"Name": "TotalArchivesNumber", "Type": "bigint"},
                                {"Name": "TotalArchivesSize", "Type": "bigint"},
                            ]
                        }
                    ],
                }
            },
            "node-12": {
                "S3DirectTarget": {
                    "Name": "S3 bucket - Not Migrated",
                    "Inputs": ["node-8"],
                    "Compression": "none",
                    "Format": "csv",
                    "SchemaChangePolicy": {"EnableUpdateCatalog": False},
                    "Path.$": f"States.Format('s3://{self.s3_bucket_name}/{{}}/not_migrated/', $.workflow_run)",
                }
            },
            "node-13": {
                "SelectFromCollection": {
                    "Name": "SelectFromCollection",
                    "Inputs": ["node-9"],
                    "Index": 0,
                }
            },
            "node-14": {
                "CustomCode": {
                    "Name": METRIC_COLLECTION_CUSTOM_CODE_NAME,
                    "Inputs": ["node-10", "node-11"],
                    "ClassName": METRIC_COLLECTION_CUSTOM_CODE_NAME,
                    "Code": self.metric_collection_code,
                }
            },
            "node-15": {
                "S3CsvSource": {
                    "Name": "S3 bucket - Naming Overrides",
                    "Paths.$": f"States.Array(States.Format('s3://{self.s3_bucket_name}/{{}}/naming_overrides/', $.workflow_run))",
                    "QuoteChar": "quote",
                    "Separator": "comma",
                    "Recurse": True,
                    "WithHeader": True,
                    "Escaper": "",
                    "OutputSchemas": [
                        {"Columns": self.archive_naming_override_columns}
                    ],
                },
            },
            "node-16": {
                "SparkSQL": {
                    "Name": "SQL sorting",
                    "Inputs": ["node-15", "node-13"],
                    "SqlQuery": SORTING_SQL_QUERY,
                    "SqlAliases": [
                        {"From": "node-15", "Alias": "namingOverrides"},
                        {"From": "node-13", "Alias": "myDataSource"},
                    ],
                    "OutputSchemas": [{"Columns": self.csv_file_columns_output}],
                }
            },
            "node-17": {
                "CustomCode": {
                    "Name": VALIDATION_CUSTOM_CODE_NAME,
                    "Inputs": ["node-7", "node-16"],
                    "ClassName": VALIDATION_CUSTOM_CODE_NAME,
                    "Code": VALIDATION_CODE,
                }
            },
            "node-18": {
                "S3DirectTarget": {
                    "Name": "S3 bucket - Sorted Inventory",
                    "Inputs": ["node-16"],
                    "PartitionKeys.$": "States.Array(States.Array('PartitionId'))",
                    "Compression": "none",
                    "Format": "csv",
                    "SchemaChangePolicy": {"EnableUpdateCatalog": False},
                    "Path.$": f"States.Format('s3://{self.s3_bucket_name}/{{}}/sorted_inventory/', $.workflow_run)",
                }
            },
        }

        return tasks.CallAwsService(
            self.stack,
            "GlueJobAutogenerateEtl" if id is None else id,
            service="glue",
            action="updateJob",
            parameters={
                "JobName": self.glue_job_name,
                "JobUpdate": {
                    "GlueVersion": GLUE_VERSION,
                    "Role": self.glue_job_arn,
                    "ExecutionProperty": {
                        "MaxConcurrentRuns": self.glue_max_concurent_runs
                    },
                    "CodeGenConfigurationNodes": graph_workflow,
                    "Command": {
                        "Name": JOB_COMMAND_NAME,
                        "ScriptLocation.$": f"States.Format('s3://{self.s3_bucket_name}/{{}}/scripts/inventory_sort_script.py', $.workflow_run)",
                        "PythonVersion": PYTHON_VERSION,
                    },
                },
            },
            iam_resources=[self.glue_job_arn, self.s3_bucket_arn],
            iam_action="iam:PassRole",
            result_path="$.glue_job_auto_generate_etl_result",
        )

    def start_job(self, id: Optional[str] = None) -> tasks.GlueStartJobRun:
        return tasks.GlueStartJobRun(
            self.stack,
            "GlueStartJobRun" if id is None else id,
            glue_job_name=self.glue_job_name,
            task_timeout=sfn.Timeout.duration(Duration.minutes(30)),
            notify_delay_after=Duration.minutes(5),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--workflow_run_name.$": "$.workflow_run",
                    "--metric_table_name": self.metric_table,
                    "--migration_type.$": "$.migration_type",
                    "--inventory_bucket": self.s3_bucket_name,
                    "--enable-job-insights": "true",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--job-language": "python",
                }
            ),
            result_path="$.glue_start_job_result",
        )
