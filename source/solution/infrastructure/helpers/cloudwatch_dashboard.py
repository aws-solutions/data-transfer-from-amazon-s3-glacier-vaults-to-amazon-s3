"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


from typing import List, Tuple

from aws_cdk import Aws, Duration, Fn, Stack
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_sqs as sqs

METRICS_NAMESPACE = "DataTransferFromAmazonS3GlacierVaultsToAmazonS3"
METRIC_LABEL_LIST = [
    ("TotalArchiveCount", "Aggregated Count of Archives"),
    ("TotalArchiveSize", "Aggregated Size of Archives"),
    ("RequestedArchiveCount", "Aggregated Count of Requested Archives"),
    ("RequestedArchiveSize", "Aggregated Size of Requested Archives"),
    ("StagedArchiveCount", "Aggregated Count of Staged Archives"),
    ("StagedArchiveSize", "Aggregated Size of Staged Archives"),
    ("DownloadedArchiveCount", "Aggregated Count of Downloaded Archives"),
    ("DownloadedArchiveSize", "Aggregated Size of Downloaded Archives"),
    ("FailedArchiveCount", "Aggregated Count of Failed Archives"),
    ("FailedArchiveSize", "Aggregated Size of Failed Archives"),
    ("SkippedArchiveCount", "Aggregated Count of Skipped Archives Larger Than 5TB"),
    ("SkippedArchiveSize", "Aggregated Size of Skipped Archives Larger Than 5TB"),
]


class CwDashboard(object):
    def __init__(self, scope: Stack, queues: List[Tuple[str, sqs.Queue]]) -> None:
        count_metrics_list = []
        size_metrics_list = []
        stack_id = Fn.select(2, Fn.split("/", Aws.STACK_ID))
        self.dashboard = cw.Dashboard(
            scope,
            "CloudWatchDashboard",
            default_interval=Duration.days(1),
            dashboard_name=f"Data-transfer-from-Amazon-S3-Glacier-to-Amazon-S3-Dashboard-{stack_id}",
            variables=[
                cw.DashboardVariable(
                    id="WorkflowRun",
                    type=cw.VariableType.PROPERTY,
                    label="Workflow Run ID",
                    input_type=cw.VariableInputType.SELECT,
                    value="WorkflowRun",
                    values=cw.Values.from_search_components(
                        namespace=METRICS_NAMESPACE,
                        dimensions=["WorkflowRun"],
                        populate_from="WorkflowRun",
                        metric_name="TotalArchiveCount",
                    ),
                    default_value=cw.DefaultValue.FIRST,
                    visible=True,
                )
            ],
        )

        for name, label in METRIC_LABEL_LIST:
            metric = self.create_metric(
                name,
                label,
                {"WorkflowRun": "No-Workflow"},
                cw.Stats.MAXIMUM,
                METRICS_NAMESPACE,
            )
            if "Count" in name:
                count_metrics_list.append(metric)
            else:
                size_metrics_list.append(metric)

        sqs_metrics = [
            self.create_metric(
                "ApproximateAgeOfOldestMessage",
                name,
                {"QueueName": queue.queue_name},
                cw.Stats.MAXIMUM,
                "AWS/SQS",
            )
            for name, queue in queues
        ]

        widget_title_prefix = (
            "Data Retrieval for Amazon Glacier S3 Progress Metrics - {}"
        )
        # Full Percision to make sure users see all the counts
        self.add_number_widgets(
            count_metrics_list, widget_title_prefix.format("Count"), True
        )
        # GUI cannot show massive amount of Archive Sizes, so we disable full precision to round up
        self.add_number_widgets(
            size_metrics_list, widget_title_prefix.format("Size"), False
        )

        self.add_graph_widgets(sqs_metrics, "ApproximateAgeOfOldestMessage")

    def add_number_widgets(
        self, metrics_list: list[cw.Metric], title: str, full_precision: bool
    ) -> None:
        self.dashboard.add_widgets(
            cw.SingleValueWidget(
                metrics=metrics_list,
                period=Duration.minutes(5),
                width=24,
                title=title,
                full_precision=full_precision,
            )
        )

    def add_graph_widgets(self, metrics_list: list[cw.Metric], title: str) -> None:
        self.dashboard.add_widgets(
            cw.GraphWidget(
                title=title,
                view=cw.GraphWidgetView.TIME_SERIES,
                width=24,
                height=6,
                left=metrics_list,
            )
        )

    def create_metric(
        self,
        metric_name: str,
        label: str,
        dimensions_map: dict[str, str],
        statistic: str,
        namespace: str,
    ) -> cw.Metric:
        return cw.Metric(
            unit=cw.Unit.COUNT,
            metric_name=metric_name,
            label=label,
            namespace=namespace,
            dimensions_map=dimensions_map,
            account=Aws.ACCOUNT_ID,
            statistic=statistic,
            period=Duration.seconds(300),
        )
