"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Optional

import jsii
from aws_cdk import Aws, CfnCondition, CfnResource, Fn, IAspect, Stack, Tags
from aws_cdk import aws_applicationinsights as applicationinsights
from aws_cdk import aws_servicecatalogappregistry_alpha as appreg
from constructs import Construct, IConstruct


@jsii.implements(IAspect)
class AppRegistryCondition(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)
        self.deploy_if_not_china_partition = CfnCondition(
            self,
            "DeployIfNotChinaPartition",
            expression=Fn.condition_not(Fn.condition_equals(Aws.PARTITION, "aws-cn")),
        )

    def visit(self, node: IConstruct) -> None:
        if isinstance(node, CfnResource):
            node.add_override(
                "Condition", self.deploy_if_not_china_partition.logical_id
            )


@jsii.implements(IAspect)
class AppRegistry(Construct):
    """This construct creates the resources required for AppRegistry and injects them as Aspects"""

    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)
        self.solution_name = scope.node.try_get_context("SOLUTION_NAME")
        self.app_registry_name = scope.node.try_get_context("APP_REGISTRY_NAME")
        self.solution_id = scope.node.try_get_context("SOLUTION_ID")
        self.solution_version = scope.node.try_get_context("SOLUTION_VERSION")
        self.application_type = scope.node.try_get_context("APPLICATION_TYPE")

    def visit(self, node: IConstruct) -> None:
        """The visitor method invoked during cdk synthesis"""
        if isinstance(node, Stack):
            if not node.nested and node.__class__.__name__ == "SolutionStack":
                stack: Stack = node
                self._create_app_for_app_registry()
                self.application.associate_stack(stack)
                self._create_attribute_group()
                self._add_tags_for_application()
                # Adding Application Insights might result in an error because it uses a Service Role, which gets provisioned the
                # first time Application Insights is used. If the role is not provisioned in time, it can cause the deployment to fail.
                # Keeping the code commented here in case this limitation is resolved in the near future.
                # self._create_app_for_app_insights()

    def _create_app_for_app_registry(self) -> None:
        """Method to create an AppRegistry Application"""
        self.application = appreg.Application(
            self,
            "RegistrySetup",
            application_name=Fn.join(
                "-",
                [
                    self.app_registry_name,
                    Fn.select(2, Fn.split("/", Aws.STACK_ID)),
                    Aws.REGION,
                    Aws.ACCOUNT_ID,
                ],
            ),
            description=f"Service Catalog application to track and manage all your resources for the solution {self.solution_name}",
        )

    def _add_tags_for_application(self) -> None:
        """Method to add tags to the AppRegistry's Application instance"""
        if not self.application:
            self._create_app_for_app_registry()

        Tags.of(self.application).add("Solutions:SolutionID", self.solution_id)
        Tags.of(self.application).add("Solutions:SolutionName", self.solution_name)
        Tags.of(self.application).add(
            "Solutions:SolutionVersion", self.solution_version
        )
        Tags.of(self.application).add(
            "Solutions:ApplicationType", self.application_type
        )

    def _create_attribute_group(self) -> None:
        """Method to add attributes to be as associated with the Application's instance in AppRegistry"""
        if not self.application:
            self._create_app_for_app_registry()

        self.application.associate_attribute_group(
            appreg.AttributeGroup(
                self,
                "AppAttributes",
                attribute_group_name=Aws.STACK_NAME,
                description="Attributes for Solutions Metadata",
                attributes={
                    "applicationType": self.application_type,
                    "version": self.solution_version,
                    "solutionID": self.solution_id,
                    "solutionName": self.solution_name,
                },
            )
        )

    def _create_app_for_app_insights(self) -> None:
        """Method to create resources to enable application insights"""
        if not self.application:
            self._create_app_for_app_registry()

        assert isinstance(self.application.node.default_child, CfnResource)
        dependent_resource: CfnResource = self.application.node.default_child

        applicationinsights.CfnApplication(
            self,
            "AppInsights",
            resource_group_name=Fn.join(
                "-",
                [
                    "AWS_AppRegistry_Application",
                    Aws.STACK_NAME,
                    self.app_registry_name,
                    Aws.REGION,
                    Aws.ACCOUNT_ID,
                ],
            ),
            auto_configuration_enabled=True,
            cwe_monitor_enabled=True,
            ops_center_enabled=True,
        ).add_depends_on(dependent_resource)
