# Data Transfer from Amazon S3 Glacier vaults to Amazon S3 build pipeline

This project utilizes cdk pipelines for setting up a development pipeline that builds, deploys, and tests the solution.

## Setup

1. Create a CodeCommit repo and add it as a remote

        git remote add codecommit <codecommit repo url>

2. Deploy the pipeline stack, using the context to set the repo name and branch

        npx cdk deploy pipeline -c repository_name=<repo name> -c branch=<branch name>

## Developer workflow

Use the CodeCommit repo as a place to develop code. Pushing to it will trigger the pipeline and run all the automated
tests. When you are satisfied with the commits, you can push upstream to origin or submit the commits for review.

You can deploy multiple instances of the stack pointed at different branches, and each will have the branch appended to
the stack name for differentiation.

## Optional context parameters for pipeline

There is optional context parameter to skip integration tests

    npx cdk deploy pipeline -c skip_integration_tests=true
