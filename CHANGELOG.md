# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cross-account Glacier vault migration support via `SourceVaultAccountIdParameter`
- Glue job `defusedxml` dependency resolution for archive naming

## [1.1.4] - 2024-11-20

### BREAKING CHANGES

- **Removed S3 bucket deployment support**: All deployments now use CDK asset bundling instead of pre-built S3 artifacts. Existing deployments using S3 bucket method must migrate to CDK asset bundling.
- **Removed environment variables**: `DIST_OUTPUT_BUCKET`, `SOLUTION_NAME`, and `VERSION` are no longer supported or required.
- **Removed custom S3 synthesizer**: `source/solution/app.py` no longer supports S3 bucket synthesizer logic.

### Added

- CDK asset bundling with local source validation for all Lambda functions
- Support for China (Beijing), China (Ningxia), and GovCloud regions
- `DEPLOYMENT_METHOD=CDK_ASSETS` environment variable in Lambda functions for deployment tracking
- Lambda-specific `source/solution/application/requirements.txt` for asset bundling dependencies
- Region-specific bundling configurations with appropriate Docker images
- Enhanced asset exclusion patterns: `cdk.out/**`, `.git/**`, `**/node_modules/**`
- Automatic Python dependency installation during CDK bundling process

### Changed

- Default Python runtime to 3.12 for standard AWS regions (enhanced security)
- Python runtime to 3.11 for China (aws-cn) and GovCloud (aws-us-gov) regions (compliance)
- Deployment method from S3 bucket artifacts to pure CDK asset bundling
- `source/solution/app.py` simplified to single synthesizer configuration
- `source/solution/infrastructure/stack.py` enhanced with CDK asset bundling and dependency management
- `source/solution/infrastructure/helpers/solutions_function.py` updated with default CDK asset bundling
- `cdk.json` app command to use direct Python execution
- `SOLUTION_VERSION` updated to `v2.0.0` in `cdk.json`

### Deprecated

- S3 bucket deployment method (fully removed in this version)

### Removed

- S3 bucket synthesizer logic from `source/solution/app.py`
- Dependency on `DIST_OUTPUT_BUCKET` environment variable
- Dependency on `SOLUTION_NAME` environment variable
- Dependency on `VERSION` environment variable
- Pre-built S3 solution artifacts requirement
- Custom S3 synthesizer configurations

### Security

- Enhanced security through local source validation (eliminates external S3 dependencies)
- Improved audit trails with `DEPLOYMENT_METHOD` environment variable tracking
- Latest Python runtimes (3.12/3.11) with enhanced security features
- Eliminated external S3 bucket dependencies reducing attack surface

## [1.1.3] - 2024-10-03

### Fixed

- Inaccurate metrics that occurred during simultaneous data transfers
- Cross-account transfer initiation (feature currently unsupported, now prevented)

## [1.1.2] - 2024-07-25

### Added

- Support for China (Beijing) region
- Support for China (Ningxia) region
- Support for GovCloud regions

### Fixed

- Case insensitivity handling of "Path" in ArchiveDescription JSON parsing [#4](https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/pull/4)

## [1.1.1] - 2024-05-13

### Added

- Extended list of supported AWS regions

### Changed

- Lambda functions runtime upgraded to Python 3.12

### Fixed

- Glue job failures when processing empty vaults
- InitiateRetrieval workflow now skips incorrect inventory entries instead of failing entire process

## [1.1.0] - 2024-04-04

### Added

- Support for referencing external destination buckets
- SQS metrics widgets to CloudWatch dashboard for transfer progress monitoring
- Pre-built CloudWatch Logs Insights query for Lambda errors

### Changed

- MetricsProcessor Lambda now implements retry logic for TransactionConflict exceptions
- TransactWriteItems ClientRequestToken generation changed from MD5 to SHA-256 hashing
- Archive naming logic wrapped in try-except to prevent Glue job failures from single parsing errors
- Enhanced SSM Automation documents descriptions
- Added user-agents to all service clients for solution usage tracking

### Fixed

- Glue jobs not generating CloudWatch logs
- Glue job failures when description fields contain UTF-8 encoded characters
- Duplicate archive names when identical names exist with same creation time

## [1.0.0] - 2023-12-19

### Added

- Initial release of Data Transfer from Amazon S3 Glacier Vaults to Amazon S3 solution
- Serverless architecture for automated vault-to-S3 transfers
- Step Functions orchestration workflows
- Lambda functions for chunk retrieval and validation
- DynamoDB for metadata and metrics storage
- CloudWatch dashboard for monitoring transfer progress
- Support for multiple AWS regions

[Unreleased]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.1.4...HEAD
[1.1.4]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/releases/tag/v1.0.0
