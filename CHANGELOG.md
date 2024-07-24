# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.2] - 2024-07-25

### Added

- Support for new regions - China (Beijing), China (Ningxia), and GovCloud.

### Fixed

- Handling of case insensitivity of "Path" in ArchiveDescription Json parsing [#4](https://github.com/aws-solutions/data-transfer-from-amazon-s3-glacier-vaults-to-amazon-s3/pull/4).

## [1.1.1] - 2024-05-13

### Updated

- Upgrade Lambda functions runtime version to Python 3.12
- Extended the list of supported regions

### Fixed

- Fix Glue job failures occurring when empty vault
- Fix InitiateRetrieval workflow to skip incorrect inventory entries and avoid failing the entire process

## [1.1.0] - 2024-04-04

### Added

- Allow referencing an external destination bucket.
- Add SQS metrics widgets to CloudWatch dashboard to have a way to monitor the progress of the transfer
- Add a pre-built CloudWatch Logs Insights query for Lambdas errors

### Updated

- Implement retry within MetricsProcessor Lambda when encountering TransactionConflict exception
- Use SHA-256 to generate TransactWriteItems ClientRequestToken as a more secure alternative to MD5 hashing
- Add try-except block around the archive naming logic to prevent the entire Glue job from failing due to a single/few names parsing errors
- Enhance SSM Automation documents descriptions
- Add user-agents to all service clients to track usage on solution service API usage dashboard

### Fixed

- Fix Glue jobs that are not generating CloudWatch logs
- Fix Glue job failures occurring when description fields contain UTF-8 encoded characters
- Fix duplicate archive names when there are identical archive names with the same creation time

## [1.0.0] - 2023-12-19

### Added

- Initial revision