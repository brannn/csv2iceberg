# Changelog

All notable changes to SQL Batcher will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive test framework with support for all database adapters
- Better support for running tests without available database connections
- Enhanced documentation including TESTING.md with testing guidelines

### Changed
- Improved test coverage for PostgreSQL adapter
- Consolidated testing on pytest, removing unittest style assertions
- Better error handling in batch processing for oversized statements

### Fixed
- Issue with PostgreSQL COPY command and proper cursor handling
- Test failures in PostgreSQL adapter tests when mocking the connection

## [0.1.0] - 2025-04-26

### Added
- Core SQLBatcher implementation with batching based on size limits
- Support for dynamic column-based batch size adjustment
- Query collector interface for analyzing generated SQL
- Abstract adapter interface for database-specific implementations
- Implementations for multiple database systems:
  - Generic adapter for any database connection
  - PostgreSQL adapter with COPY command optimization
  - Trino adapter with session property support
  - Snowflake adapter with parameterized queries
  - BigQuery adapter with job monitoring
  - Spark adapter for distributed SQL processing
- Dry run mode for inspecting batches without execution
- Comprehensive examples for all supported databases
- Type annotations throughout the codebase
- Documentation with installation and usage instructions
- Test infrastructure with pytest markers for different database systems