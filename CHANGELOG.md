# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.8] - 2025-11-21

### Added

- API: update files, input_cols & fileMeta trough PUT
- API: Extra checks on input file and datakey
- Core: Pre-process clientnames as case-insensitive

### Changed

- API: input_cols is optional when creating a job
- API: closer in functionality of frontend
- Update: channels package -> v4.3.2

### Fixed

- Lookup tables false-positives

## [1.2.7] - 2025-11-13

### Added

- Missing copyright text in progress_control.py
- Force ASCII to UTF-8 encoding

### Changed

- Update: Docker version to python:3.13.6-slim
- Update: pypi packages

### Fixed

- Force ASCII to UTF-8 encoding

## [1.2.6] - 2025-11-12

### Added

- Progress cancellation functionality with API support
- Cancelled state to job status choices
- Job control to cancel running processes
- Health check endpoint via Server-Sent Events (SSE)

### Changed

- Updated build.spec to remove unused files when building
- Optimized build script for PyInstaller
- Improved Swagger API schemas
- Cleaned up comments and logging throughout codebase
- Disabled console output when running as PyInstaller executable

### Fixed

- NoneType AttributeError in Progress Tracker
- Newer versions of Polars don't accept return_dtype in map_batches
- Datakey built in wrong folder
- Proper handling of job deletion including directories and files

## [v1.1.1] - 2025-09-09

### New Features

- Real-time progress tracking with Server-Sent Events
- Enhanced logging with DEBUG.log output file
- Job-based file processing system
- Populate files with URL, filesize, and last modified date
- Terminal progress bar for better visibility

### Improvements

- Refactored logging system with configurable log levels
- Replaced synchronous processing with vectorized Polars solution
- Improved Polars memory efficiency for large datasets
- Updated datakey structure and header titles
- Expanded Django's logging capabilities

### Bug Fixes

- Log file not created in frozen environment
- Write protection preventing backend boot
- False positives in detection
- Circular import error in settings
- Filter null rows only on report column
- Syntax errors in frontend communication
- OSError handling to keep backend running
- Progress tracking display issues in frontend
- UTF-8 encoding for logger

## [v1.0.4] - 2025-09-04

### New Features

- Terminal line separator for better output formatting

### Improvements

- Improved API response handling
- Upgraded logging to accept environment variables or arguments for level setting

### Bug Fixes

- Various bugfixes in API responses
- Memory efficiency improvements

## [v1.0.3] - 2025-08-25

### New Features

- Extended lookup tables for Dutch names and locations
- Additional Dutch surnames (393,468 unique entries)
- Enhanced Dutch first names database
- Eponymous disease list to prevent false positives
- Medical terminology whitelist

### Improvements

- Rebalanced lookup tables to prevent false positives
- Updated Deduce lookup tables cache
- Proper debug flag configuration

### Bug Fixes

- Regex pattern matching issues
- Deduce update compatibility (missing base_config)
- Docker build process for Windows

## [v1.0.2] - 2025-08-18

### New Features

- Custom name detector for improved Dutch name recognition
- Extended unit test cases
- Logging toggle functionality

### Improvements

- Optimized column creation in data processing
- Refactored deduce_handler following DRY principles
- Relocated lookup tables to dedicated folder
- Set cache folder for lookup tables

### Bug Fixes

- Whitespace handling on patient names and keys
- Double Deduce initialization
- Loading of custom lookup tables
- Surname detector test cases

## [v1.0.1] - 2025-08-12

### New Features

- Dedicated deployment folder structure
- Improved Dockerfile with proper volume handling

### Improvements

- Relocated all files to app directory
- Updated project structure for better organization

### Bug Fixes

- Dockerfile help documentation
- Volume mounting issues

## [v1.0.0] - 2025-08-07

### Initial Release

- REST API for text pseudonymization using Deduce algorithm
- Polars-based high-performance vectorized data processing
- Automatic OpenAPI/Swagger documentation
- Custom Dutch lookup tables for improved accuracy
- Pattern matching for names, locations, and institutions
- Detailed logging and error handling

---

## Legend

- **New Features**: New functionality added to the project
- **Improvements**: Enhancements to existing features
- **Bug Fixes**: Resolved issues and corrections
- **Breaking Changes**: Changes that may require updates to existing implementations
- **Security**: Security-related improvements and fixes
