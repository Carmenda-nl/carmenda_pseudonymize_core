# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] - 2026-08-03

### Added

- Log exception details when a job fails
- Track python version file

### Changed

- Cleanup workflows
- Update changelog
- Addres to new repo in cliff.toml
- Readme & remove bruno files
- Workflows to use shared workflows
- Old refs in build files
- Update packages & fix polars decrement

### Fixed

- Lowercase log_level before passing to uvicorn
- Version file not properly promoted to stable

## [2.0.0] - 2026-07-10

### Added

- Install fastapi [LVD-296]
- Extend json response from processor
- Add a pydantic model to schema & extend route logic
- Pydantic based settings & .env support
- Fastapi base & swagger config
- Api info & health endpoint
- Lifespan cleanup & shutdown
- Process API endpoint
- Extend error responses & file cleanup on cancel
- Extend tracker status with cancel & running
- Extra description to schemas
- Create input folder when not available
- Engine version to info endpoint
- Improve output endpoints response
- App title to .env, endpoint & settings
- SSE endpoint stream
- Machine to machine (m2m) secure connection

### Changed

- Remove django's main & settings
- Update environment files to fastAPI & python 3.14
- Replace old django api with a new fastAPI
- Remove output_cols from core.py + minor lint fixes
- Update packages & cleanup pyproject.toml
- Update make-file & remove old core script
- Makefile & packages
- Relocate lookup tables
- Update info endpoint url
- Reorganise core
- Cleanup output from lifespan
- Relocate deduce based code
- Remove old progress control
- Add progress tracker as argument
- Log file only created when job is running
- Cleanup logger & fix log_level not from .env
- Worker progress tracker accepts cancel
- Naming fixes & minor refactor in processor pipeline
- API schema's update & add info response
- Rename info to app_info & add extra routes
- Restructure endpoints
- Improve docstring
- Combine & add a `check` make command
- Ruff lint fixes
- Remove refs to job to improve code readability
- Replace JSONResponse with response models
- Mypy lint error fixes
- Refactor code & cleanup
- Typo fixes
- Fix tracker not done when error in pipeline
- Ruff lint fixes
- Update docstrings
- Refactor process.py
- Move .env & folder settings to one config in main
- Minor code typo fixes
- Update deployment files to work with fastapi
- Update bruno collections
- Github action scripts updated
- Remove old naming
- Id based process with paths instead of downloads
- Update bruno files
- Make job_id optional
- Field explanation extended
- Upgrade packages
- Move file checks to api-gateway
- Update readme
- Update bruno project files
- Update bruno environments
- Readme add path mount for docker env
- Minor bugfix unknown model
- Update readme
- Update bruno files
- Cleanup unused packages
- Improve naming & docstrings
- Update readme & bruno files
- Update readme
- Build.spec cleanup
- Rename pyinstaller naming to frozen
- Remove old unused stages
- Update docstring & bruno files
- Enforce LF
- Update readme
- Update bruno files with headers
- Update readme
- Filter & clean info log
- Refactor to fix ruff lint errors
- Update all packages
- Update readme
- Downgrade changelog
- Update pipelines to latest
- Ruff lint error double quotes in version
- Worflows writes version with double quotes
- Pre release v2.0.0

### Fixed

- Fix older core tests
- Naming missmatch fixed
- Ruff lint error fixes
- Update output location (used old logic)
- Fix error_file not properly closed
- Fix output files were not properly renamed
- Fix log got cleaned on an error
- Fix gcc requires build-essentials in python 3.14
- Remove dublicate status|stage
- Fix output folder created when not needed
- Create a folder when not available
- Fix files not properly loading
- Fix lint error
- Accept a relative or absolute path:
- Fix file handling removes output subfolders
- Fix memory overload in docker env
- Fix unmapped_cols not parsed to df & missing in output
- Fix column order got corrupt in output
- Fix rich progress bar hangs on 99%
- Open PR activates prepare stable pipeline
- Pipelines not properly finished (403)

## [1.6.3] - 2026-06-10

### Changed

- Update & bugfix workflows
- Minor ruff quote usage fix
- Changelog

### Fixed

- App version not properly updated
- Serializer had a max of 100 characters
- Bugfix workflows

## [1.6.2] - 2026-05-18

### Added

- Get current version in API docs [LVD-293]
- Multiline help text in api input_cols [LVD-294]

### Fixed

- Fix single report not processed [LVD-299]

## [1.6.1] - 2026-05-11

### Fixed

- Closing thread fails when file stil in use
- Wait for background thread to finish before garbage collection

## [1.6.0] - 2026-05-08

### Added

- Add missing v1.5.0 notes
- Proper git attributes
- Replace_synonym multi report support
- Multi report support for deidentify_text
- Add multi report support in API

### Changed

- Datakey always renamed to filename_key.csv
- Changelog accept merge from remote
- Ruff format
- Update dependencies to latest
- Changelog update
- Clean up CHANGELOG.md by removing duplicate entries
- Revise CHANGELOG.md for version 1.6.0-beta

### Fixed

- Datakey missed in zip preview [LVD-285]
- Keep regular text between <> [LVD-266]
- Set default language to dutch
- Fix changelog.md not properly updated
- Fix ruff format error
- Catch error when no report in input_cols
- Workflow generates dublicate releases in changelog.md
- Do not remove older changelogs

## [1.5.0] - 2026-04-28

### Added

- SSE endpoint for progress tracking
- Translatable stage labels in api
- Available languages to settings endpoint
- Ceate a Bruno collection to test the API endpoints
- Dutch translations for api validators

### Changed

- uv sync --update packages
- Core transl. to api serializer
- Refactor performance metrics
- Move django-channels to warning lvl for cleaner debug
- Update django logger & levels
- Refactor & organize jobs_view
- Silence charset_normalizer in log
- Old code cleanup

### Fixed

- Remove django transl. dependencie from core
- Translation string mismatch
- Clean input_cols, datakey & consent when new input_file
- Lazy load git in version.py & clean log
- Settings_log not properly loaded in frozen environment
- Gettext shadowed by local tuple-unpack var
- Consent_file not properly: renamed, deleted & handled
- Metrics not translating in thread

## [1.4.2] - 2026-03-31

### Added

- Auto gen changelogs
- Metrics output for API
- Progress metrics (LVD-219) (LVD-222)
- App version control (LVD-218)
- Write version to file logic
- 3 head & 3 tail rows for preview

### Changed

- Overwrite storage, refactor reset_output & add consent_file to API
- Refactor & bugfix DeidentificationJobViewSet `PUT`
- Remove hardcoded file paths in `collect_output_files`
- Update changelog & readme (version control)
- Move packaging files to own endpoint
- Create a seperate serializer for API zip function
- Convert project to a uv based
- Move firstnames to common words (LVD-241)
- Action workflow scripts
- Python dependencies updated
- Refactor time metrics & add hours

### Fixed

- Newlines not rendered properly
- No proper Keep a Changelog format
- Pickle file not properly located
- Require the first word to be a known name
- Update lookup tables
- Fix double version.txt creation
- Version not properly located in frozen env
- Error when git not installed

## [1.3.1] - 2026-03-05

### Added

- Permission boolean
- API consent file build logic
- A zip files endpoint

### Changed

- Moved all test outcome files to one folder
- Overwrite storage, refactor reset_output
- Remove hardcoded file paths in collect_output_files

### Fixed

- Bugfix: boolean field not updated on PUT
- Refactor + bugfix jobs API
- Prevent silent failing
- improved Unescape HTML entities

## [1.3.0] - 2026-02-11

### Added

- Uploading basic excel files

### Changed

- Upgrade used py packages
- CSV file handling

### Fixed

- Faulty CSV file rows are not collected in a seperate file
- build.spec creates double cache folder
- Solved all type errors with mypy

## [1.2.10] - 2026-01-06

### Changed

- Update polars v1.36.1
- Copyright year 2026

### Fixed

- Ignore quote char in strings
- Minor false positives in lookup tables

## [1.2.9] - 2025-12-04

### Added

- Core: Major coverage update, adding missing first names
- Core: Clean HTML tags in report text

### Fixed

- Prevent confused parser on semicolons
- Cleanup UTF-8 BOM characters from header per column

## [1.2.8] - 2025-11-25

### Added

- API: update files, input_cols & fileMeta trough PUT
- API: Extra checks on input file and datakey
- API: Error row collector
- Core: Pre-process clientnames as case-insensitive

### Changed

- API: input_cols is optional when creating a job
- API: closer in functionality of frontend
- API: Improved file handler for robust encoding support
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
