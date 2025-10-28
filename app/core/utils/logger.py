# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for Carmenda pseudonymization core services."""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path


def _get_log_level(arg_log_level: str | None = None) -> int:
    """Get log level from argument or environment variable."""
    if arg_log_level:
        return logging.getLevelName(arg_log_level.upper())

    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    return logging.getLevelName(log_level)


def setup_logging(log_level: str | None = None) -> logging.Logger:
    """Set up comprehensive logging with log levels."""
    log_level = _get_log_level(log_level)
    log_path = Path(__file__).resolve().parent.parent.parent / 'data/output'

    try:
        log_path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        warnings.warn(f'Cannot create log directory "{log_path}": {error}', stacklevel=2)

    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setup deidentify logger
    logger = logging.getLogger('deidentify')
    logger.setLevel(log_level)
    logger.propagate = True

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    per_job_only = os.environ.get('PER_JOB_LOG_ONLY', 'false').lower() == 'true'

    if not per_job_only:
        log_file_path = log_path / 'deidentification.log'
        try:
            log_file_path.open('w', encoding='utf-8').close()  # <-- reset log file
            file_handler = logging.FileHandler(str(log_file_path), encoding='utf-8')
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.INFO)
            logger.addHandler(file_handler)
        except (OSError, PermissionError) as error:
            warnings.warn(f'Cannot create log file "{log_file_path}": {error}', stacklevel=2)

    # debug file handler if log level is DEBUG
    if log_level == logging.DEBUG:
        debug_file_path = log_path / 'debug.log'

        try:
            debug_handler = logging.FileHandler(str(debug_file_path), encoding='utf-8')
            debug_handler.setFormatter(file_formatter)
            debug_handler.setLevel(logging.DEBUG)
            logger.addHandler(debug_handler)
        except (OSError, PermissionError) as error:
            warnings.warn(f'Cannot create debug log file "{debug_file_path}": {error}', stacklevel=2)

    return logger


def setup_clean_logger() -> logging.Logger:
    """Set up a logger that outputs clean text without prefixes to console."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add console handler with no formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger


def setup_progress_logger() -> logging.Logger:
    """Set up a dedicated logger for progress tracking that outputs to console."""
    logger = logging.getLogger('progress')
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger


def setup_test_logging() -> logging.Logger:
    """Set up simplified logging specifically for tests."""
    test_formatter = logging.Formatter('%(message)s')

    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    # Add console handler with simple formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(test_formatter)
    logger.addHandler(console_handler)

    return logger
