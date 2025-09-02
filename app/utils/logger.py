# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for Carmenda pseudonymization core services."""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path


def setup_logging(log_level: int = 20) -> logging.Logger:
    """Set up comprehensive logging with log levels."""
    log_dir = str(Path(tempfile.gettempdir()) / 'deidentification_logs') if hasattr(sys, '_MEIPASS') else 'data/output'
    log_path = Path(log_dir)

    try:
        log_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        error_msg = f'Cannot create log directory "{log_dir}": {e}'
        raise OSError(error_msg) from e

    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setup deidentify logger
    logger = logging.getLogger('deidentify')
    logger.setLevel(log_level)
    logger.propagate = True  # <- disables logging to console if `False`

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    # Log to file (INFO level only)
    log_file_path = log_path / 'deidentification.log'
    try:
        file_handler = logging.FileHandler(str(log_file_path))
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except (OSError, PermissionError) as e:
        error_msg = f'Cannot create log file "{log_file_path}": {e}'
        raise OSError(error_msg) from e

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
