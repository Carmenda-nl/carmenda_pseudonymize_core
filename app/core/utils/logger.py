# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for Carmenda pseudonymization core services."""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def setup_logging() -> logging.Logger:
    """Set up the deidentify logger. The log file itself is only open while a process runs."""
    level: int = logging.getLevelName(os.environ.get('LOG_LEVEL', 'INFO').upper())

    # Silence asyncio debug log
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    logger = logging.getLogger('deidentify')
    logger.setLevel(level)
    logger.propagate = True

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    return logger


def attach_job_log() -> logging.FileHandler | None:
    """Open the process log file and attach it to the deidentify logger for the duration of a process."""
    log_path = Path(__file__).resolve().parent.parent.parent / 'data/output'
    log_file_path = log_path / 'deidentification.log'

    try:
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_file_path), mode='w', encoding='utf-8')
    except (OSError, PermissionError) as error:
        warnings.warn(f'Cannot create log file "{log_file_path}": {error}', stacklevel=2)
        return None

    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    file_handler.setLevel(logging.INFO)
    logging.getLogger('deidentify').addHandler(file_handler)
    return file_handler


def detach_job_log(file_handler: logging.FileHandler | None) -> None:
    """Detach and close the process log file, so it is no longer locked between processes."""
    if file_handler is None:
        return

    logging.getLogger('deidentify').removeHandler(file_handler)
    file_handler.close()


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
