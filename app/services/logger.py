# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for Carmenda pseudonymization core services."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_dir: str = 'data/output', console_level: int | None = None) -> logging.Logger:
    """Set up comprehensive logging with log levels."""
    log_path = Path(log_dir)

    try:
        log_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        error_msg = f'Cannot create log directory "{log_dir}": {e}'
        raise OSError(error_msg) from e

    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setup deidentify logger
    logger = logging.getLogger('deidentify.logger')
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    # Create file handler for deidentify log (INFO level and above)
    log_file_path = log_path / 'deidentification.log'
    try:
        file_handler = logging.FileHandler(str(log_file_path))
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except (OSError, PermissionError) as e:
        error_msg = f'Cannot create log file "{log_file_path}": {e}'
        raise OSError(error_msg) from e

    # Add console handler if requested
    if console_level is not None:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(file_formatter)
        console_handler.setLevel(console_level)
        logger.addHandler(console_handler)

    return logger
