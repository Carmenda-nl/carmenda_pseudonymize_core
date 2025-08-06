# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

import logging
import os


def setup_logging(log_dir='data/output'):
    """
    Set up comprehensive logging with multiple handlers and log levels.

    Args:
        log_dir (str): Directory to store log files

    Returns:
        tuple: deidentify logger
    """
    # ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(message)s')  # Only show the message in terminal

    # setup deidentify logger
    logger = logging.getLogger('deidentify.logger')
    logger.setLevel(logging.DEBUG)

    # clear existing handlers to prevent duplicates
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    # file handler for deidentify log (INFO level and above)
    file_handler = logging.FileHandler(os.path.join(log_dir, 'deidentification.log'))
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)  # Only INFO, WARNING, ERROR, CRITICAL to file
    logger.addHandler(file_handler)

    # console handler for terminal output (DEBUG level and above)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.DEBUG)  # All levels to console
    logger.addHandler(console_handler)

    return logger
