# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

import logging
import os


def setup_logging(log_dir="data/output"):
    """
    Set up comprehensive logging with multiple handlers and log levels.

    Args:
        log_dir (str): Directory to store log files

    Returns:
        tuple: Main logger and py4j logger
    """
    # ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    # setup main logger
    logger_main = logging.getLogger(__name__)
    logger_main.setLevel(logging.INFO)
    logger_main.handlers.clear()  # clear any existing handlers

    # file handler for main log
    file_handler = logging.FileHandler(os.path.join(log_dir, "main.log"))
    file_handler.setFormatter(formatter)
    logger_main.addHandler(file_handler)

    # setup py4j logger to suppress verbose logs
    logger_py4j = logging.getLogger("py4j")
    logger_py4j.setLevel(logging.ERROR)

    # file handler for py4j log
    py4j_file_handler = logging.FileHandler(os.path.join(log_dir, "py4j.log"))
    py4j_file_handler.setFormatter(formatter)
    logger_py4j.addHandler(py4j_file_handler)

    return logger_main, logger_py4j
