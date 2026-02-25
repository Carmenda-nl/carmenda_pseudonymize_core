# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for processing deidentification jobs API."""

from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings


def setup_job_logging(job_id: str) -> logging.FileHandler:
    """Create a per-job FileHandler to the 'deidentify' logger."""
    deidentify_logger = logging.getLogger('deidentify')
    job_log_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job_id)
    job_log_dir.mkdir(parents=True, exist_ok=True)
    job_log_path = job_log_dir / 'deidentification.log'

    # Open in write mode to overwrite existing file for this job.
    job_handler = logging.FileHandler(str(job_log_path), mode='w', encoding='utf-8')
    job_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    job_handler.setLevel(deidentify_logger.level)
    deidentify_logger.addHandler(job_handler)

    return job_handler
