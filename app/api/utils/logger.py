# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Logging setup utilities for processing deidentification jobs API."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from django.utils.translation import gettext as _

if TYPE_CHECKING:
    from api.models import DeidentificationJob


def setup_job_logging(job_id: str, input_file: str, job: DeidentificationJob) -> logging.FileHandler:
    """Create a per-job FileHandler to the 'deidentify' logger."""
    deidentify_logger = logging.getLogger('deidentify')
    job_log_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job_id)
    job_log_dir.mkdir(parents=True, exist_ok=True)

    base_name = Path(input_file).stem
    log_filename = f'{base_name}.log'
    job_log_path = job_log_dir / log_filename

    # Open in write mode to overwrite existing file for this job.
    job_handler = logging.FileHandler(str(job_log_path), mode='w', encoding='utf-8')
    job_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    job_handler.setLevel(deidentify_logger.level)
    deidentify_logger.addHandler(job_handler)

    job.log_file.name = str(Path('output') / str(job_id) / log_filename)
    job.save(update_fields=['log_file'])

    return job_handler


def stage_label(progress_info: dict, fallback: str) -> str:
    """Translate and format a stage label, optionally including row counts."""
    stage = progress_info.get('stage')
    processed = progress_info.get('rows_processed')
    total = progress_info.get('rows_total')

    if not isinstance(stage, str):
        return _(fallback)

    translated_stage = _(stage)

    if processed is not None and total:
        processed_details = _('processed %(processed)s/%(total)s rows') % {'processed': processed, 'total': total}
        return f'{translated_stage} - {processed_details}'

    return translated_stage
