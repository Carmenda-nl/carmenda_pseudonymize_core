# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Background job runner for deidentification jobs.

Responsible for:
    - Starting processing in a background thread.
    - Updating job status in the database on completion, cancellation, or error.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from django.conf import settings

from api.models import DeidentificationJob
from api.utils.logger import setup_job_logging
from core.processor import process_data
from core.utils.logger import setup_logging
from core.utils.progress_control import JobCancelledError, job_control
from core.utils.progress_tracker import tracker

logger = setup_logging()


def _handle_process_completion(current_job: DeidentificationJob, processor: str, file: str) -> None:
    """Handle processing completion: save preview, create zip, and update job status."""
    processed_data = json.loads(processor)
    current_job.processed_preview = processed_data
    current_job.save()

    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(current_job.job_id)
    datakey_output_name = f'{Path(file).stem}_key.csv'

    core_files = [
        ('output_file', job_output_dir / f'{Path(file).stem}_pseudonymised{Path(file).suffix}'),
        ('output_datakey', job_output_dir / datakey_output_name),
        ('skipped_lines', next(job_output_dir.glob('*_skipped_lines.csv'), None)),
    ]

    save_fields = []
    for field_name, path in core_files:
        if path is not None and path.exists():
            getattr(current_job, field_name).name = str(path.relative_to(Path(settings.MEDIA_ROOT)))
            save_fields.append(field_name)
    if save_fields:
        current_job.save(update_fields=save_fields)

    current_job.status = 'completed'
    current_job.save()


def _handle_process_cancellation(job_id: str) -> None:
    """Handle processing cancellation: update database and send cancellation update."""
    logger.info('Processing "%s" cancelled by user', job_id)
    cancelled_job = DeidentificationJob.objects.get(pk=job_id)
    cancelled_job.error_message = 'Processing cancelled by user'
    cancelled_job.status = 'cancelled'
    cancelled_job.save()


def _handle_process_error(job_id: str, error: Exception) -> None:
    """Handle processing error: update database and send error update."""
    logger.exception('Processing "%s" failed', job_id)
    error_job = DeidentificationJob.objects.get(pk=job_id)
    error_job.error_message = f'Processing error: {error}'
    error_job.status = 'failed'
    error_job.save()


def run_processing(job_id: str, input_file: str, input_cols: str, output_cols: str, datakey: str) -> None:
    """Run processing in background thread."""
    current_job = DeidentificationJob.objects.get(pk=job_id)
    job_handler = setup_job_logging(job_id, input_file, current_job)

    try:
        with job_control.run_job(job_id):
            processor = process_data(
                input_file=input_file,
                input_cols=input_cols,
                output_cols=output_cols,
                datakey=datakey,
            )

            _handle_process_completion(current_job, processor, input_file)

    except JobCancelledError:
        _handle_process_cancellation(job_id)
    except Exception as error:
        logger.exception('Unexpected error during run process %s', job_id)
        _handle_process_error(job_id, error)
    finally:
        deidentify_logger = logging.getLogger('deidentify')
        deidentify_logger.removeHandler(job_handler)
        job_handler.close()

        try:
            # Ensure the progress bar is finalized to avoid leftover UI/console output
            tracker.clean_progress_bar()
        except (AttributeError, RuntimeError) as error:
            logger.debug('Failed to finalize progress for %s: %s', job_id, error)
