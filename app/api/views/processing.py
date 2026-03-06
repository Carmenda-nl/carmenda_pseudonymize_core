# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Background processing logic for deidentification jobs."""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings

from api.models import DeidentificationJob
from api.utils.logger import setup_job_logging
from core.processor import process_data
from core.utils.logger import setup_logging
from core.utils.progress_control import JobCancelledError, job_control
from core.utils.progress_tracker import tracker

logger = setup_logging()


def send_process_progress(job_id: str, percentage: int, stage: str, job_status: str = 'processing') -> None:
    """Send processing progress update via WebSocket."""
    channel_layer = get_channel_layer()
    if channel_layer:
        async_to_sync(channel_layer.group_send)(
            f'job_progress_{job_id}',
            {
                'type': 'job_progress',
                'percentage': percentage,
                'stage': stage,
                'status': job_status,
            },
        )


def _handle_process_completion(job_id: str, current_job: DeidentificationJob, processor: str | None, file: str) -> None:
    """Handle processing completion: save preview, create zip, and send completion update."""
    if processor:
        processed_data = json.loads(processor)
        current_job.processed_preview = processed_data
        current_job.save()

    # Set job fields for files created by the core processor
    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(current_job.job_id)

    core_files = [
        ('output_file', job_output_dir / f'{Path(file).stem}_pseudonymised{Path(file).suffix}'),
        ('output_datakey', next(job_output_dir.glob('*_key.csv'), None)),
        ('error_rows_file', next(job_output_dir.glob('*_errors.csv'), None)),
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

    send_process_progress(job_id, 100, 'Completed', 'completed')


def _handle_process_cancellation(job_id: str) -> None:
    """Handle processing cancellation: update database and send cancellation update."""
    logger.info('Processing "%s" Cancelled by user', job_id)
    cancelled_job = DeidentificationJob.objects.get(pk=job_id)
    cancelled_job.error_message = 'Processing cancelled by user'
    cancelled_job.status = 'cancelled'
    cancelled_job.save()

    progress_info = tracker.get_progress()
    percentage = progress_info.get('percentage', 0)
    progress_percentage = int(percentage) if isinstance(percentage, str) else (percentage or 0)
    send_process_progress(job_id, progress_percentage, 'Cancelled', 'cancelled')


def _handle_process_error(job_id: str, error: Exception) -> None:
    """Handle processing error: update database and send error update."""
    logger.exception('Processing "%s" failed', job_id)
    error_job = DeidentificationJob.objects.get(pk=job_id)
    error_job.error_message = f'Processing error: {error}'
    error_job.status = 'failed'
    error_job.save()

    send_process_progress(job_id, 0, f'Error: {error}', 'failed')


def run_processing(job_id: str, input_file: str, input_cols: str, output_cols: str, datakey: str) -> None:
    """Run processing in background thread."""
    current_job = DeidentificationJob.objects.get(pk=job_id)
    job_handler = setup_job_logging(job_id, input_file, current_job)

    try:
        with job_control.run_job(job_id):
            tracker.reset()

            # Set up progress monitoring
            stop_monitoring = threading.Event()
            completion_percentage = 100

            def monitor_progress() -> None:
                """Monitor tracker progress and send updates via WebSocket."""
                last_percentage = -1
                while not stop_monitoring.is_set():
                    progress_info = tracker.get_progress()
                    raw_percentage = progress_info['percentage']
                    current_percentage = (
                        int(raw_percentage) if isinstance(raw_percentage, str) else (raw_percentage or 0)
                    )
                    raw_stage = progress_info['stage']
                    current_stage = str(raw_stage) if raw_stage else 'Processing'

                    if abs(current_percentage - last_percentage) >= 1 or current_percentage == completion_percentage:
                        send_process_progress(job_id, current_percentage, current_stage)
                        last_percentage = current_percentage

                    stop_monitoring.wait(0.1)

            # Start progress monitoring in separate thread
            monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
            monitor_thread.start()

            processor = process_data(
                input_file=input_file,
                input_cols=input_cols,
                output_cols=output_cols,
                datakey=datakey,
            )

            stop_monitoring.set()
            monitor_thread.join(timeout=1.0)

            _handle_process_completion(job_id, current_job, processor, input_file)

    except JobCancelledError:
        _handle_process_cancellation(job_id)

    except Exception as error:
        logger.exception('Unexpected error during run process %s', job_id)
        _handle_process_error(job_id, error)
    finally:
        deidentify_logger = logging.getLogger('deidentify')
        deidentify_logger.removeHandler(job_handler)
        job_handler.close()

        # Stop and join the monitor thread if it exists
        try:
            if 'stop_monitoring' in locals():
                stop_monitoring.set()
            if 'monitor_thread' in locals():
                monitor_thread.join(timeout=1.0)
        except (AttributeError, RuntimeError) as error:
            logger.debug('Failed to stop monitor thread for %s: %s', job_id, error)

        try:
            # Ensure the progress bar is finalized to avoid leftover UI/console output
            tracker.finalize_progress(complete='no')
        except (AttributeError, RuntimeError) as error:
            logger.debug('Failed to finalize progress for %s: %s', job_id, error)
