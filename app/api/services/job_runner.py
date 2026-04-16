# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Running logic for deidentification jobs.

This service handles the following responsibilities:
    - Threading and progress monitoring for running the core processing in the background.
    - Uses WebSocket communication for real-time progress updates to the frontend.
    - Metrics formatting for human-readable performance information.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings
from django.utils.translation import gettext as _

from api.models import DeidentificationJob
from api.utils.logger import setup_job_logging
from core.processor import process_data
from core.utils.logger import setup_logging
from core.utils.progress_control import JobCancelledError, job_control
from core.utils.progress_tracker import tracker

logger = setup_logging()


def _format_metrics(metrics: dict) -> dict:
    """Format raw performance metrics from core into translated human-readable strings."""
    hours = metrics.get('hours', 0)
    minutes = metrics.get('minutes', 0)
    seconds = metrics.get('seconds', 0)
    time_per_row_ms = metrics.get('time_per_row_ms', 0)

    parts = []
    if hours:
        parts.append(_('%(count)d hour') % {'count': hours} if hours == 1 else _('%(count)d hours') % {'count': hours})
    if minutes:
        parts.append(
            _('%(count)d minute') % {'count': minutes} if minutes == 1 else _('%(count)d minutes') % {'count': minutes}
        )
    if seconds or not parts:
        parts.append(
            _('%(count)d second') % {'count': seconds} if seconds == 1 else _('%(count)d seconds') % {'count': seconds}
        )

    and_str = _('and')
    total_time_str = f'{", ".join(parts[:-1])} {and_str} {parts[-1]}' if len(parts) > 1 else parts[0]

    return {
        'total_rows': metrics.get('total_rows'),
        'total_time': total_time_str,
        'time_per_row': f'{time_per_row_ms:.3f} ms',
    }


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
        if 'metrics' in processed_data:
            processed_data['metrics'] = _format_metrics(processed_data['metrics'])
        current_job.processed_preview = processed_data
        current_job.save()

    # Set job fields for files created by the core processor
    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(current_job.job_id)

    core_files = [
        ('output_file', job_output_dir / f'{Path(file).stem}_pseudonymised{Path(file).suffix}'),
        ('output_datakey', next(job_output_dir.glob('*_key.csv'), None)),
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
    last_stage = str(progress_info.get('stage') or 'Cancelled')
    send_process_progress(job_id, progress_percentage, last_stage, 'cancelled')


def _handle_process_error(job_id: str, error: Exception) -> None:
    """Handle processing error: update database and send error update."""
    logger.exception('Processing "%s" failed', job_id)
    error_job = DeidentificationJob.objects.get(pk=job_id)
    error_job.error_message = f'Processing error: {error}'
    error_job.status = 'failed'
    error_job.save()

    send_process_progress(job_id, 0, f'Error: {error}', 'failed')


def _monitor_progress(job_id: str, stop_monitoring: threading.Event, completion_percentage: int = 100) -> None:
    """Monitor processing progress and send updates via WebSocket until stop event is set."""
    last_percentage = -1
    last_stage = ''
    while not stop_monitoring.is_set():
        progress_info = tracker.get_progress()
        raw_percentage = progress_info['percentage']
        current_percentage = int(raw_percentage) if isinstance(raw_percentage, str) else (raw_percentage or 0)
        raw_stage = progress_info['stage']
        rows_processed = progress_info.get('rows_processed')
        rows_total = progress_info.get('rows_total')
        if raw_stage and rows_processed is not None and rows_total:
            current_stage = f'{raw_stage} - processed {rows_processed}/{rows_total} rows'
        else:
            current_stage = str(raw_stage) if raw_stage else 'Processing'

        percentage_changed = (
            abs(current_percentage - last_percentage) >= 1 or current_percentage == completion_percentage
        )
        stage_changed = current_stage != last_stage

        if percentage_changed or stage_changed:
            send_process_progress(job_id, current_percentage, current_stage)
            last_percentage = current_percentage
            last_stage = current_stage

        stop_monitoring.wait(0.1)


def run_processing(job_id: str, input_file: str, input_cols: str, output_cols: str, datakey: str) -> None:
    """Run processing in background thread."""
    current_job = DeidentificationJob.objects.get(pk=job_id)
    job_handler = setup_job_logging(job_id, input_file, current_job)

    try:
        with job_control.run_job(job_id):
            # Set up progress monitoring
            stop_monitoring = threading.Event()
            completion_percentage = 100

            # Start progress monitoring in separate thread
            monitor_thread = threading.Thread(
                target=_monitor_progress,
                args=(job_id, stop_monitoring, completion_percentage),
                daemon=True,
            )
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
            tracker.clean_progress_bar()
        except (AttributeError, RuntimeError) as error:
            logger.debug('Failed to finalize progress for %s: %s', job_id, error)
