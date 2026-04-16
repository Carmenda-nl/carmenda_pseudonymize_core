# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Progress monitoring for deidentification jobs.

This module handles the following responsibilities:
    - Sending real-time progress updates to the frontend via WebSocket.
    - Polling the progress tracker and forwarding changes while processing is running.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from core.utils.progress_tracker import tracker

if TYPE_CHECKING:
    import threading


def push_progress_update(job_id: str, percentage: int, stage: str, job_status: str = 'processing') -> None:
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


def monitor_progress(job_id: str, stop_monitoring: threading.Event, completion_percentage: int = 100) -> None:
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
            push_progress_update(job_id, current_percentage, current_stage)
            last_percentage = current_percentage
            last_stage = current_stage

        stop_monitoring.wait(0.1)

