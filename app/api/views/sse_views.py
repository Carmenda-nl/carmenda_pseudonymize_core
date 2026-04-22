# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Server-Sent Events view for real-time job progress updates."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from django.http import StreamingHttpResponse
from django.utils import translation
from django.utils.translation import gettext as _

from api.models import DeidentificationJob
from core.utils.progress_tracker import tracker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from django.http import HttpRequest

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.1  # seconds between tracker polls


def _build_stage_label(
    raw_stage: int | str | None, rows_processed: int | str | None, rows_total: int | str | None, fallback: str
) -> str:
    """Translate and format a stage label, optionally including row counts."""
    if not isinstance(raw_stage, str):
        return fallback

    translated = _(raw_stage)

    if rows_processed is not None and rows_total:
        row_str = _('processed %(processed)s/%(total)s rows') % {
            'processed': rows_processed,
            'total': rows_total,
        }
        return f'{translated} - {row_str}'

    return translated


async def job_progress(request: HttpRequest, job_id: str) -> StreamingHttpResponse:
    """Stream job progress updates as Server-Sent Events."""
    language = getattr(request, 'LANGUAGE_CODE', translation.get_language() or 'nl')

    async def event_stream() -> AsyncGenerator[str]:
        translation.activate(language)

        try:
            job = await DeidentificationJob.objects.aget(pk=job_id)
        except DeidentificationJob.DoesNotExist:
            yield f'data: {json.dumps({"type": "error", "message": f"Job {job_id} not found"})}\n\n'
            return

        logger.debug('SSE connected to job %s (status: %s)', job_id, job.status)

        # Send initial state immediately
        if job.status == 'processing':
            progress_info = tracker.get_progress()
            raw = progress_info['percentage']
            percentage = int(raw) if isinstance(raw, (int, str)) else 0
            stage = _build_stage_label(
                progress_info['stage'],
                progress_info.get('rows_processed'),
                progress_info.get('rows_total'),
                job.status,
            )
        else:
            percentage = 100 if job.status == 'completed' else 0
            stage = _(job.status)

        yield (
            'data: '
            + json.dumps(
                {
                    'type': 'progress_update',
                    'percentage': percentage,
                    'stage': stage,
                    'status': job.status,
                }
            )
            + '\n\n'
        )

        if job.status not in ('processing', 'pending'):
            logger.debug('SSE job %s already in terminal state, closing stream', job_id)
            return

        last_percentage = percentage
        last_stage = stage

        while True:
            await asyncio.sleep(_POLL_INTERVAL)

            job = await DeidentificationJob.objects.aget(pk=job_id)

            if job.status == 'processing':
                progress_info = tracker.get_progress()
                raw = progress_info['percentage']
                current_percentage = int(raw) if isinstance(raw, (int, str)) else 0
                current_stage = _build_stage_label(
                    progress_info['stage'],
                    progress_info.get('rows_processed'),
                    progress_info.get('rows_total'),
                    'Processing',
                )
            else:
                current_percentage = 100 if job.status == 'completed' else 0
                current_stage = _(job.status)

            percentage_changed = abs(current_percentage - last_percentage) >= 1
            stage_changed = current_stage != last_stage

            if percentage_changed or stage_changed:
                yield (
                    'data: '
                    + json.dumps(
                        {
                            'type': 'progress_update',
                            'percentage': current_percentage,
                            'stage': current_stage,
                            'status': job.status,
                        }
                    )
                    + '\n\n'
                )
                last_percentage = current_percentage
                last_stage = current_stage

            if job.status not in ('processing', 'pending'):
                logger.debug('SSE job %s reached terminal state "%s", closing stream', job_id, job.status)
                break

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response
