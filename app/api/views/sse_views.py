# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Server-Sent Events view for real-time job progress updates."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from django.http import StreamingHttpResponse
from django.utils import translation
from django.utils.translation import gettext as _

from api.models import DeidentificationJob
from api.utils.logger import stage_label
from core.utils.progress_tracker import tracker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from django.http import HttpRequest

POLL_INTERVAL = 0.1


def _json_format(**data: object) -> str:
    return f'data: {json.dumps(data)}\n\n'


async def progress(request: HttpRequest, job_id: str) -> StreamingHttpResponse:
    """Stream job progress updates as Server-Sent Events."""
    language = getattr(request, 'LANGUAGE_CODE', translation.get_language())

    async def event_stream() -> AsyncGenerator[str]:
        translation.activate(language)

        job = await DeidentificationJob.objects.aget(pk=job_id)

        if job.status == 'processing':
            progress_info = tracker.get_progress()
            percentage = progress_info['percentage']
            stage = stage_label(progress_info, job.status)
        else:
            percentage = 100 if job.status == 'completed' else 0
            stage = _(job.status)

        yield _json_format(percentage=percentage, stage=stage, status=job.status)
        last_percentage = int(percentage) if isinstance(percentage, (int, str)) else 0
        last_stage = stage

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            job = await DeidentificationJob.objects.aget(pk=job_id)

            if job.status == 'processing':
                progress_info = tracker.get_progress()
                percentage = progress_info['percentage']
                current_percentage = int(percentage) if isinstance(percentage, (int, str)) else 0
                current_stage = stage_label(progress_info, job.status)
            else:
                current_percentage = 100 if job.status == 'completed' else 0
                current_stage = _(job.status)

            percentage_changed = abs(current_percentage - last_percentage) >= 1
            stage_changed = current_stage != last_stage

            if percentage_changed or stage_changed:
                yield _json_format(percentage=current_percentage, stage=current_stage, status=job.status)
                last_percentage = current_percentage
                last_stage = current_stage

            if job.status not in ('processing', 'pending'):
                break

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'

    return response
