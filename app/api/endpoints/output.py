# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Output endpoints.

Provides API endpoints for:
    - Polling the progress of the running process
    - Streaming the progress of the running process as SSE
    - Retrieving the result of a completed process
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from starlette.responses import JSONResponse, StreamingResponse

from api.endpoints.process import worker
from api.schemas import ProcessResponse, ProgressResponse, RunningResponse, error_responses

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

router = APIRouter(tags=['Output'])


@router.get('/api/progress', responses=error_responses((404, 'No active process found')))
def get_progress(job_id: str = '') -> ProgressResponse:
    """Return the progress of the current process."""
    if worker.tracker is None or worker.job_id != job_id:
        raise HTTPException(status_code=404, detail='No active process found')
    return ProgressResponse.model_validate(worker.tracker.get_progress())


@router.get(
    '/api/process',
    response_model=ProcessResponse,
    responses={
        **error_responses(
            (404, 'No active process found'),
            (500, 'Process failed'),
        ),
        409: {'model': RunningResponse, 'description': 'Process is still running'},
    },
)
def get_result(job_id: str = '') -> ProcessResponse | JSONResponse:
    """Return the result of the current process once it has completed."""
    if worker.tracker is None or worker.job_id != job_id:
        raise HTTPException(status_code=404, detail='No active process found')
    if worker.result is None:
        return JSONResponse(
            status_code=409,
            content={'detail': 'Process is still running', 'percentage': worker.tracker.percentage},
        )

    if 'error' in worker.result:
        raise HTTPException(status_code=500, detail=worker.result['error'])

    return ProcessResponse(preview=worker.result['preview'], metrics=worker.result['metrics'])


@router.get('/api/progress/stream', responses=error_responses((404, 'No active process found')))
async def stream_progress(job_id: str) -> StreamingResponse:
    """Stream the progress of the current process as Server-Sent Events."""
    if worker.tracker is None or worker.job_id != job_id:
        raise HTTPException(status_code=404, detail='No active process found')

    async def event_stream() -> AsyncGenerator[str]:
        last: str | None = None

        while True:
            if worker.tracker is None or worker.job_id != job_id:
                break

            done = worker.result is not None
            payload = {**worker.tracker.get_progress(), 'done': done}
            snapshot = json.dumps(payload, sort_keys=True)

            if snapshot != last:
                yield f'data: {snapshot}\n\n'
                last = snapshot

            if done:
                break

            await asyncio.sleep(0.2)

    return StreamingResponse(
        event_stream(),
        media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )
