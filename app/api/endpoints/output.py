# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Output endpoints.

Provides API endpoints for:
    - Polling the progress of the running process
    - Retrieving the result of a completed process
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from api.endpoints.process import worker
from api.schemas import ProcessResponse, ProgressResponse, error_responses

router = APIRouter(tags=['Output'])


@router.get('/api/progress', responses=error_responses((404, 'No process submitted')))
def get_progress(job_id: str = '') -> ProgressResponse:
    """Return the progress of the current process."""
    if worker.tracker is None or worker.job_id != job_id:
        raise HTTPException(status_code=404, detail='No process submitted')
    return ProgressResponse.model_validate(worker.tracker.get_progress())


@router.get(
    '/api/process',
    responses=error_responses(
        (404, 'No process submitted'),
        (409, 'Process is still running'),
        (500, 'Process failed'),
    ),
)
def get_result(job_id: str = '') -> ProcessResponse:
    """Return the result of the current process once it has completed."""
    if worker.tracker is None or worker.job_id != job_id:
        raise HTTPException(status_code=404, detail='No process submitted')
    if worker.result is None:
        raise HTTPException(status_code=409, detail='Process is still running')

    if 'error' in worker.result:
        raise HTTPException(status_code=500, detail=worker.result['error'])

    return ProcessResponse(preview=worker.result['preview'], metrics=worker.result['metrics'])
