# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Process engine endpoints.

The engine is a worker that processes a single process at a time.

Provides API endpoints for:
   - Submitting a pseudonymization process (rejected with 409 while one is running)
   - cancel a pseudonymization process (404 if none is running)
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_202_ACCEPTED

from api.schemas import DatakeyPath, FilePath, InputCols, JobId, StatusResponse, error_responses
from api.utils.file_handling import cleanup_output
from api.utils.worker import run_job, worker
from core.utils.progress_tracker import ProgressTracker
from main.config import settings

router = APIRouter(tags=['Process engine'])
executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='process')


@router.post(
    '/api/process',
    status_code=HTTP_202_ACCEPTED,
    responses=error_responses(
        (409, 'A process is running'),
        (400, 'Invalid file path'),
        (404, 'File not found'),
    ),
)
def process_file(file: FilePath, cols: InputCols, job_id: JobId = '', datakey: DatakeyPath = '') -> StatusResponse:
    """Submit a pseudonymization process session."""
    if worker.is_running:
        raise HTTPException(status_code=409, detail='A process is already running')

    input_root = Path(settings.input_folder).resolve()
    base_input = (input_root / job_id).resolve() if job_id else input_root
    file_path = Path(file)
    input_path = (base_input / file_path).resolve() if not file_path.is_absolute() else file_path.resolve()

    if not input_path.is_relative_to(input_root):
        raise HTTPException(status_code=400, detail='Invalid file path')
    if not input_path.exists():
        raise HTTPException(status_code=404, detail='File not found')

    output_root = Path(settings.output_folder).resolve()
    output_path = (output_root / job_id).resolve() if job_id else output_root

    if not output_path.is_relative_to(output_root):
        raise HTTPException(status_code=400, detail='Invalid job id')

    output_path.mkdir(parents=True, exist_ok=True)

    if datakey:
        datakey_file_path = Path(datakey)
        datakey_input_path = str(
            (base_input / datakey_file_path).resolve()
            if not datakey_file_path.is_absolute()
            else datakey_file_path.resolve()
        )
        if not Path(datakey_input_path).is_relative_to(input_root):
            raise HTTPException(status_code=400, detail='Invalid datakey path')
    else:
        datakey_input_path = ''

    cleanup_output()

    worker.job_id = job_id
    worker.tracker = ProgressTracker()
    worker.result = None

    executor.submit(run_job, str(input_path), cols, datakey_input_path, worker.tracker, str(output_path))
    return StatusResponse(status='accepted')


@router.delete('/api/process', status_code=HTTP_202_ACCEPTED, responses=error_responses((404, 'No process running')))
def cancel_process() -> StatusResponse:
    """Cancel the running process (if any) and wait until its cleanup has finished."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No process running')

    worker.tracker.cancel()
    return StatusResponse(status='cancelling')
