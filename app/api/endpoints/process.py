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

import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_202_ACCEPTED

from api.schemas import FileField, InputCols, OptionalFileField, StatusResponse, error_responses
from api.utils.file_handling import cleanup_output
from api.utils.worker import run_job, worker
from core.utils.progress_tracker import ProgressTracker

router = APIRouter(tags=['Process engine'])
executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='process')


@router.post('/api/process', status_code=HTTP_202_ACCEPTED, responses=error_responses((409, 'A process is running')))
async def process_file(file: FileField, input_cols: InputCols, datakey: OptionalFileField = None) -> StatusResponse:
    """Submit a pseudonymization process session."""
    if worker.is_running:
        raise HTTPException(status_code=409, detail='A process is already running')

    cleanup_output()

    temp_root = Path(tempfile.gettempdir()) / 'Carmenda'
    temp_root.mkdir(exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix='input_', dir=temp_root))

    input_filename = Path(cast('str', file.filename)).name
    temp_path = temp_dir / input_filename

    with temp_path.open('wb') as process_file:
        shutil.copyfileobj(file.file, process_file)

    if datakey:
        datakey_suffix = Path(cast('str', datakey.filename)).suffix
        datakey_path = temp_dir / f'datakey{datakey_suffix}'

        with datakey_path.open('wb') as datakey_file:
            shutil.copyfileobj(datakey.file, datakey_file)
    else:
        datakey_path = None

    worker.tracker = ProgressTracker()
    worker.result = None

    executor.submit(run_job, worker.tracker, str(temp_path), input_cols, str(datakey_path), str(temp_dir))
    return StatusResponse(status='accepted')


@router.delete('/api/process', status_code=HTTP_202_ACCEPTED, responses=error_responses((404, 'No process running')))
def cancel_process() -> StatusResponse:
    """Cancel the running process (if any) and wait until its cleanup has finished."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No process running')

    worker.tracker.cancel()
    return StatusResponse(status='cancelling')
