# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Process engine endpoints.

The engine is a worker that processes a single process at a time.

Provides API endpoints for:
    - Submitting a pseudonymization process (rejected with 409 while one is running)
"""

from __future__ import annotations

import contextlib
import dataclasses
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from starlette.status import HTTP_202_ACCEPTED

from api.schemas import ErrorResponse, FileField, InputCols, OptionalFileField, StatusResponse
from core.processor import process_data
from core.utils.file_handling import get_environment
from core.utils.logger import attach_job_log, detach_job_log
from core.utils.progress_tracker import ProgressTracker

router = APIRouter(tags=['Process engine'])


@dataclasses.dataclass
class Worker:
    """State of this single-process worker — the gateway polls progress and fetches the result."""

    tracker: ProgressTracker | None = None
    result: dict[str, Any] | None = None

    @property
    def is_running(self) -> bool:
        """Whether a process is currently being processed (started but no result yet)."""
        return self.tracker is not None and self.result is None


worker = Worker()

# Dedicated worker thread, independent of the HTTP request/response cycle.
# max_workers=1 also enforces the single-process principle at execution level.
executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='process')

# All process input files live under one dedicated temp root, so leftovers
# from a hard-killed process can be wiped safely at the next startup.
TEMP_ROOT = Path(tempfile.gettempdir()) / 'carmenda_deduce'


def cleanup_temp() -> None:
    """Remove input files left behind when a previous process was killed mid-process."""
    shutil.rmtree(TEMP_ROOT, ignore_errors=True)


def cleanup_output() -> None:
    """Remove artifacts from the output folder; the gateway downloads them right after each process."""
    _, output_folder = get_environment()
    output_root = Path(output_folder)

    with contextlib.suppress(OSError):
        for pattern in ('input_pseudonymised*', 'input_key*', '*.log'):
            for artifact in output_root.glob(pattern):
                artifact.unlink(missing_ok=True)


def _run_job(tracker: ProgressTracker, input_file: str, input_cols: str, datakey: str | None, temp_dir: str) -> None:
    """Runs process_data on the worker thread and stores the result on the worker state."""
    log_handler = attach_job_log()
    try:
        result = process_data(
            file=input_file,
            input_cols=input_cols,
            tracker=tracker,
            datakey=datakey,
        )
    except Exception as exc:  # noqa: BLE001 — a failed or cancelled process must free the worker, not crash it
        # Polars wraps exceptions from the row loop, so use our own message for cancellations
        result = {'error': 'Process was cancelled' if tracker.cancel_requested else str(exc)}
    finally:
        # Clean up first, then publish the result — setting worker.result marks the process as done
        with contextlib.suppress(Exception):
            tracker.clean_progress_bar()
        detach_job_log(log_handler)
        shutil.rmtree(temp_dir, ignore_errors=True)

        if 'error' in result:
            cleanup_output()

        worker.result = result


def shutdown_worker() -> None:
    """Cancel the running process (if any) and wait until its cleanup has finished."""
    if worker.tracker is not None:
        worker.tracker.cancel()

    deadline = time.time() + 30
    while worker.is_running and time.time() < deadline:
        time.sleep(0.1)


@router.post(
    '/api/process',
    status_code=HTTP_202_ACCEPTED,
    response_model=StatusResponse,
    responses={409: {'model': ErrorResponse, 'description': 'A process is already running'}},
)
async def process_file(input_file: FileField, input_cols: InputCols, datakey: OptionalFileField = None) -> JSONResponse:
    """Accept a pseudonymization process and run in it."""
    if worker.is_running:
        raise HTTPException(status_code=409, detail='A process is already running')

    cleanup_output()

    TEMP_ROOT.mkdir(exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix='input_', dir=TEMP_ROOT)
    work_dir = Path(temp_dir)

    input_suffix = Path(input_file.filename).suffix if input_file.filename else ''
    input_path = work_dir / f'input{input_suffix}'
    with input_path.open('wb') as f:
        shutil.copyfileobj(input_file.file, f)

    datakey_path = None
    if datakey and datakey.filename:
        datakey_suffix = Path(datakey.filename).suffix
        datakey_path = work_dir / f'datakey{datakey_suffix}'
        with datakey_path.open('wb') as f:
            shutil.copyfileobj(datakey.file, f)

    worker.tracker = ProgressTracker()
    worker.result = None

    executor.submit(
        _run_job,
        worker.tracker,
        str(input_path),
        input_cols,
        str(datakey_path) if datakey_path else None,
        temp_dir,
    )

    return JSONResponse(content={'status': 'accepted'}, status_code=HTTP_202_ACCEPTED)


@router.delete(
    '/api/process',
    status_code=HTTP_202_ACCEPTED,
    response_model=StatusResponse,
    responses={404: {'model': ErrorResponse, 'description': 'No process running'}},
)
async def cancel_process() -> JSONResponse:
    """Cancel the currently running process; it aborts at its next checkpoint and frees the worker."""
    if worker.tracker is None or not worker.is_running:
        raise HTTPException(status_code=404, detail='No process running')

    worker.tracker.cancel()
    return JSONResponse(content={'status': 'cancelling'}, status_code=HTTP_202_ACCEPTED)
