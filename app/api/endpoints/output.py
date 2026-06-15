# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Output endpoints.

Provides API endpoints for:
    - Polling the progress of the running process
    - Retrieving the result of a completed process
    - Downloading output files (result, datakey or log)
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from api.endpoints.process import worker
from api.schemas import ErrorResponse, ProcessResponse, ProgressResponse
from core.utils.file_handling import get_environment

router = APIRouter(tags=['Output'])


@router.get(
    '/api/progress',
    response_model=ProgressResponse,
    responses={404: {'model': ErrorResponse, 'description': 'No process submitted'}},
)
def get_progress() -> ProgressResponse:
    """Return the progress of the current process."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No process submitted')
    return ProgressResponse(**worker.tracker.get_progress())


@router.get(
    '/api/process',
    response_model=ProcessResponse,
    responses={
        404: {'model': ErrorResponse, 'description': 'No process submitted'},
        409: {'model': ErrorResponse, 'description': 'Process still running'},
        500: {'model': ErrorResponse, 'description': 'Process failed'},
    },
)
def get_result() -> ProcessResponse:
    """Return the result of the current process once it has completed."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No process submitted')
    if worker.result is None:
        raise HTTPException(status_code=409, detail='Process still running')

    if 'error' in worker.result:
        raise HTTPException(status_code=500, detail=worker.result['error'])

    response = ProcessResponse(
        preview=worker.result['preview'],
        metrics=worker.result['metrics'],
        output_url=f'/api/download/{Path(worker.result["output_file"]).name}',
    )
    if worker.result.get('datakey'):
        response.datakey_url = f'/api/download/{Path(worker.result["datakey"]).name}'
    if worker.result.get('log'):
        response.log_url = f'/api/download/{Path(worker.result["log"]).name}'

    return response


@router.get(
    '/api/download/{filename}',
    responses={
        400: {'model': ErrorResponse, 'description': 'Invalid filename'},
        404: {'model': ErrorResponse, 'description': 'File not found'},
    },
)
def download_file(filename: str) -> FileResponse:
    """Download a processed output file by filename (output, datakey or log)."""
    output_folder = get_environment()[1]
    file_path = (Path(output_folder) / filename).resolve()
    output_root = Path(output_folder).resolve()

    if not file_path.is_relative_to(output_root):
        raise HTTPException(status_code=400, detail='Invalid filename')
    if not file_path.exists():
        raise HTTPException(status_code=404, detail='File not found')

    return FileResponse(path=str(file_path), filename=filename)
