# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Output endpoints.

The engine is a worker that processes a single job at a time.

"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from api.endpoints.process import worker
from api.schemas import ProcessResponse, ProgressResponse
from core.utils.file_handling import get_environment

router = APIRouter(tags=['Output'])


@router.get('/api/progress', response_model=ProgressResponse)
async def get_progress() -> JSONResponse:
    """Return the progress of the current job."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No job submitted')
    return JSONResponse(content=worker.tracker.get_progress())


@router.get('/api/process', response_model=ProcessResponse)
async def get_result() -> JSONResponse:
    """Return the result of the current job once it has completed."""
    if worker.tracker is None:
        raise HTTPException(status_code=404, detail='No job submitted')
    if worker.result is None:
        raise HTTPException(status_code=409, detail='Job still running')

    if 'error' in worker.result:
        raise HTTPException(status_code=500, detail=worker.result['error'])

    response: dict = {
        'preview': worker.result['preview'],
        'metrics': worker.result['metrics'],
        'output_url': f'/api/download/{Path(worker.result["output_path"]).name}',
    }
    if worker.result.get('datakey_path'):
        response['datakey_url'] = f'/api/download/{Path(worker.result["datakey_path"]).name}'
    if worker.result.get('log_path'):
        response['log_url'] = f'/api/download/{Path(worker.result["log_path"]).name}'

    return JSONResponse(content=response)


@router.get('/api/download/{filename}')
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
