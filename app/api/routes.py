# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""FastAPI routes for the pseudonymization API.

This module provides the API endpoints for:
    - Receiving uploaded report files and optional datakeys
    - Forwarding data to the processing core
    - Returning pseudonymized results with metrics in Json format
"""

import asyncio
import tempfile
from functools import partial
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.schemas import FileField, InputCols, OptionalFileField, ProcessResponse, ProgressResponse
from core.processor import process_data
from core.utils.progress_tracker import tracker

router = APIRouter()


@router.post('/api/process/', tags=['Process engine'], response_model=ProcessResponse)
async def process_file(input_file: FileField, input_cols: InputCols, datakey: OptionalFileField = None) -> JSONResponse:
    """Pseudonymize the uploaded report and return preview, metrics and download URLs."""
    with tempfile.TemporaryDirectory(prefix='input_') as temp_file:
        work_dir = Path(temp_file)

        input_suffix = Path(input_file.filename).suffix if input_file.filename else ''
        input_path = work_dir / f'input{input_suffix}'
        input_path.write_bytes(await input_file.read())

        datakey_path = None
        if datakey and datakey.filename:
            datakey_suffix = Path(datakey.filename).suffix
            datakey_path = work_dir / f'datakey{datakey_suffix}'
            datakey_path.write_bytes(await datakey.read())

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            partial(
                process_data,
                input_file=str(input_path),
                input_cols=input_cols,
                datakey=str(datakey_path) if datakey_path else None,
            ),
        )

    response = {
        'preview': result['preview'],
        'metrics': result['metrics'],
        'output_url': f'/api/download/{Path(result["output_path"]).name}',
    }

    if result['datakey_path']:
        response['datakey_url'] = f'/api/download/{Path(result["datakey_path"]).name}'

    if result['log_path']:
        response['log_url'] = f'/api/download/{Path(result["log_path"]).name}'

    return JSONResponse(content=response)


@router.get('/api/progress/', tags=['Process engine'], response_model=ProgressResponse)
async def get_progress() -> JSONResponse:
    """Return the current processing progress."""
    return JSONResponse(content=tracker.get_progress())
