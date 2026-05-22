# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API routes."""

import json
import tempfile
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import JSONResponse

from core.processor import process_data

FileField = Annotated[UploadFile, File()]
OptionalFileField = Annotated[UploadFile | None, File()]
FormStr = Annotated[str, Form()]

router = APIRouter()


@router.post('/api/process/', tags=['Process engine'])
async def process_file(input_file: FileField, input_cols: FormStr, datakey: OptionalFileField = None) -> JSONResponse:
    """Pseudonymize the uploaded report and return with metrics."""
    with tempfile.TemporaryDirectory(prefix='input_') as temp_file:
        work_dir = Path(temp_file)

        input_suffix = Path(input_file.filename).suffix if input_file.filename else ''
        input_path = work_dir / f'input{input_suffix}'
        input_path.write_bytes(await input_file.read())

        datakey_path = None
        if datakey:
            datakey_suffix = Path(datakey.filename).suffix if datakey.filename else ''
            datakey_path = work_dir / f'datakey{datakey_suffix}'
            datakey_path.write_bytes(await datakey.read())

        result = process_data(
            input_file=str(input_path),
            input_cols=input_cols,
            datakey=str(datakey_path) if datakey_path else None,
        )

        result = json.loads(result)
        return JSONResponse(content=result)
