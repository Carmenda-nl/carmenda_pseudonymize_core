# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Health check & application info endpoint."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.schemas import InfoResponse
from main.config import settings

router = APIRouter(tags=['Info'])


@router.get('/info', response_model=InfoResponse)
def app_info() -> JSONResponse:
    """Returns if the api is healthy with status and current base settings."""
    return JSONResponse(
        content={
            'status': 'ok',
            'host': settings.host,
            'port': settings.port,
            'debug': settings.debug,
            'log_level': settings.log_level,
        }
    )
