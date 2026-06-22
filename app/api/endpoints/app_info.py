# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Health check & application info endpoint.

Provides API endpoints for:
    - Health check and current base settings (`GET /api/info`)
"""

from fastapi import APIRouter

from api.schemas import InfoResponse
from main.config import settings

router = APIRouter(tags=['Info'])


@router.get('/api/info')
def app_info() -> InfoResponse:
    """Returns if the api is healthy with status and current base settings."""
    return InfoResponse(
        status='ok',
        host=settings.host,
        port=settings.port,
        debug=settings.debug,
        log_level=settings.log_level,
        environment=settings.environment,
        input_folder=settings.input_folder,
        output_folder=settings.output_folder,
    )
