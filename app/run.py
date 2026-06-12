# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""FastAPI base and Swagger config."""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI

from api import router
from api.endpoints.process import cleanup_temp, shutdown_worker
from main._version import __version__ as app_version
from main.config import settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

# Silence asyncio
logging.getLogger('asyncio').setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
    """Wipe stale jobs at startup; on shutdown, properly cancel any running job."""
    cleanup_temp()
    yield
    shutdown_worker()


app = FastAPI(
    title='Carmenda deduce',
    version=app_version,
    lifespan=lifespan,
    swagger_ui_parameters={'defaultModelsExpandDepth': -1},
    docs_url='/docs' if settings.debug else None,
    openapi_url='/openapi.json' if settings.debug else None,
    redoc_url=None,
)

app.include_router(router)

if __name__ == '__main__':
    uvicorn.run('run:app', host=settings.host, port=settings.port, reload=settings.debug, log_level=settings.log_level)
