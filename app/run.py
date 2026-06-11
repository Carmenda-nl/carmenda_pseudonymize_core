# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""FastAPI base and Swagger config."""

import logging

import uvicorn
from fastapi import FastAPI

from api import router
from main._version import __version__
from main.config import settings

logging.getLogger('asyncio').setLevel(logging.WARNING)


app = FastAPI(
    title='Carmenda deduce',
    version=__version__,
    swagger_ui_parameters={'defaultModelsExpandDepth': -1},
    docs_url='/docs' if settings.debug else None,
    openapi_url='/openapi.json' if settings.debug else None,
    redoc_url=None,
)

app.include_router(router)

if __name__ == '__main__':
    uvicorn.run('run:app', host=settings.host, port=settings.port, reload=settings.debug, log_level=settings.log_level)
