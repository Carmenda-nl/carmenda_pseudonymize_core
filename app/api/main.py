# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""FastAPI base and Swagger config.

swagger_ui_parameters hides the schemas in the Swagger interface.
Swagger interface available in /api/docs
"""

from fastapi import FastAPI

from _version import __version__
from api.routes import router

app = FastAPI(
    title='Carmenda deduce',
    version=__version__,
    swagger_ui_parameters={'defaultModelsExpandDepth': -1},
)

app.include_router(router)
