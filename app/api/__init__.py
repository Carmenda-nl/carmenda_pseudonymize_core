# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API routers — combines all endpoint routers."""

from fastapi import APIRouter

from api.endpoints.info import router as info_router
from api.endpoints.process import router as process_router

router = APIRouter()

router.include_router(info_router)
router.include_router(process_router)
