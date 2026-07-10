# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API routers — combines all endpoint routers."""

from fastapi import APIRouter, Depends

from api.endpoints.app_info import router as info_router
from api.endpoints.output import router as output_router
from api.endpoints.process import router as process_router
from api.utils.security import verify_m2m

router = APIRouter()

router.include_router(info_router)
router.include_router(process_router, dependencies=[Depends(verify_m2m)])
router.include_router(output_router, dependencies=[Depends(verify_m2m)])
