# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views package.

This package contains the API views for the Django backend.
"""

from api.views.jobs import DeidentificationJobViewSet
from api.views.root import APIRootView

__all__ = ['APIRootView', 'DeidentificationJobViewSet']
