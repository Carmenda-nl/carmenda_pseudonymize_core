# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Config app for the Django project."""

from django.apps import AppConfig


class ConfigConfig(AppConfig):
    """Base configuration for the current app."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'config'
