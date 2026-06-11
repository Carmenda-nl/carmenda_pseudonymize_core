# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Centralised application configuration via pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings to configure the API."""

    host: str = 'localhost'
    port: int = 8001
    debug: bool = False
    log_level: str = 'INFO'

    model_config = SettingsConfigDict(env_file='.env')


settings = Settings()
