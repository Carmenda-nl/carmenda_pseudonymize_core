# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Centralised application configuration via pydantic-settings."""

import os
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def _detect_env() -> tuple[str, Path, str, str]:
    """Get proper folder & file paths based on the current environment."""
    if os.environ.get('DOCKER_ENV') == 'true':
        return 'docker', Path('/app/.env'), '/app/data/input', '/app/data/output'
    if getattr(sys, 'frozen', False):
        base = Path(getattr(sys, '_MEIPASS', '.'))
        return 'pyinstaller', base / 'app' / '.env', str(base / 'data' / 'input'), str(base / 'data' / 'output')
    return 'development', Path(__file__).parent.parent / '.env', 'data/input', 'data/output'


env, env_file, input_folder, output_folder = _detect_env()
Path(output_folder).mkdir(parents=True, exist_ok=True)


class Settings(BaseSettings):
    """Settings to configure the API."""

    host: str = 'localhost'
    port: int = 8001
    debug: bool = False
    log_level: str = 'INFO'
    environment: str = env
    input_folder: str = input_folder
    output_folder: str = output_folder
    model_config = SettingsConfigDict(env_file=env_file)


settings = Settings()
