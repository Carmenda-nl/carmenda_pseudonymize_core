# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Django logging configuration.

ASGI has logger overrules (too much noise in production)
"""

from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def get_log_level() -> str:
    """Get the LOG_LEVEL from Django settings."""
    try:
        return getattr(settings, 'LOG_LEVEL', 'INFO')
    except ImproperlyConfigured:
        return 'INFO'


LOG_LEVEL = get_log_level()
BASE_DIR = Path(__file__).resolve().parent.parent


# Create directory if it doesn't exist
logs_dir = BASE_DIR / 'data' / 'output'
logs_dir.mkdir(parents=True, exist_ok=True)


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'console': {
            'format': '{asctime} {name} {levelname} {message}',
            'style': '{',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': LOG_LEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'console',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.request': {
            'handlers': ['console'],
            'level': 'ERROR',
            'propagate': False,
        },
        'django.db.backends': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'api': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'core': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'main': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'utils': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
    },
}
