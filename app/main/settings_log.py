# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Django logging configuration."""

from django.conf import settings

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'console': {
            'format': '{asctime} {name} {levelname} {message}',
            'style': '{',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'file': {
            'level': settings.LOG_LEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': settings.BASE_DIR / 'data' / 'output' / 'backend-api.log',
            'formatter': 'verbose',
        },
        'console': {
            'level': settings.LOG_LEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'console',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': settings.LOG_LEVEL,
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.request': {
            'handlers': ['console', 'file'],
            'level': 'ERROR',
            'propagate': False,
        },
        'django.db.backends': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'api': {
            'handlers': ['console'],
            'level': settings.LOG_LEVEL,
            'propagate': False,
        },
        'core': {
            'handlers': ['console'],
            'level': settings.LOG_LEVEL,
            'propagate': False,
        },
        'main': {
            'handlers': ['console'],
            'level': settings.LOG_LEVEL,
            'propagate': False,
        },
        'utils': {
            'handlers': ['console'],
            'level': settings.LOG_LEVEL,
            'propagate': False,
        },
    },
}

# Create directory if it doesn't exist
logs_dir = settings.BASE_DIR / 'data' / 'output'
logs_dir.mkdir(parents=True, exist_ok=True)
