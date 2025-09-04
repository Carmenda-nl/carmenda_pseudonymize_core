# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Settings for the Django project."""

import sys
from pathlib import Path

import environ
from django.core.management.utils import get_random_secret_key

env = environ.FileAwareEnv(
    # Set casting, default values for env's
    DEBUG=(bool, False),
    LOG_LEVEL=(str, 'INFO'),
)

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Take environment variables from .env file
environ.Env.read_env(BASE_DIR / '.env')

DEBUG = env('DEBUG')
SECRET_KEY = env('SECRET_KEY', default=get_random_secret_key())

# Logging configuration
LOG_LEVEL = env('LOG_LEVEL')

ALLOWED_HOSTS = ['localhost', '127.0.0.1']
CSRF_TRUSTED_ORIGINS = env.list('CSRF_TRUSTED_ORIGINS', default=['http://127.0.0.1'])


INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'drf_spectacular',
    'corsheaders',
    'api',
    'main',
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

if getattr(sys, 'frozen', False):
    # Add additional middleware for pyinstaller envs (fixes: MEDIA_URL in prod)
    MIDDLEWARE.append('main.middleware.ServeMediaFilesMiddleware')


# CORS settings for app communication
CORS_ALLOWED_ORIGINS = [
    'http://localhost:3000',
    'http://localhost:8000',
]

CORS_ALLOW_METHODS = (
    'DELETE',
    'GET',
    'POST',
    'PUT',
)


ROOT_URLCONF = 'main.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'main.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
        'OPTIONS': {
            'timeout': 30,
        },
        'CONN_MAX_AGE': 600,  # Keep connections alive for 10 minutes
        'CONN_HEALTH_CHECKS': True,
    },
}


# Django REST framework
# https://www.django-rest-framework.org

if DEBUG:
    REST_FRAMEWORK = {
        'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
    }

    SPECTACULAR_SETTINGS = {
        'TITLE': 'Deidentification API',
        'DESCRIPTION': 'API for file-based deidentification',
        'VERSION': '1.0.0',
        'SERVE_INCLUDE_SCHEMA': False,
        'TAGS': [
            {'name': 'Jobs', 'description': 'general job management endpoints'},
            {'name': 'Processing', 'description': 'endpoints related to job deidentification processing'},
            {'name': 'Cleanup', 'description': 'removes all the created jobs and leftover files'},
        ],
    }
else:
    REST_FRAMEWORK = {'DEFAULT_RENDERER_CLASSES': ('rest_framework.renderers.JSONRenderer',)}


# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

# Media files for upload
MEDIA_URL = '/data/'
MEDIA_ROOT = BASE_DIR / 'data'

# Static files
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'static'


# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
