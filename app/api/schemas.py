# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""OpenAPI schema definitions for the API views."""

from drf_spectacular.utils import extend_schema
from rest_framework import serializers


class APIRootResponseSerializer(serializers.Serializer):
    """Response serializer for API root view."""

    v1_jobs = serializers.URLField(source='v1/jobs', help_text='URL to the jobs list endpoint')
    v1_docs = serializers.URLField(
        source='v1/docs',
        required=False,
        help_text='URL to the API documentation (only available in debug mode)',
    )
    v1_schema = serializers.URLField(
        source='v1/schema',
        required=False,
        help_text='URL to the API schema (only available in debug mode)',
    )


class JobCreatedResponseSerializer(serializers.Serializer):
    """Response serializer for job creation."""

    message = serializers.CharField(default='Job created successfully and is ready to be processed')
    job_id = serializers.CharField()
    process_url = serializers.CharField()


class JobProcessingResponseSerializer(serializers.Serializer):
    """Response serializer for successful job processing."""

    message = serializers.CharField(default='Job processing finished successfully')


class JobProcessingErrorResponseSerializer(serializers.Serializer):
    """Response serializer for job processing errors."""

    error = serializers.CharField(default='Job processing failed')
    details = serializers.CharField()


class JobStatusResponseSerializer(serializers.Serializer):
    """Response serializer for job status."""

    job_id = serializers.CharField()
    endpoint = serializers.CharField()
    current_status = serializers.CharField()
    progress = serializers.IntegerField()
    stage = serializers.CharField()
    error_message = serializers.CharField()


# Schema definitions for endpoints
API_ROOT_SCHEMA = extend_schema(
    responses={
        200: APIRootResponseSerializer,
    },
)

CREATE_JOB_SCHEMA = extend_schema(
    responses={
        201: JobCreatedResponseSerializer,
    },
)

PROCESS_JOB_POST_SCHEMA = extend_schema(
    methods=['post'],
    responses={
        200: JobProcessingResponseSerializer,
        500: JobProcessingErrorResponseSerializer,
    },
)

PROCESS_JOB_GET_SCHEMA = extend_schema(
    methods=['get'],
    responses={
        200: JobStatusResponseSerializer,
    },
)
