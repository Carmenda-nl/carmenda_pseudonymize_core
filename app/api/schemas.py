# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""OpenAPI schema definitions for the API views."""

from drf_spectacular.utils import extend_schema
from rest_framework import serializers

from api.serializers import DeidentificationJobSerializer


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

    message = serializers.CharField(default='Job processing started in background')
    job_id = serializers.CharField()
    status = serializers.CharField(default='processing')


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


class JobCancellationResponseSerializer(serializers.Serializer):
    """Response serializer for job cancellation request."""

    message = serializers.CharField(default='Cancellation requested')
    job_id = serializers.CharField()


class JobCancellationStatusSerializer(serializers.Serializer):
    """Response serializer for job cancellation status (GET)."""

    job_id = serializers.CharField()
    status = serializers.CharField()
    progress = serializers.IntegerField()
    stage = serializers.CharField()
    error_message = serializers.CharField()


class JobNotRunningResponseSerializer(serializers.Serializer):
    """Response serializer when trying to cancel a job that's not running."""

    message = serializers.CharField(default='Job not running')
    status = serializers.CharField()


class JobCancellationErrorSerializer(serializers.Serializer):
    """Response serializer for job cancellation errors."""

    error = serializers.CharField()


class JobProcessErrorSerializer(serializers.Serializer):
    """Response serializer for job processing validation errors."""

    error = serializers.CharField()
    message = serializers.CharField()


# Schema definitions for endpoints
API_ROOT_SCHEMA = extend_schema(
    responses={
        200: APIRootResponseSerializer,
    },
)

CREATE_JOB_SCHEMA = extend_schema(
    responses={
        201: DeidentificationJobSerializer,
    },
)

PROCESS_JOB_POST_SCHEMA = extend_schema(
    methods=['post'],
    responses={
        202: JobProcessingResponseSerializer,
        400: JobProcessErrorSerializer,
        500: JobProcessingErrorResponseSerializer,
    },
)

PROCESS_JOB_GET_SCHEMA = extend_schema(
    methods=['get'],
    responses={
        200: JobStatusResponseSerializer,
    },
)

CANCEL_JOB_POST_SCHEMA = extend_schema(
    methods=['post'],
    responses={
        200: JobNotRunningResponseSerializer,
        202: JobCancellationResponseSerializer,
        500: JobCancellationErrorSerializer,
    },
)

CANCEL_JOB_GET_SCHEMA = extend_schema(
    methods=['get'],
    responses={
        200: JobCancellationStatusSerializer,
    },
)
