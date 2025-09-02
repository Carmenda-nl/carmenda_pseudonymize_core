# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from django.http import HttpRequest

from django.conf import settings
from drf_spectacular.utils import OpenApiResponse, extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from api.manager import process_deidentification
from api.models import DeidentificationJob
from api.serializers import DeidentificationJobSerializer, JobStatusSerializer, ProcessJobSerializer


class ApiTags:
    """API tag constants for Swagger/OpenAPI documentation grouping.

    These tags are used to categorize endpoints in the API documentation
    for better organization and discoverability.
    """

    JOBS = 'Jobs'
    PROCESSING = 'Processing'
    CLEANUP = 'Cleanup'


@extend_schema(tags=[ApiTags.JOBS])
@extend_schema_view(
    process=extend_schema(tags=[ApiTags.PROCESSING]),
    check_status=extend_schema(tags=[ApiTags.PROCESSING]),
)
class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """ViewSet for managing deidentification jobs.

    This ViewSet provides endpoints for creating, retrieving,
    and deleting deidentification jobs.
    """

    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer
    http_method_names: ClassVar = ['get', 'post', 'delete']

    def create(self, request: HttpRequest, *_args: object, **_kwargs: object) -> Response:
        """Prepare a new job with an uploaded file and column mapping configuration.

        The job is initialized with 'pending' status and awaits explicit processing.

        Required parameters:
            - input_file: The healthcare data file to be processed
            - input_cols: Mapping configuration specifying which columns to process

        Returns:
            HTTP_201_CREATED: Job created successfully
            HTTP_400_BAD_REQUEST: Validation errors in the request data

        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save(status='pending')

        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    @extend_schema(
        methods=['post'],
        request=ProcessJobSerializer,
        responses={
            200: OpenApiResponse(description='Job processing started successfully'),
            400: OpenApiResponse(description='Job cannot be processed or input file is missing'),
            500: OpenApiResponse(description='Failed to start job processing'),
        },
    )
    @action(detail=True, methods=['post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Change the job status to 'processing' and initiate the asynchronous deidentification process.

        Validates that the input file exists before starting.

        Args:
            request (HttpRequest): The HTTP request
            pk (str | None): The job_id of the job to process

        Returns:
            Response: HTTP_200_OK if job processing started successfully,
                      HTTP_400_BAD_REQUEST if job cannot be processed or input file is missing,
                      HTTP_500_INTERNAL_SERVER_ERROR if failed to start job processing

        """
        job = self.get_object()

        # Check if the input file exist
        if not job.input_file or not Path(job.input_file.path).exists():
            return Response({'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # Reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = ''
        job.save()

        try:
            # Start the anonymizing process
            process_deidentification(job.job_id)

            return Response({'Job processing started successfully'}, status=status.HTTP_200_OK)
        except (OSError, RuntimeError, ValueError) as e:
            job.status = 'failed'
            job.save()

            return Response({'Failed to start job processing': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(
        methods=['get'],
        responses={200: OpenApiResponse(JobStatusSerializer())},
    )
    @action(detail=True, methods=['get'])
    def check_status(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Return the current status, progress percentage, and any error messages for the specified job.

        Args:
            request (HttpRequest): The HTTP request
            pk (str | None): The job_id of the job to check

        Returns:
            Response: HTTP_200_OK with job status details in response body

        """
        job = self.get_object()
        serializer = JobStatusSerializer(job)

        return Response(
            serializer.data,
            status=status.HTTP_200_OK,
        )


@extend_schema(tags=[ApiTags.CLEANUP])
class ResetJobsViewSet(viewsets.ViewSet):
    """Reset the system by deleting all job records and associated files.

    Warnings:
        - This endpoint permanently deletes all job data
        - Use only for development, testing, or when a complete system reset is needed

    Endpoints:
        - DELETE /reset/select_all/: Delete all jobs and associated files

    """

    serializer_class = DeidentificationJobSerializer

    @extend_schema(
        methods=['delete'],
        responses={
            200: OpenApiResponse(description='Jobs successfully deleted'),
            500: OpenApiResponse(description='Error deleting jobs'),
        },
    )
    @action(detail=False, methods=['delete'])
    def select_all(self, request: HttpRequest) -> Response:
        """Delete all jobs and associated files from the system.

        This operation:
        1. Removes all job records from the database
        2. Deletes all input and output files from storage
        3. Re-creates empty directories to maintain structure

        Warning: This action is irreversible and will permanently delete all job data.

        Returns:
            HTTP_200_OK: All jobs successfully deleted, with count of deleted jobs
            HTTP_500_INTERNAL_SERVER_ERROR: Error occurred during deletion

        """
        try:
            # Get all jobs to track stats before deletion
            total_jobs = DeidentificationJob.objects.count()

            # Get paths to input and output directories
            input_dir = Path(settings.MEDIA_ROOT) / 'input'
            output_dir = Path(settings.MEDIA_ROOT) / 'output'

            # Ensure logger is not reserved before removal
            logging.shutdown()

            # Remove all job-related files
            if input_dir.exists():
                shutil.rmtree(input_dir)
            if output_dir.exists():
                shutil.rmtree(output_dir)

            # Delete all job records
            DeidentificationJob.objects.all().delete()

            # Recreate empty directories to maintain structure
            input_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)

            return Response(
                f'All {total_jobs} jobs have been removed successfully.',
                status=status.HTTP_200_OK,
            )

        except (OSError, RuntimeError, PermissionError) as e:
            return Response(
                {'An error occurred while resetting jobs.\n': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
