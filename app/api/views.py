# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from django.http import HttpRequest

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
    """ViewSet for managing deidentification jobs."""

    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer
    http_method_names: ClassVar = ['get', 'post', 'delete']

    def create(self, request: HttpRequest, *_args: object, **_kwargs: object) -> Response:
        """Prepare a new job with an uploaded file and column mapping configuration."""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save(status='pending')

        message = 'Job created successfully and is ready to be processed'
        process_url = f'{request.build_absolute_uri()}{job.job_id}/process/'
        curl_url = f'curl -X POST {process_url}'

        # Create clean response with only essential information
        response_data = {
            'message': message,
            'job_id': str(job.job_id),
            'curl_url': curl_url,
        }

        return Response(response_data, status=status.HTTP_201_CREATED)

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
        """Change the job status to 'processing' and initiate the asynchronous deidentification process."""
        job = self.get_object()

        # Check if the input file exist
        if not job.input_file or not Path(job.input_file.path).exists():
            return Response({'error': 'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # Reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = ''
        job.save()

        try:
            # Start the anonymizing process
            process_deidentification(job.job_id)

            return Response({'message': 'Job processing started successfully'}, status=status.HTTP_200_OK)
        except (OSError, RuntimeError, ValueError) as e:
            job.status = 'failed'
            job.save()

            return Response(
                {'error': 'Failed to start job processing', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @extend_schema(
        methods=['get'],
        responses={200: OpenApiResponse(JobStatusSerializer())},
    )
    @action(detail=True, methods=['get'])
    def check_status(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Return the current status, progress percentage, and any error messages for the specified job."""
        job = self.get_object()
        serializer = JobStatusSerializer(job)

        return Response(
            serializer.data,
            status=status.HTTP_200_OK,
        )
