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
    from rest_framework.request import Request

from django.conf import settings
from drf_spectacular.utils import OpenApiResponse, extend_schema, extend_schema_view, inline_serializer
from rest_framework import serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.models import DeidentificationJob
from api.serializers import DeidentificationJobListSerializer, DeidentificationJobSerializer
from core.utils.logger import setup_logging


class ApiTags:
    """API tag constants for Swagger/OpenAPI documentation grouping.

    These tags are used to categorize endpoints in the API documentation
    for better organization and discoverability.
    """

    API = 'API'
    JOBS = 'Jobs'
    PROCESSING = 'Processing'
    CLEANUP = 'Cleanup'


@extend_schema(tags=[ApiTags.API])
class APIRootView(APIView):
    """Custom API root view that includes documentation links."""

    def get(self, request: Request, fmt: str | None = None) -> Response:
        """Return links to all available endpoints."""
        data = {
            'v1/jobs': reverse('jobs-list', request=request, format=fmt),
        }
        if settings.DEBUG:
            data['v1/docs'] = reverse('swagger-ui', request=request, format=fmt)
            data['v1/schema'] = reverse('schema', request=request, format=fmt)

        return Response(data)


@extend_schema(tags=[ApiTags.JOBS])
@extend_schema_view(process=extend_schema(tags=[ApiTags.PROCESSING]))
class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """ViewSet for managing deidentification jobs."""

    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer
    http_method_names: ClassVar = ['get', 'post', 'delete']

    def get_serializer_class(self) -> DeidentificationJobSerializer:
        """Return the appropriate serializer based on the action."""
        if self.action == 'list':
            return DeidentificationJobListSerializer

        return DeidentificationJobSerializer

    @extend_schema(
        responses={
            201: inline_serializer(
                name='JobCreatedResponse',
                fields={
                    'message': serializers.CharField(default='Job created successfully and is ready to be processed'),
                    'job_id': serializers.CharField(),
                    'process_url': serializers.CharField(),
                },
            ),
        },
    )
    def create(self, request: HttpRequest, *_args: object, **_kwargs: object) -> Response:
        """Prepare a new job with an uploaded file and column mapping configuration."""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job = serializer.save(status='pending')
        message = 'Job created successfully and is ready to be processed'
        url = f'{request.build_absolute_uri()}{job.job_id}/process/'
        process_url = f'curl -X POST {url}'

        response_data = {
            'message': message,
            'job_id': str(job.job_id),
            'process_url': process_url,
        }
        return Response(response_data, status=status.HTTP_201_CREATED)

    @extend_schema(
        methods=['post'],
        responses={
            200: OpenApiResponse(description='Job processing started successfully'),
            400: OpenApiResponse(description='Input file is missing'),
            500: OpenApiResponse(description='Failed to start job processing'),
        },
    )
    @extend_schema(methods=['get'], exclude=True)
    @action(detail=True, methods=['get', 'post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Start the deidentification process for a job."""
        job = self.get_object()

        # GET shows information about how to use this endpoint
        if request.method == 'GET':
            return Response({
                'message': 'Use POST to start processing this job',
                'job_id': str(job.job_id),
                'current_status': job.status,
                'method': 'POST',
                'endpoint': request.build_absolute_uri(),
            }, status=status.HTTP_200_OK)

        if not job.input_file or not Path(job.input_file.path).exists():
            return Response({'error': 'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # Reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = ''
        job.save()







        try:
            # Start the anonymizing process
            job_obj, config, _output_filename = _setup_deidentification_job(job.job_id)
            _execute_deidentification(config, job_obj)

            return Response({'message': 'Job processing ended successfully'}, status=status.HTTP_200_OK)
        except (OSError, RuntimeError, ValueError) as e:
            logger = setup_logging()
            logger.exception('Error starting job %s', job.job_id)
            try:
                job_obj = DeidentificationJob.objects.get(pk=job.job_id)
                job_obj.status = 'failed'
                job_obj.error_message = str(e)
                job_obj.save()
            except (OSError, RuntimeError):
                logger.exception('Failed to update job status')

            return Response(
                {'error': 'Failed to start job processing', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
