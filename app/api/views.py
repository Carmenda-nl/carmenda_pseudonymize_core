# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from rest_framework.request import Request

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from drf_spectacular.utils import OpenApiResponse, extend_schema, extend_schema_view, inline_serializer
from rest_framework import serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.models import DeidentificationJob
from api.serializers import DeidentificationJobListSerializer, DeidentificationJobSerializer
from api.utils import collect_output_files, create_zipfile, match_output_cols
from core.processor import process_data
from core.utils.logger import setup_logging
from core.utils.progress_tracker import tracker

logger = setup_logging()


def favicon_view(_request: HttpRequest) -> HttpResponse:
    """Disabe favicon requests by returning an empty response."""
    return HttpResponse(status=204)


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
            200: inline_serializer(
                name='JobProcessingResponse',
                fields={'message': serializers.CharField(default='Job processing finished successfully')},
            ),
            400: OpenApiResponse(description='Input file is missing'),
            500: inline_serializer(
                name='JobProcessingErrorResponse',
                fields={
                    'error': serializers.CharField(default='Job processing failed'),
                    'details': serializers.CharField(),
                },
            ),
        },
    )
    @extend_schema(
        methods=['get'],
        responses={
            200: inline_serializer(
                name='JobStatusResponse',
                fields={
                    'job_id': serializers.CharField(),
                    'endpoint': serializers.CharField(),
                    'current_status': serializers.CharField(),
                    'progress': serializers.IntegerField(),
                    'stage': serializers.CharField(),
                    'error_message': serializers.CharField(),
                },
            ),
        },
    )
    @action(detail=True, methods=['get', 'post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Start the deidentification process for a job."""
        job = self.get_object()

        # Get current progress from tracker if processing
        if request.method == 'GET':
            if job.status == 'processing':
                progress = tracker.get_progress()
                current_progress = progress['percentage']
                current_stage = progress['stage']
            else:
                current_progress = 100 if job.status == 'completed' else 0
                current_stage = job.status

            return Response(
                {
                    'job_id': str(job.job_id),
                    'endpoint': request.build_absolute_uri(),
                    'current_status': job.status,
                    'progress': current_progress,
                    'stage': current_stage,
                    'error_message': job.error_message,
                },
                status=status.HTTP_200_OK,
            )

        if not job.input_file or not Path(job.input_file.path).exists():
            return Response({'error': 'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # Reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = ''
        job.save()

        try:
            job = DeidentificationJob.objects.get(pk=job.job_id)
            job.status = 'processing'
            job.save()

            input_cols = job.input_cols
            output_cols = match_output_cols(input_cols)

            input_file = Path(job.input_file.name).name
            datakey = Path(job.datakey.name).name if job.datakey else None

            # Execute a deidentification job
            processor_pipeline = process_data(
                input_file=input_file,
                input_cols=input_cols,
                output_cols=output_cols,
                datakey=datakey,
            )

            if processor_pipeline:
                processed_data = json.loads(processor_pipeline)
                job.processed_preview = processed_data
                job.save()

            files_to_zip, output_filename = collect_output_files(job, input_file)
            create_zipfile(job, files_to_zip, output_filename)

            job.status = 'completed'
            job.save()

            return Response({'message': 'Job processing finished successfully'}, status=status.HTTP_200_OK)

        except (OSError, RuntimeError, ValueError) as error:
            job.error_message = f'Job error: {error}'
            job.status = 'failed'
            job.save()

            return Response(
                {'error': 'Job processing failed', 'details': str(error)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
