# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, NamedTuple

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
from core.utils.logger import setup_logging

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


class DeidentificationConfig(NamedTuple):
    """Configuration for deidentification processing."""

    input_file: str
    input_cols: dict
    output_cols: dict
    datakey: dict


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
        print('job:', job.job_id)
        return Response({'message': 'Job processing ended successfully'}, status=status.HTTP_200_OK)

        # # GET shows information about how to use this endpoint
        # if request.method == 'GET':
        #     return Response({
        #         'message': 'Use POST to start processing this job',
        #         'job_id': str(job.job_id),
        #         'current_status': job.status,
        #         'method': 'POST',
        #         'endpoint': request.build_absolute_uri(),
        #     }, status=status.HTTP_200_OK)

        # if not job.input_file or not Path(job.input_file.path).exists():
        #     return Response({'error': 'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # # Reset the status and error message on re-runs
        # job.status = 'processing'
        # job.error_message = ''
        # job.save()





        # try:
        #     # Start the anonymizing process
        #     job = DeidentificationJob.objects.get(pk=job.job_id)
        #     job.status = 'processing'
        #     job.save()

        #     input_cols = job.input_cols
        #     output_cols = match_output_cols(input_cols)

        #     input_file = Path(job.input_file.name).name
        #     datakey = Path(job.key_file.name).name if job.key_file else None

        #     config = DeidentificationConfig(
        #         input_file=input_file,
        #         input_cols=input_cols,
        #         output_cols=output_cols,
        #         datakey=datakey,
        #     )

        #     # Execute a deidentification job
        #     from core.processor import process_data  # noqa: PLC0415 (Initialize core only when needed)

        #     # Call the core's deidentification process (data processor)
        #     processed_rows_json = process_data(
        #         input_file=config.input_file,
        #         input_cols=config.input_cols,
        #         output_cols=config.output_cols,
        #         datakey=config.datakey,
        #     )

        #     if processed_rows_json:
        #         processed_data = json.loads(processed_rows_json)
        #         job.processed_preview = processed_data
        #         job.save()



        #     # Collect output files and update job model
        #     files_to_zip, output_filename = collect_output_files(job, config.input_file)
        #     create_zipfile(job, files_to_zip, output_filename)

        #     # If no errors, update job to 'completed'
        #     job.status = 'completed'
        #     job.save()

        #     return Response({'message': 'Job processing ended successfully'}, status=status.HTTP_200_OK)

        # except (OSError, RuntimeError, ValueError) as e:
        #     # Update job status
        #     logger.exception('Error processing job %s', job.pk)
        #     try:
        #         job.status = 'failed'
        #         job.error_message = f'Job error: {e!s}'
        #         job.save()
        #     except (OSError, RuntimeError):
        #         logger.exception('Failed to update job status for job %s', job.id)

        #     return Response(
        #         {'error': 'Failed to start job processing', 'details': str(e)},
        #         status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     )
