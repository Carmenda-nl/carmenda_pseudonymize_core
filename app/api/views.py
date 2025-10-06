# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Generator

    from django.http import HttpRequest
    from rest_framework.request import Request

from django.conf import settings
from django.http import StreamingHttpResponse
from drf_spectacular.utils import OpenApiResponse, extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.models import DeidentificationJob
from api.serializers import (
    DeidentificationJobListSerializer,
    DeidentificationJobSerializer,
    JobStatusSerializer,
    ProcessJobSerializer,
)
from utils.file_handling import check_file
from utils.progress_tracker import tracker


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
)
class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """ViewSet for managing deidentification jobs."""

    queryset = DeidentificationJob.objects.all()
    serializer_class = DeidentificationJobSerializer
    http_method_names: ClassVar = ['get', 'post', 'delete']

    def get_serializer_class(self):  # type: ignore[override]  # noqa: ANN201
        """Return the appropriate serializer based on the action."""
        if self.action == 'list':
            return DeidentificationJobListSerializer
        return DeidentificationJobSerializer

    def create(self, request: HttpRequest, *_args: object, **_kwargs: object) -> Response:
        """Prepare a new job with an uploaded file and column mapping configuration."""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        input_file = serializer.validated_data.get('input_file')
        input_cols = serializer.validated_data.get('input_cols')

        if input_file:
            try:
                # Handle both InMemoryUploadedFile and TemporaryUploadedFile
                if hasattr(input_file, 'temporary_file_path'):
                    # File is stored on disk
                    file_path = input_file.temporary_file_path()
                    temp_file_created = False
                else:
                    # File is in memory, save temporarily
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                        for chunk in input_file.chunks():
                            temp_file.write(chunk)
                        file_path = temp_file.name
                    temp_file_created = True

                # Check file encoding, line endings, and separator
                encoding, _line_ending, separator = check_file(file_path)

                # Parse input columns to validate they exist in the file
                input_cols_dict = dict(column.strip().split('=') for column in input_cols.split(','))

                # Read header line to check column existence
                with Path(file_path).open(encoding=encoding) as f:
                    header = f.readline().strip()
                    columns = [col.strip() for col in header.split(separator)]

                # Clean up temporary file if we created one
                if temp_file_created:
                    Path(file_path).unlink()

                # Validate that specified columns exist in the file
                for col_value in input_cols_dict.values():
                    if col_value not in columns:
                        available_cols = ', '.join(columns)
                        error_msg = (
                            f'Column "{col_value}" specified in input_cols not found in file. '
                            f'Available columns: {available_cols}'
                        )
                        return Response(
                            {'error': error_msg},
                            status=status.HTTP_400_BAD_REQUEST,
                        )

            except (OSError, UnicodeDecodeError, ValueError) as e:
                return Response(
                    {'error': f'Invalid file format: {e!s}'},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        job = serializer.save(progress_status='pending')

        message = 'Job created successfully and is ready to be processed'
        url = f'{request.build_absolute_uri()}{job.job_id}/process/'
        process_url = f'curl -X POST {url}'

        # Create clean response with only essential information
        response_data = {
            'message': message,
            'job_id': str(job.job_id),
            'process_url': process_url,
        }

        return Response(response_data, status=status.HTTP_201_CREATED)

    @extend_schema(
        methods=['post'],
        request=ProcessJobSerializer,
        responses={
            200: OpenApiResponse(description='Job processing ended successfully'),
            400: OpenApiResponse(description='Job cannot be processed or input file is missing'),
            500: OpenApiResponse(description='Failed to start job processing'),
        },
    )
    @extend_schema(
        methods=['get'],
        responses={
            200: OpenApiResponse(JobStatusSerializer(), description='Job status, progress, and error information'),
        },
    )
    @action(detail=True, methods=['get', 'post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """GET: Check job status and progress. POST: Start the deidentification process."""
        job = self.get_object()

        # Handle GET request - return job status and progress
        if request.method == 'GET':
            serializer = JobStatusSerializer(job)
            response_data = {
                'job_id': str(job.job_id),
                **serializer.data,
            }
            return Response(response_data, status=status.HTTP_200_OK)

        # Handle POST request - start processing
        # Check if the input file exist
        if not job.input_file or not Path(job.input_file.path).exists():
            return Response({'error': 'Input file is missing'}, status=status.HTTP_400_BAD_REQUEST)

        # Reset the status and error message on re-runs
        job.progress_status = 'processing'
        job.error_message = ''
        job.save()

        try:
            # Start the anonymizing process
            process_deidentification(job.job_id)

            return Response({'message': 'Job processing ended successfully'}, status=status.HTTP_200_OK)
        except (OSError, RuntimeError, ValueError) as e:
            job.progress_status = 'failed'
            job.save()

            return Response(
                {'error': 'Failed to start job processing', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @extend_schema(
        methods=['get'],
        responses={
            200: OpenApiResponse(description='Server-Sent Events stream for real-time progress updates'),
        },
        description='Stream real-time progress updates for a job using Server-Sent Events (SSE)',
    )
    @action(detail=True, methods=['get'], url_path='progress-stream')
    def progress_stream(self, request: HttpRequest, pk: str | None = None) -> StreamingHttpResponse:
        """Stream real-time progress updates using Server-Sent Events."""
        job = self.get_object()

        def event_stream() -> Generator[str, None, None]:
            """Generate SSE events with progress updates."""
            previous_percentage = -1
            previous_stage = None

            while True:
                try:
                    # Refresh job from database
                    job.refresh_from_db()

                    # Get current progress
                    if job.progress_status == 'processing':
                        # During processing, get live updates from tracker
                        progress_info = tracker.get_progress()
                        current_percentage = progress_info['percentage']
                        current_stage = progress_info['stage']
                    else:
                        # Use database values for completed/failed jobs
                        current_percentage = job.progress_percentage
                        current_stage = job.current_stage or job.progress_status

                    # Only send update if something changed
                    if current_percentage != previous_percentage or current_stage != previous_stage:
                        data = {
                            'job_id': str(job.job_id),
                            'status': job.progress_status,
                            'progress': current_percentage,
                            'stage': current_stage,
                            'error_message': job.error_message,
                        }
                        yield f'data: {json.dumps(data)}\n\n'
                        previous_percentage = current_percentage
                        previous_stage = current_stage

                    # Stop streaming if job is done
                    if job.progress_status in ['completed', 'failed']:
                        break

                    # Wait before next update
                    time.sleep(0.5)

                except DeidentificationJob.DoesNotExist:
                    yield f'data: {json.dumps({"error": "Job not found"})}\n\n'
                    break
                except Exception as e:  # noqa: BLE001
                    yield f'data: {json.dumps({"error": str(e)})}\n\n'
                    break

        response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'
        return response


def process_deidentification(job_id: str) -> None:
    """Process the deidentification job synchronously.

    This function executes the deidentification process and waits for completion.
    """
    from api.manager import (
        _execute_deidentification,
        _setup_deidentification_job,
    )

    try:
        job, config, _output_filename = _setup_deidentification_job(job_id)
        _execute_deidentification(config, job)

    except (OSError, RuntimeError, ValueError) as e:
        from utils.logger import setup_logging

        logger = setup_logging()
        logger.exception('Error starting job %s', job_id)
        try:
            job = DeidentificationJob.objects.get(pk=job_id)
            job.progress_status = 'failed'
            job.error_message = str(e)
            job.save()
        except (OSError, RuntimeError):
            logger.exception('Failed to update job status')

