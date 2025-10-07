# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
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
)
from api.utils import (
    _execute_deidentification,
    _setup_deidentification_job,
)
from core.utils.logger import setup_logging
from core.utils.progress_tracker import tracker


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

        job = serializer.save(status='pending')

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
                    if job.status == 'processing':
                        # During processing, get live updates from tracker
                        progress_info = tracker.get_progress()
                        current_percentage = progress_info['percentage']
                        current_stage = progress_info['stage']
                    else:
                        # Use database values for completed/failed jobs
                        current_percentage = 100 if job.status == 'completed' else 0
                        current_stage = job.status

                    # Only send update if something changed
                    if current_percentage != previous_percentage or current_stage != previous_stage:
                        data = {
                            'job_id': str(job.job_id),
                            'status': job.status,
                            'progress': current_percentage,
                            'stage': current_stage,
                            'error_message': job.error_message,
                        }
                        yield f'data: {json.dumps(data)}\n\n'
                        previous_percentage = current_percentage
                        previous_stage = current_stage

                    # Stop streaming if job is done
                    if job.status in ['completed', 'failed']:
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

