# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

if TYPE_CHECKING:
    from rest_framework.request import Request

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from drf_spectacular.utils import extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.models import DeidentificationJob
from api.schemas import CREATE_JOB_SCHEMA, PROCESS_JOB_GET_SCHEMA, PROCESS_JOB_POST_SCHEMA
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


def run_processing(
    job_id: str, input_file: str, input_cols: dict, output_cols: dict, datakey: str | None = None,
) -> None:
    """Run processing in background thread."""
    try:
        current_job = DeidentificationJob.objects.get(pk=job_id)
        channel_layer = get_channel_layer()

        def update_job_progress(percentage: int, stage: str) -> None:
            """Send progress updates via WebSocket."""
            async_to_sync(channel_layer.group_send)(
                f'job_progress_{job_id}',
                {
                    'type': 'job_progress',
                    'percentage': percentage,
                    'stage': stage,
                    'status': 'processing',
                },
            )

        tracker.reset()

        # Set up a callback to monitor progress from tracker
        stop_monitoring = threading.Event()
        completion_percentage = 100

        def monitor_progress() -> None:
            """Monitor tracker progress and send updates via WebSocket."""
            last_percentage = -1

            while not stop_monitoring.is_set():
                progress_info = tracker.get_progress()
                current_percentage = progress_info['percentage']
                current_stage = progress_info['stage'] or 'Processing'

                if abs(current_percentage - last_percentage) >= 1 or current_percentage == completion_percentage:
                    update_job_progress(current_percentage, current_stage)
                    last_percentage = current_percentage

                stop_monitoring.wait(0.1)

        # Start progress monitoring in separate thread
        monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
        monitor_thread.start()

        processor_pipeline = process_data(
            input_file=input_file,
            input_cols=input_cols,
            output_cols=output_cols,
            datakey=datakey,
        )

        stop_monitoring.set()
        monitor_thread.join(timeout=1.0)

        if processor_pipeline:
            processed_data = json.loads(processor_pipeline)
            current_job.processed_preview = processed_data
            current_job.save()

        files_to_zip, output_filename = collect_output_files(current_job, input_file)
        create_zipfile(current_job, files_to_zip, output_filename)

        current_job.status = 'completed'
        current_job.save()

        async_to_sync(channel_layer.group_send)(
            f'job_progress_{job_id}',
            {
                'type': 'job_progress',
                'percentage': 100,
                'stage': 'Completed',
                'status': 'completed',
            },
        )

    except Exception as error:
        logger.exception('[JOB %s] Processing failed', job_id)
        error_job = DeidentificationJob.objects.get(pk=job_id)
        error_job.error_message = f'Job error: {error}'
        error_job.status = 'failed'
        error_job.save()

        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            f'job_progress_{job_id}',
            {
                'type': 'job_progress',
                'percentage': 0,
                'stage': f'Error: {error}',
                'status': 'failed',
            },
        )


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

    @CREATE_JOB_SCHEMA
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

    @PROCESS_JOB_POST_SCHEMA
    @PROCESS_JOB_GET_SCHEMA
    @action(detail=True, methods=['get', 'post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Start the deidentification process for a job."""
        job = self.get_object()

        # Get current progress from tracker if processing
        if request.method == 'GET':
            if job.status == 'processing':
                progress_info = tracker.get_progress()
                current_progress = progress_info['percentage']
                current_stage = progress_info['stage'] or job.status
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
        # Reset the status and error message on re-runs
        job.status = 'processing'
        job.error_message = ''
        job.save()

        try:
            input_cols = job.input_cols
            output_cols = match_output_cols(input_cols)
            input_file = Path(job.input_file.name).name
            datakey = Path(job.datakey.name).name if job.datakey else None

            # Start processing in background thread
            processing_thread = threading.Thread(
                target=run_processing,
                args=(str(job.job_id), input_file, input_cols, output_cols, datakey),
                daemon=True,
            )
            processing_thread.start()

            return Response(
                {
                    'message': 'Job processing started in background',
                    'job_id': str(job.job_id),
                    'status': 'processing',
                },
                status=status.HTTP_202_ACCEPTED,
            )
        except Exception as error:
            job.error_message = f'Job error: {error}'
            job.status = 'failed'
            job.save()

            logger.exception('Job %s failed to start', job.job_id)

            return Response(
                {'error': 'Job processing failed', 'details': str(error)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
