# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""API views for deidentification jobs logic."""

from __future__ import annotations

import json
import logging
import shutil
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from drf_spectacular.utils import extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView

from api.models import DeidentificationJob
from api.schemas import (
    API_ROOT_SCHEMA,
    CANCEL_JOB_GET_SCHEMA,
    CANCEL_JOB_POST_SCHEMA,
    CREATE_JOB_SCHEMA,
    PROCESS_JOB_GET_SCHEMA,
    PROCESS_JOB_POST_SCHEMA,
)
from api.serializers import DeidentificationJobListSerializer, DeidentificationJobSerializer
from api.utils import collect_output_files, create_zipfile, match_output_cols, setup_job_logging
from core.processor import process_data
from core.utils.logger import setup_logging
from core.utils.progress_control import JobCancelledError, job_control
from core.utils.progress_tracker import tracker

if TYPE_CHECKING:
    from rest_framework.request import Request

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
    CANCEL = 'Cancel'


@extend_schema(tags=[ApiTags.API])
@API_ROOT_SCHEMA
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


def _send_job_progress(job_id: str, percentage: int, stage: str, job_status: str = 'processing') -> None:
    """Send job progress update via WebSocket."""
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        f'job_progress_{job_id}',
        {
            'type': 'job_progress',
            'percentage': percentage,
            'stage': stage,
            'status': job_status,
        },
    )


def _handle_job_completion(
    job_id: str,
    current_job: DeidentificationJob,
    processor_pipeline: str | None,
    input_file: str,
) -> None:
    """Handle job completion: save preview, create zip, and send completion update."""
    if processor_pipeline:
        processed_data = json.loads(processor_pipeline)
        current_job.processed_preview = processed_data
        current_job.save()

    files_to_zip, output_filename = collect_output_files(current_job, input_file)
    create_zipfile(current_job, files_to_zip, output_filename)

    current_job.status = 'completed'
    current_job.save()

    _send_job_progress(job_id, 100, 'Completed', 'completed')


def _handle_job_cancellation(job_id: str) -> None:
    """Handle job cancellation: update database and send cancellation update."""
    logger.info('[JOB %s] Cancelled by user', job_id)
    cancelled_job = DeidentificationJob.objects.get(pk=job_id)
    cancelled_job.error_message = 'Job cancelled by user'
    cancelled_job.status = 'cancelled'
    cancelled_job.save()

    progress_percentage = tracker.get_progress().get('percentage', 0)
    _send_job_progress(job_id, progress_percentage, 'Cancelled', 'cancelled')


def _handle_job_error(job_id: str, error: Exception) -> None:
    """Handle job processing error: update database and send error update."""
    logger.exception('[JOB %s] Processing failed', job_id)
    error_job = DeidentificationJob.objects.get(pk=job_id)
    error_job.error_message = f'Job error: {error}'
    error_job.status = 'failed'
    error_job.save()

    _send_job_progress(job_id, 0, f'Error: {error}', 'failed')


def run_processing(job_id: str, input_file: str, input_cols: dict, output_cols: dict, datakey: str | None) -> None:
    """Run processing in background thread."""
    current_job = DeidentificationJob.objects.get(pk=job_id)
    job_handler = setup_job_logging(job_id)

    try:
        with job_control.run_job(job_id):
            tracker.reset()

            # Set up progress monitoring
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
                        _send_job_progress(job_id, current_percentage, current_stage)
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

            _handle_job_completion(job_id, current_job, processor_pipeline, input_file)

    except JobCancelledError:
        _handle_job_cancellation(job_id)

    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        _handle_job_error(job_id, error)
    finally:
        deidentify_logger = logging.getLogger('deidentify')
        deidentify_logger.removeHandler(job_handler)
        job_handler.close()

        # Stop and join the monitor thread if it exists
        try:
            if 'stop_monitoring' in locals():
                stop_monitoring.set()
            if 'monitor_thread' in locals():
                monitor_thread.join(timeout=1.0)
        except (AttributeError, RuntimeError) as e:
            logger.debug('Failed to stop monitor thread for %s: %s', job_id, e)

        # Ensure the progress bar is finalized to avoid leftover UI/console output
        try:
            # Do not force-complete the progress when cleaning up after an
            # unexpected cancellation — that would make the UI jump to 100%.
            tracker.finalize_progress(complete='no')
        except (AttributeError, RuntimeError) as e:
            logger.debug('Failed to finalize progress for %s: %s', job_id, e)


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

    def perform_destroy(self, instance: DeidentificationJob) -> None:
        """Delete associated files from storage and remove job directory.

        If the job is currently processing, cancel it first before deleting files.
        """
        # Cancel the job if it's currently processing
        if instance.status == 'processing':
            logger.info('[JOB %s] Cancelling running job before deletion', instance.job_id)
            try:
                job_control.cancel(str(instance.job_id))

                # Update job status
                instance.status = 'cancelled'
                instance.error_message = 'Job cancelled before deletion'
                instance.save()

                # Send WebSocket notification
                channel_layer = get_channel_layer()
                async_to_sync(channel_layer.group_send)(
                    f'job_progress_{instance.job_id}',
                    {
                        'type': 'job_progress',
                        'percentage': tracker.get_progress().get('percentage', 0),
                        'stage': 'Cancelled (deletion)',
                        'status': 'cancelled',
                    },
                )

                # Give the process a moment to handle cancellation
                time.sleep(0.5)

            except (RuntimeError, OSError) as error:
                logger.warning('[JOB %s] Failed to cancel job before deletion: %s', instance.job_id, error)

        # Delete associated files
        files = ['input_file', 'datakey', 'output_file', 'log_file', 'zip_file']

        for file in files:
            file_field = getattr(instance, file, None)

            if file_field and getattr(file_field, 'name', None):
                file_field.delete(save=False)

        media_root = settings.MEDIA_ROOT

        dirs = [
            Path(media_root) / 'input' / str(instance.job_id),
            Path(media_root) / 'output' / str(instance.job_id),
        ]

        for job_dir in dirs:
            if job_dir.exists() and job_dir.is_dir():
                shutil.rmtree(job_dir)

        super().perform_destroy(instance)

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
            filename = Path(job.input_file.name).name
            input_file = f'{job.job_id}/{filename}'

            datakey = None
            if job.datakey:
                datakey_name = Path(job.datakey.name).name
                datakey = f'{job.job_id}/{datakey_name}'

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
        except (OSError, ValueError, KeyError) as error:
            job.error_message = f'Job error: {error}'
            job.status = 'failed'
            job.save()

            logger.exception('Job %s failed to start', job.job_id)

            return Response(
                {'error': 'Job processing failed', 'details': str(error)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    @extend_schema(tags=[ApiTags.CANCEL])
    @CANCEL_JOB_POST_SCHEMA
    @CANCEL_JOB_GET_SCHEMA
    @action(detail=True, methods=['get', 'post'])
    def cancel(self, request: HttpRequest, pk: str | None = None) -> Response:
        """GET: return current job status and last progress percentage.

        POST: request cancellation of a running job (existing behaviour).
        """
        job = self.get_object()

        # Handle GET: return status and last seen progress
        if request.method == 'GET':
            progress_info = tracker.get_progress()
            current_progress = progress_info.get('percentage', 0)
            current_stage = progress_info.get('stage') or job.status

            return Response(
                {
                    'job_id': str(job.job_id),
                    'status': job.status,
                    'progress': current_progress,
                    'stage': current_stage,
                    'error_message': job.error_message,
                },
                status=status.HTTP_200_OK,
            )

        # POST: request cancellation
        if job.status != 'processing':
            return Response({'message': 'Job not running', 'status': job.status}, status=status.HTTP_200_OK)

        try:
            job_control.cancel(str(job.job_id))
            job.status = 'cancelled'
            job.error_message = 'Cancellation requested'
            job.save()

            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                f'job_progress_{job.job_id}',
                {
                    'type': 'job_progress',
                    'percentage': tracker.get_progress().get('percentage', 0),
                    'stage': 'Cancelling',
                    'status': 'cancelling',
                },
            )

            return Response(
                {'message': 'Cancellation requested', 'job_id': str(job.job_id)},
                status=status.HTTP_202_ACCEPTED,
            )
        except Exception as error:
            logger.exception('Failed to request cancellation for job %s', job.job_id)
            return Response({'error': str(error)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
