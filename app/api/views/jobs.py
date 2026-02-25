# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""ViewSet for managing deidentification jobs."""

from __future__ import annotations

import gc
from django.db.models.fields.files import FieldFile
import shutil
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from drf_spectacular.utils import extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from api.models import DeidentificationJob
from api.schemas import (
    CANCEL_JOB_GET_SCHEMA,
    CANCEL_JOB_POST_SCHEMA,
    CREATE_JOB_SCHEMA,
    PROCESS_JOB_GET_SCHEMA,
    PROCESS_JOB_POST_SCHEMA,
)
from api.serializers import (
    JobListSerializer,
    JobSerializer,
    JobStatusSerializer,
)
from api.utils.file_handling import generate_consent, generate_preview, match_output_cols
from api.views.processing import run_processing, send_job_progress
from api.views.root import ApiTags
from core.utils.logger import setup_logging
from core.utils.progress_control import job_control
from core.utils.progress_tracker import tracker

if TYPE_CHECKING:
    from django.http import HttpRequest
    from rest_framework.request import Request

logger = setup_logging()


@extend_schema(tags=[ApiTags.JOBS])
@extend_schema_view(process=extend_schema(tags=[ApiTags.PROCESSING]))
class DeidentificationJobViewSet(viewsets.ModelViewSet):
    """ViewSet for managing deidentification jobs."""

    queryset = DeidentificationJob.objects.all()
    serializer_class = JobSerializer
    http_method_names = ('get', 'post', 'put', 'delete')

    def get_serializer_class(self) -> type[JobSerializer | JobListSerializer | JobStatusSerializer]:
        """Return a serializer based on the current action."""
        if self.action == 'list':
            return JobListSerializer
        if self.action in ['process', 'cancel']:
            return JobStatusSerializer
        return JobSerializer

    def _delete_file(self, file_field: FieldFile) -> None:
        """Delete a file from disk if it exists."""
        try:
            Path(file_field.path).unlink(missing_ok=True)
        except (OSError, ValueError, PermissionError):
            logger.warning('Failed to delete file: %s', file_field)

    # def _clean_input_files(self, job: DeidentificationJob, request: HttpRequest) -> None:
    #     """Clean old input files when new ones are uploaded."""
    #     if 'input_file' in request.FILES:
    #         self._clean_file(job.input_file, str(job.job_id), 'input')

    #         # Clean output files
    #         if job.zip_file:
    #             job.zip_file.delete(save=False)
    #         if job.log_file:
    #             job.log_file.delete(save=False)
    #         if job.error_rows_file:
    #             job.error_rows_file.delete(save=False)
    #         if job.output_file:
    #             job.output_file.delete(save=False)

    #         if 'datakey' not in request.FILES and job.datakey:
    #             job.datakey.delete(save=False)

    #     if 'datakey' in request.FILES:
    #         self._clean_file(job.datakey, str(job.job_id), 'datakey')




    def update(self, request: Request, *args: object, **kwargs: object) -> Response:
        """Update a job with PUT request, deleting old files before adding new ones."""
        job = self.get_object()
        data = request.POST.copy()
        data.update(request.FILES)

        if 'input_file' in request.FILES:
            new_columns = request.data.get('input_cols')
            job.status = 'pending'

            if not new_columns or new_columns == job.input_cols:
                job.input_cols = ''
                data.pop('input_cols', None)

            if job.datakey:
                self._delete_file(job.datakey)
            if 'datakey' not in request.FILES:
                job.datakey = None










            job.save(update_fields=['status', 'processed_preview', 'input_cols', 'datakey'])
            generate_preview(job)



















        if 'data_permission' not in data:
            data['data_permission'] = 'false'











        serializer = self.get_serializer(job, data=data, partial=True)
        serializer.is_valid(raise_exception=True)
        job = serializer.save()








        old_permission = job.data_permission
        # Manage consent file if data_permission changed
        if 'data_permission' in serializer.validated_data and job.data_permission != old_permission:
            print('Managing consent file...')
            # generate_consent(job)

        # Reset output files if input_file, input_cols, or datakey are updated/removed
        datakey_changed = 'datakey' in request.FILES
        should_reset = 'input_file' in request.FILES or 'input_cols' in request.data or datakey_changed


        if should_reset:
            job.reset_output()








        return Response(serializer.data, status=status.HTTP_200_OK)



    def perform_destroy(self, instance: DeidentificationJob) -> None:
        """Delete associated files from storage and remove job directory."""
        if instance.status == 'processing':
            # Cancel the job if it's currently processing
            logger.info('Job "%s" Cancelling running job before deletion', instance.job_id)
            try:
                job_control.cancel(str(instance.job_id))

                instance.status = 'cancelled'
                instance.error_message = 'Job cancelled before deletion'
                instance.save()

                # Send WebSocket notification
                percentage = tracker.get_progress().get('percentage')
                send_job_progress(
                    str(instance.job_id),
                    percentage if isinstance(percentage, int) else 0,
                    'Cancelled (deletion)',
                    'cancelled',
                )

                # Give the process a moment to handle cancellation
                time.sleep(0.5)

            except (RuntimeError, OSError):
                logger.warning('Failed to cancel job before deletion.')

        # Force garbage collection to release file handles
        gc.collect()

        dirs = [
            Path(settings.MEDIA_ROOT) / 'input' / str(instance.job_id),
            Path(settings.MEDIA_ROOT) / 'output' / str(instance.job_id),
        ]

        for job_dir in dirs:
            if job_dir.exists() and job_dir.is_dir():
                shutil.rmtree(job_dir)

        super().perform_destroy(instance)

    @CREATE_JOB_SCHEMA
    def create(self, request: Request, *_args: object, **_kwargs: object) -> Response:
        """Prepare a new job with an uploaded file and column mapping configuration."""
        if 'input_file' not in request.FILES:
            return Response(
                {
                    'error': 'input file is required',
                    'message': 'A POST request must include an input_file. Use PUT to update an existing job.',
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job = serializer.save(status='pending')
        generate_preview(job)

        detail_serializer = JobSerializer(job, context={'request': request})
        return Response(detail_serializer.data, status=status.HTTP_201_CREATED)

    @PROCESS_JOB_POST_SCHEMA
    @PROCESS_JOB_GET_SCHEMA
    @action(detail=True, methods=['get', 'post'])
    def process(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Start the deidentification process for a job."""
        job = self.get_object()

        if not job.input_cols:
            return Response(
                {
                    'error': 'Cannot process job without input_cols',
                    'message': 'Please update the job with valid input_cols before processing',
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

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
        """Request cancellation of a running job."""
        job = self.get_object()

        # GET: return status and last seen progress
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

            percentage = tracker.get_progress().get('percentage')
            send_job_progress(
                str(job.job_id),
                percentage if isinstance(percentage, int) else 0,
                'Cancelling',
                'cancelling',
            )

            return Response(
                {'message': 'Cancellation requested', 'job_id': str(job.job_id)},
                status=status.HTTP_202_ACCEPTED,
            )
        except Exception as error:
            logger.exception('Failed to request cancellation for job: %s', job.job_id)
            return Response({'error': str(error)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
