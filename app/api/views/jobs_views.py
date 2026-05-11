# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""ViewSet for managing deidentification jobs."""

from __future__ import annotations

import gc
import shutil
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from drf_spectacular.utils import extend_schema, extend_schema_view
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from api.models import DeidentificationJob
from api.schemas import (
    CANCEL_JOB_POST_SCHEMA,
    CREATE_JOB_SCHEMA,
    PROCESS_JOB_GET_SCHEMA,
    PROCESS_JOB_POST_SCHEMA,
    ZIP_FILES_GET_SCHEMA,
    ZIP_FILES_POST_SCHEMA,
)
from api.serializers import (
    JobListSerializer,
    JobSerializer,
    JobStatusSerializer,
    ZipSerializer,
)
from api.services.job_runner import run_processing
from api.utils.file_handling import (
    collect_output_files,
    create_zipfile,
    generate_consent,
    generate_preview,
    match_output_cols,
)
from api.views.root_views import ApiTags
from core.utils.logger import setup_logging
from core.utils.progress_control import job_control

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

    def get_serializer_class(self) -> type[JobSerializer | JobListSerializer | JobStatusSerializer | ZipSerializer]:
        """Return a serializer based on the current action."""
        if self.action == 'list':
            return JobListSerializer
        if self.action in ['process', 'cancel']:
            return JobStatusSerializer
        if self.action == 'zip_files':
            return ZipSerializer
        return JobSerializer

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

        if job.data_permission:
            generate_consent(job)

        detail_serializer = JobSerializer(job, context={'request': request})

        return Response(detail_serializer.data, status=status.HTTP_201_CREATED)

    def _reset_job(self, request: Request, job: DeidentificationJob, *, input_uploaded: bool) -> None:
        """Reset the job's output and related fields."""
        if job.status == 'processing':
            job_control.cancel(str(job.job_id))

        job_control.join_thread(str(job.job_id))
        gc.collect()
        job.reset_output()

        if input_uploaded:
            generate_preview(job)
        if input_uploaded and 'data_permission' not in request.data:
            job.data_permission = False
            job.save(update_fields=['data_permission'])
        if job.data_permission:
            generate_consent(job)

    def update(self, request: Request, *args: object, **kwargs: object) -> Response:
        """Update a job with PUT request, deleting or overwrite old files after adding new ones."""
        job = self.get_object()
        data = request.POST.copy()
        data.update(request.FILES)

        old_columns = job.input_cols
        old_input = job.input_file.name if job.input_file else None
        old_datakey = job.datakey.name if job.datakey else None
        old_permission = job.data_permission

        new_columns = request.data.get('input_cols')
        columns_changed = new_columns is not None and new_columns != old_columns

        new_input = request.FILES.get('input_file')
        input_uploaded = new_input is not None
        input_changed = input_uploaded and (not old_input or Path(old_input).name != new_input.name)

        new_datakey = request.FILES.get('datakey')
        datakey_changed = new_datakey is not None

        if new_columns == old_columns and input_uploaded:
            job.input_cols = ''
            data.pop('input_cols', None)

        serializer = self.get_serializer(job, data=data, partial=True)
        serializer.is_valid(raise_exception=True)
        job = serializer.save()

        if input_changed and old_input and job.input_file.name != old_input:
            job.input_file.storage.delete(old_input)

        if (datakey_changed or input_uploaded) and old_datakey:
            job.datakey.storage.delete(old_datakey)
            job.datakey = new_datakey
            job.save(update_fields=['datakey'])

        new_permission = serializer.validated_data.get('data_permission', False)
        permission_changed = new_permission != old_permission

        if permission_changed:
            job.data_permission = new_permission
            job.save(update_fields=['data_permission'])
            job.reset_zip()
            job.save(update_fields=['zip_file', 'zip_preview'])

        should_reset = input_uploaded or datakey_changed or columns_changed

        if not should_reset and ('data_permission' in request.data or (not job.data_permission and job.consent_file)):
            generate_consent(job)

        if should_reset:
            self._reset_job(request, job, input_uploaded=input_uploaded)

        return Response(JobSerializer(job, context={'request': request}).data, status=status.HTTP_200_OK)

    def perform_destroy(self, instance: DeidentificationJob) -> None:
        """Delete associated files from storage and remove job directory."""
        if instance.status == 'processing':
            logger.info('Job "%s" Cancelling running job before deletion', instance.job_id)

            try:
                job_control.cancel(str(instance.job_id))

                instance.status = 'cancelled'
                instance.error_message = 'Job cancelled before deletion'
                instance.save()
            except (RuntimeError, OSError):
                logger.warning('Failed to cancel job before deletion.')

        job_control.join_thread(str(instance.job_id))

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

    @extend_schema(tags=[ApiTags.CANCEL])
    @CANCEL_JOB_POST_SCHEMA
    @action(detail=True, methods=['post'])
    def cancel(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Request cancellation of a running job."""
        job = self.get_object()
        if job.status != 'processing':
            return Response({'message': 'Job not running', 'status': job.status}, status=status.HTTP_200_OK)

        try:
            job_control.cancel(str(job.job_id))
            job.status = 'cancelled'
            job.error_message = 'Cancellation requested'
            job.save()

            return Response(
                {'message': 'Cancellation requested', 'job_id': str(job.job_id)},
                status=status.HTTP_202_ACCEPTED,
            )
        except Exception as error:
            logger.exception('Failed to request cancellation for job: %s', job.job_id)
            return Response({'error': str(error)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
            data = self.get_serializer(job).data
            data['endpoint'] = request.build_absolute_uri()
            return Response(data, status=status.HTTP_200_OK)

        if job.status == 'processing':
            return Response(
                {'message': 'Job is already processing'},
                status=status.HTTP_409_CONFLICT,
            )

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
            thread = threading.Thread(
                target=run_processing,
                args=(str(job.job_id), input_file, input_cols, output_cols, datakey),
                daemon=True,
            )
            job_control.register_thread(str(job.job_id), thread)
            thread.start()

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

    @ZIP_FILES_POST_SCHEMA
    @ZIP_FILES_GET_SCHEMA
    @action(detail=True, methods=['get', 'post'])
    def zip_files(self, request: HttpRequest, pk: str | None = None) -> Response:
        """Collect output files and create a ZIP archive for download."""
        job = self.get_object()

        if not job.output_file or job.status != 'completed':
            return Response(
                {
                    'error': 'Job not ready for packaging',
                    'message': 'The job must have an output file and a completed process status.',
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if request.method == 'GET':
            return Response(self.get_serializer(job).data, status=status.HTTP_200_OK)

        files_to_zip = collect_output_files(job)
        create_zipfile(job, files_to_zip)

        job.refresh_from_db()
        return Response(self.get_serializer(job).data, status=status.HTTP_200_OK)
