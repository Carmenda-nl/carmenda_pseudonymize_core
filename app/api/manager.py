# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Manager module for processing deidentification jobs.

This module handles the background processing of deidentification jobs,
including file processing, status tracking, and zipping the results.
"""

from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path
from typing import NamedTuple

from django.conf import settings

from api.models import DeidentificationJob
from utils.logger import setup_logging

logger = setup_logging()


class DeidentificationConfig(NamedTuple):
    """Configuration for deidentification processing."""

    input_fofi: str
    input_cols: dict
    output_cols: dict
    data_key: dict
    output_extension: str


def _execute_deidentification(config: DeidentificationConfig, job: DeidentificationJob) -> None:
    """Execute a deidentification job."""
    try:
        from core.data_processor import process_data  # noqa: PLC0415 (Initialize core only when needed)

        # Call the core's deidentification process (data processor)
        processed_rows_json = process_data(
            input_fofi=config.input_fofi,
            input_cols=config.input_cols,
            output_cols=config.output_cols,
            data_key=config.data_key,
            output_extension=config.output_extension,
        )

        _save_processed_preview(job, processed_rows_json)

        # Collect output files and update job model
        files_to_zip, output_filename = _collect_output_files(job, config.input_fofi)
        _create_and_store_zip(job, files_to_zip, output_filename)

        # If no errors, update job to 'completed'
        job.status = 'completed'
        job.save()

    except (OSError, RuntimeError, ValueError) as e:
        # Update job status
        logger.exception('Error processing job %s', job.pk)
        try:
            job.status = 'failed'
            job.error_message = f'Job error: {e!s}'
            job.save()
        except (OSError, RuntimeError):
            logger.exception('Failed to update job status for job %s', job.id)


def _transform_output_cols(input_cols: str) -> str:
    """Transform input column mappings to their corresponding output mappings.

    Example:
        transform_output_cols('patientName=name, report=text, other=value')
        'patientName=patientID, report=processed_report, other=value'

    """
    parts = [part.strip() for part in input_cols.split(',')]

    for col_name, part in enumerate(parts):
        if part.startswith('patientName='):
            parts[col_name] = 'patientName=patientID'
        elif part.startswith('report='):
            parts[col_name] = 'report=processed_report'

    return ', '.join(parts)


def _setup_deidentification_job(job_id: str) -> tuple[DeidentificationJob, DeidentificationConfig, str]:
    """Set up the job and create configuration for deidentification."""
    job = DeidentificationJob.objects.get(pk=job_id)
    job.status = 'processing'
    job.save()

    input_cols = job.input_cols
    output_cols = _transform_output_cols(input_cols)

    input_fofi = Path(job.input_file.name).name
    input_extension = Path(input_fofi).suffix.lower()
    output_extension = input_extension if input_extension in ['.csv', '.parquet'] else '.csv'
    data_key = Path(job.key_file.name).name if job.key_file else None

    config = DeidentificationConfig(
        input_fofi=input_fofi,
        input_cols=input_cols,
        output_cols=output_cols,
        data_key=data_key,
        output_extension=output_extension,
    )

    return job, config, input_fofi


def _save_processed_preview(job: DeidentificationJob, processed_rows_json: str | None) -> None:
    """Save the processed data preview to the job model."""
    if processed_rows_json:
        processed_data = json.loads(processed_rows_json)
        job.processed_preview = processed_data
        job.save()


def _collect_output_files(job: DeidentificationJob, input_fofi: str) -> tuple[list[str], str]:
    """Collect paths of all output files and update job model."""
    data_output_dir = Path(settings.MEDIA_ROOT) / 'output'
    output_path = data_output_dir / input_fofi
    key_path = data_output_dir / 'data_key.csv'
    log_path = data_output_dir / 'deidentification.log'

    files_to_zip = []
    output_filename = input_fofi

    if output_path.exists():
        relative_path = output_path.relative_to(Path(settings.MEDIA_ROOT))
        job.output_file.name = str(relative_path)
        files_to_zip.append(str(output_path))
        output_filename = output_path.name

    if key_path.exists():
        relative_path = key_path.relative_to(Path(settings.MEDIA_ROOT))
        job.key_file.name = str(relative_path)
        files_to_zip.append(str(key_path))

    if log_path.exists():
        relative_path = log_path.relative_to(Path(settings.MEDIA_ROOT))
        job.log_file.name = str(relative_path)
        files_to_zip.append(str(log_path))

    return files_to_zip, output_filename


def _create_zip_file(_job_id: str, file_paths: list[str], output_filename: str) -> tuple[Path, str, list[str]]:
    """Create a zip file containing the output files."""
    base_name = Path(output_filename).stem
    zip_filename = f'{base_name}_deidentified.zip'

    # Define zip file path with output file name
    zip_path = Path(settings.MEDIA_ROOT) / 'output' / zip_filename

    included_files = []

    # Create the zip file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in file_paths:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                basename = file_path_obj.name

                # Add file to zip with the determined name
                zipf.write(file_path, basename)

                # Add to our list of included files
                included_files.append(basename)
            else:
                logger.warning('File not found for zipping: %s', file_path)

    return zip_path, zip_filename, included_files


def _create_and_store_zip(job: DeidentificationJob, files_to_zip: list[str], output_filename: str) -> None:
    """Create zip file and store its information in the job model."""
    if not files_to_zip:
        logger.warning('No output files found to zip for job %s', job.pk)
        return

    try:
        zip_path, zip_filename, included_files = _create_zip_file(str(job.pk), files_to_zip, output_filename)
        relative_zip_path = os.path.relpath(zip_path, settings.MEDIA_ROOT)

        job.zip_file.name = relative_zip_path
        job.zip_preview = {
            'zip_file': zip_filename,
            'files': included_files,
        }
    except (OSError, zipfile.BadZipFile, RuntimeError):
        logger.exception('Failed to create zip file')


def process_deidentification(job_id: str) -> None:
    """Process the deidentification job synchronously.

    This function executes the deidentification process and waits for completion.
    """
    try:
        job, config, _output_filename = _setup_deidentification_job(job_id)
        _execute_deidentification(config, job)

    except (OSError, RuntimeError, ValueError) as e:
        logger.exception('Error starting job %s', job_id)
        try:
            job = DeidentificationJob.objects.get(pk=job_id)
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except (OSError, RuntimeError):
            logger.exception('Failed to update job status')
