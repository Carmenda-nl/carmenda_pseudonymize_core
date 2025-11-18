# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Utility & helper modules for processing deidentification jobs API."""

from __future__ import annotations

import logging
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from api.models import DeidentificationJob

from django.conf import settings
from rest_framework import serializers

from core.utils.file_handling import check_file
from core.utils.logger import setup_logging

logger = setup_logging()


def validate_input_cols(value: str) -> str:
    """Validate that `input_cols` follows the required format.

    1. Comma-separated
    2. Each value follows the format: key=value
    3. Must contain the key `report`
    """
    if not isinstance(value, str):
        message = 'Input columns must be a string'
        raise serializers.ValidationError(message)

    fields = [field.strip() for field in value.split(',')]

    pattern = re.compile(r'^([^=]+)=(.+)$')
    field_dict = {}

    for field in fields:
        match = pattern.match(field)

        if not match:
            message = f"Field '{field}' does not follow the format 'key=value'"
            raise serializers.ValidationError(message)

        key = match.group(1)
        val = match.group(2)
        field_dict[key] = val

    if 'report' not in field_dict:
        message = "The 'report' key must be present (report=value)"
        raise serializers.ValidationError(message)

    return value


def match_output_cols(input_cols: str) -> str:
    """Transform the to be deidentified input columns to the corresponding output mappings."""
    colums = [col.strip() for col in input_cols.split(',')]
    output_cols = []

    for col in colums:
        if col.startswith('clientname='):
            output_cols.append('clientcode')
        elif col.startswith('report='):
            output_cols.append('processed_report')
        elif '=' in col:
            # Keep other columns unchanged (extract the column name after '=')
            column_name = col.split('=', 1)[1].strip()
            output_cols.append(column_name)

    return ', '.join(output_cols)


def setup_job_logging(job_id: str) -> logging.FileHandler:
    """Create a per-job FileHandler to the 'deidentify' logger."""
    deidentify_logger = logging.getLogger('deidentify')
    job_log_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job_id)
    job_log_dir.mkdir(parents=True, exist_ok=True)
    job_log_path = job_log_dir / 'deidentification.log'

    # Open in write mode to overwrite existing file for this job.
    job_handler = logging.FileHandler(str(job_log_path), mode='w', encoding='utf-8')
    job_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    job_handler.setLevel(deidentify_logger.level)
    deidentify_logger.addHandler(job_handler)

    return job_handler


def collect_output_files(job: DeidentificationJob, input_file: str) -> tuple[list[str], str]:
    """Collect paths of all `the output files that need to be zipped."""
    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job.job_id)
    base_name = Path(input_file).stem

    output_filename = f'{base_name}_deidentified.csv'
    output_path = job_output_dir / output_filename
    datakey_path = job_output_dir / 'datakey.csv'
    log_path = job_output_dir / 'deidentification.log'

    files_to_zip: list[str] = []

    if output_path.exists():
        relative_path = output_path.relative_to(Path(settings.MEDIA_ROOT))
        job.output_file.name = str(relative_path)
        files_to_zip.append(str(output_path))
        output_filename = output_path.name

    if datakey_path.exists():
        relative_path = datakey_path.relative_to(Path(settings.MEDIA_ROOT))
        job.datakey.name = str(relative_path)
        files_to_zip.append(str(datakey_path))

    if log_path.exists():
        relative_path = log_path.relative_to(Path(settings.MEDIA_ROOT))
        job.log_file.name = str(relative_path)
        files_to_zip.append(str(log_path))

    return files_to_zip, output_filename


def generate_input_preview(job: DeidentificationJob) -> None:
    """Generate a preview from the first 3 lines of the input file."""
    file_path = job.input_file.path
    encoding, line_ending, separator = check_file(file_path)

    with Path(file_path).open(encoding=encoding, newline=line_ending) as file:
        lines = file.readlines()

    header = [col.strip() for col in lines[0].strip().split(separator)]

    preview_data = [
        dict(zip(header, [val.strip() for val in line.strip().split(separator)], strict=False))
        for line in lines[1:4]
        if line.strip()
    ]

    job.preview = preview_data
    job.save(update_fields=['preview'])


def create_zipfile(job: DeidentificationJob, files_to_zip: list[str], output_filename: str) -> None:
    """Create a zip file and store its information in the job model."""
    if not files_to_zip:
        error_message = f'No output files found to zip for job {job.pk}'
        logger.error(error_message)
        raise RuntimeError(error_message)

    base_name = Path(output_filename).stem
    zip_filename = f'{base_name}.zip'
    zip_path = Path(settings.MEDIA_ROOT) / 'output' / str(job.job_id) / zip_filename

    included_files: list[str] = []

    try:
        # Create the zip file
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                file_path_obj = Path(file_path)

                if not file_path_obj.exists():
                    error_message = f'File not found for zipping: {file_path}'
                    logger.error(error_message)
                    raise RuntimeError(error_message)

                basename = file_path_obj.name
                zipf.write(file_path, basename)
                included_files.append(basename)

        # Store zip information in job model
        job.zip_file.name = os.path.relpath(zip_path, settings.MEDIA_ROOT)
        job.zip_preview = {
            'zip_file': zip_filename,
            'files': included_files,
        }

    except OSError as error:
        error_message = f'Failed to create zip file: {error}'
        logger.exception(error_message)
        raise


def get_metadata(represent: dict, instance: DeidentificationJob, fields: list[str]) -> dict:
    """Populate files with url, filesize and last_modified date."""
    for field in fields:
        file_url = represent.get(field)
        file_field = getattr(instance, field, None)
        relative_path = getattr(file_field, 'name', None) if file_field is not None else None

        if relative_path:
            file_path = Path(settings.MEDIA_ROOT) / relative_path
            if file_path.exists():
                file = file_path.stat()
                file_size = int(file.st_size)
                file_creation = int(file.st_birthtime)

                if file_size >= 1 << 30:
                    filesize = f'{file_size / (1 << 30):.2f} Gb'
                elif file_size >= 1 << 20:
                    filesize = f'{file_size / (1 << 20):.2f} Mb'
                else:
                    filesize = f'{file_size / 1024:.2f} Kb'

                represent[field] = {
                    'url': file_url,
                    'filesize': filesize,
                    'build_date': file_creation,
                }
    return represent
