# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Utility & helper modules for processing deidentification jobs API."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from api.models import DeidentificationJob

from django.conf import settings

from core.utils.logger import setup_logging

logger = setup_logging()


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


def collect_output_files(job: DeidentificationJob, input_file: str) -> tuple[list[str], str]:
    """Collect paths of all `the output files that need to be zipped."""
    data_output_dir = Path(settings.MEDIA_ROOT) / 'output'
    base_name = Path(input_file).stem

    output_filename = f'{base_name}_deidentified.csv'
    output_path = data_output_dir / output_filename
    datakey_path = data_output_dir / 'datakey.csv'
    log_path = data_output_dir / 'deidentification.log'

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


def create_zipfile(job: DeidentificationJob, files_to_zip: list[str], output_filename: str) -> None:
    """Create a zip file and store its information in the job model."""
    if not files_to_zip:
        error_message = f'No output files found to zip for job {job.pk}'
        logger.error(error_message)
        raise RuntimeError(error_message)

    base_name = Path(output_filename).stem
    zip_filename = f'{base_name}.zip'
    zip_path = Path(settings.MEDIA_ROOT) / 'output' / zip_filename

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
