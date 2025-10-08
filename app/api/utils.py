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
        if col.startswith('report='):
            output_cols.append('processed_report')

    return ', '.join(output_cols)


def collect_output_files(job: DeidentificationJob, input_file: str) -> tuple[list[str], str]:
    """Collect paths of all `the output files that need to be zipped."""
    data_output_dir = Path(settings.MEDIA_ROOT) / 'output'
    output_path = data_output_dir / input_file
    datakey_path = data_output_dir / 'datakey.csv'
    log_path = data_output_dir / 'deidentification.log'

    files_to_zip = []
    output_filename = input_file

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
    zip_filename = f'{base_name}_deidentified.zip'
    zip_path = Path(settings.MEDIA_ROOT) / 'output' / zip_filename

    included_files = []

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
