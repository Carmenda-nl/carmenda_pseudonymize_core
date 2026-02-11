# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for processing deidentification jobs API."""

from __future__ import annotations

import csv
import html
import os
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

    from api.models import DeidentificationJob

from django.conf import settings
from fastexcel import read_excel

from core.utils.csv_handler import detect_csv_properties, strip_bom
from core.utils.logger import setup_logging

logger = setup_logging()


def get_file_path(uploaded_file: UploadedFile) -> tuple[str, bool]:
    """Get file path from uploaded file, creating temporary file if needed."""
    if hasattr(uploaded_file, 'temporary_file_path'):
        return uploaded_file.temporary_file_path(), False

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        for chunk in uploaded_file.chunks():
            temp_file.write(chunk)

        return temp_file.name, True


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
    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job.job_id)
    base_name = Path(input_file).stem

    output_filename = f'{base_name}_deidentified.csv'
    output_path = job_output_dir / output_filename
    datakey_filename = Path(job.datakey.name).name if job.datakey and job.datakey.name else 'datakey.csv'
    output_datakey_path = job_output_dir / datakey_filename
    log_path = job_output_dir / 'deidentification.log'
    error_path = job_output_dir / f'{base_name}_errors.csv'

    files_to_zip: list[str] = []

    if output_path.exists():
        relative_path = output_path.relative_to(Path(settings.MEDIA_ROOT))
        job.output_file.name = str(relative_path)
        files_to_zip.append(str(output_path))
        output_filename = output_path.name

    if output_datakey_path.exists():
        relative_path = output_datakey_path.relative_to(Path(settings.MEDIA_ROOT))
        job.output_datakey.name = str(relative_path)
        files_to_zip.append(str(output_datakey_path))

    if log_path.exists():
        relative_path = log_path.relative_to(Path(settings.MEDIA_ROOT))
        job.log_file.name = str(relative_path)
        files_to_zip.append(str(log_path))

    if error_path.exists():
        relative_path = error_path.relative_to(Path(settings.MEDIA_ROOT))
        files_to_zip.append(str(error_path))

    return files_to_zip, output_filename


def _csv_preview(file_path: str) -> list[dict]:
    """Read the first 2 data rows from a CSV file."""
    properties = detect_csv_properties(Path(file_path))
    encoding, delimiter = properties['encoding'], properties['delimiter']

    with Path(file_path).open(encoding=encoding, newline='', errors='ignore') as file:
        csv_reader = csv.reader(file, delimiter=delimiter)

        header_row = next(csv_reader)
        header_row[0] = strip_bom(header_row[0])
        header = [col.strip() for col in header_row]

        preview_data = []
        for _ in range(2):
            row = next(csv_reader)

            # Decode HTML entities in each field
            decoded_row = [html.unescape(val) if val else val for val in row]
            preview_data.append(dict(zip(header, decoded_row, strict=False)))

    return preview_data


def _excel_preview(file_path: str) -> list[dict]:
    """Read the first 2 data rows from an Excel file."""
    excel_reader = read_excel(file_path)
    df = excel_reader.load_sheet(0, n_rows=2).to_polars()

    header = [str(col) for col in df.columns]
    preview_data = []

    for row in df.iter_rows():
        row_dict = {
            col: html.unescape(str(val)) if val is not None else '' for col, val in zip(header, row, strict=False)
        }
        preview_data.append(row_dict)

    return preview_data


def generate_preview(job: DeidentificationJob, metadata: dict) -> None:
    """Generate a preview from the first 3 rows of the input file (1 header + 2 data rows)."""
    file_path = job.input_file.path

    if metadata.get('file_type') == 'csv':
        preview_data = _csv_preview(file_path)
    elif metadata.get('file_type') == 'excel':
        preview_data = _excel_preview(file_path)

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
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipped_file:
            for file_path in files_to_zip:
                file_path_obj = Path(file_path)

                if not file_path_obj.exists():
                    error_message = f'File not found for zipping: {file_path}'
                    logger.error(error_message)
                    raise RuntimeError(error_message)

                basename = file_path_obj.name
                zipped_file.write(file_path, basename)
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
