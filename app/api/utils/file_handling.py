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
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

    from api.models import DeidentificationJob

import polars as pl
from django.conf import settings
from django.utils import timezone
from fastexcel import read_excel

from core.utils.csv_handler import detect_csv_properties, strip_bom
from core.utils.logger import setup_logging

logger = setup_logging()

MAX_FIRST_PREVIEW_ROWS = 3
MAX_LAST_PREVIEW_ROWS = 3


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


def _csv_preview(file_path: str) -> list[dict]:
    """Read the first & last rows from a CSV file."""
    properties = detect_csv_properties(Path(file_path))
    encoding, delimiter = properties['encoding'], properties['delimiter']

    with Path(file_path).open(encoding=encoding, newline='', errors='ignore') as file:
        csv_reader = csv.reader(file, delimiter=delimiter)

        header_row = next(csv_reader)
        header_row[0] = strip_bom(header_row[0])
        header = [col.strip() for col in header_row]

        first_rows: list[list[str]] = []
        tail: deque = deque(maxlen=MAX_LAST_PREVIEW_ROWS)

        for row in csv_reader:
            if not any(row):
                continue
            if len(first_rows) < MAX_FIRST_PREVIEW_ROWS:
                first_rows.append(row)
            else:
                tail.append(row)

    # If fewer than max data rows, only return the first rows.
    selected = first_rows + list(tail) if len(tail) >= MAX_LAST_PREVIEW_ROWS else first_rows

    preview = []
    for row in selected:
        decoded_values = [html.unescape(value) if value else value for value in row]
        preview.append(dict(zip(header, decoded_values, strict=False)))

    return preview


def _excel_preview(file_path: str) -> list[dict]:
    """Read the first & last rows from an Excel file."""
    excel_reader = read_excel(file_path)
    sheet = excel_reader.load_sheet(0)
    total_rows = sheet.total_height

    def to_dicts(df: pl.DataFrame) -> list[dict]:
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        return [{col: html.unescape(str(val)) for col, val in row.items()} for row in df.to_dicts()]

    # Buffer extra rows to compensate for empty rows being filtered out
    head_buffer = MAX_FIRST_PREVIEW_ROWS * 5

    head_df = excel_reader.load_sheet(0, n_rows=min(head_buffer, total_rows)).to_polars()
    first_rows = to_dicts(head_df)[:MAX_FIRST_PREVIEW_ROWS]

    if total_rows < MAX_FIRST_PREVIEW_ROWS + MAX_LAST_PREVIEW_ROWS:
        return first_rows

    tail_buffer = MAX_LAST_PREVIEW_ROWS * 5
    skip = max(0, total_rows - tail_buffer)
    tail_df = excel_reader.load_sheet(0, skip_rows=skip).to_polars()
    tail = to_dicts(tail_df)[-MAX_LAST_PREVIEW_ROWS:]

    return first_rows + tail


def generate_preview(job: DeidentificationJob) -> None:
    """Generate a preview from the input file (1 header + data rows)."""
    file_path = job.input_file.path
    file_extension = Path(file_path).suffix.lower()

    if file_extension == '.csv':
        preview_data = _csv_preview(file_path)
    elif file_extension in {'.xlsx', '.xls'}:
        preview_data = _excel_preview(file_path)

    job.preview = preview_data
    job.save(update_fields=['preview'])


def generate_consent(job: DeidentificationJob) -> None:
    """Create consent.txt and store its path on the job when data_permission is set."""
    output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job.job_id)

    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    if not job.data_permission or not job.input_file:
        job.consent_file.delete(save=False)
        job.consent_file = None
        job.save(update_fields=['consent_file'])
        return

    base_name = Path(job.input_file.name).stem
    consent_filename = f'{base_name}_consent.txt'
    consent_path = output_dir / consent_filename

    timestamp = timezone.now().strftime('%d-%m-%Y %H:%M')
    consent_path.write_text(
        f'Data permission granted.\n\nFilename: {base_name}\nTimestamp: {timestamp}\n',
        encoding='utf-8',
    )

    job.consent_file.name = str(Path('output') / str(job.job_id) / consent_filename)
    job.save(update_fields=['consent_file'])


def collect_output_files(job: DeidentificationJob) -> list[str]:
    """Collect all files in the job output directory, excluding the input file."""
    job_output_dir = Path(settings.MEDIA_ROOT) / 'output' / str(job.job_id)
    input_filename = Path(job.input_file.name).name

    if not job_output_dir.exists():
        return []

    return [
        str(path)
        for path in sorted(job_output_dir.iterdir())
        if path.is_file() and path.name != input_filename and path.suffix != '.zip'
    ]


def create_zipfile(job: DeidentificationJob, files_to_zip: list[str]) -> None:
    """Create a zip file and store its information in the job model."""
    output_filename = Path(job.output_file.name).name

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
        job.save(update_fields=['zip_file', 'zip_preview'])

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
                file_creation = int(getattr(file, 'st_birthtime', file.st_ctime))

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
