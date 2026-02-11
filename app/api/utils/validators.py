# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Validators for checking deidentification jobs API."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

from fastexcel import read_excel
from rest_framework import serializers

from api.utils.file_handling import get_file_path
from core.utils.csv_handler import detect_csv_properties, strip_bom
from core.utils.logger import setup_logging

logger = setup_logging()


def _validate_required_columns(columns: list[str], input_cols: str) -> None:
    """Validate that all specified input columns exist in the given columns."""
    required = dict(column.strip().split('=') for column in input_cols.split(','))

    for col_value in required.values():
        if col_value not in columns:
            available = ', '.join(columns)
            message = f'Column "{col_value}" not found in input file, available columns: {available}'
            raise serializers.ValidationError(message)


def _validate_datakey_columns(columns: list[str]) -> None:
    """Validate that datakey columns are present."""
    required_columns = ['Clientnaam', 'Synoniemen', 'Code']
    missing = [col for col in required_columns if col not in columns]

    if missing:
        message = f'Datakey file must contain columns: {", ".join(required_columns)}.'
        raise serializers.ValidationError(message)


class FileValidationResult(TypedDict, total=False):
    """Return type for validate_file."""

    file: UploadedFile
    file_type: str
    encoding: str
    delimiter: str


def _validate_csv(file_path: str, input_cols: str | None, datakey: str) -> FileValidationResult:
    """Validate a CSV file: structure, columns and metadata."""
    properties = detect_csv_properties(Path(file_path))
    encoding, delimiter = properties['encoding'], properties['delimiter']

    with Path(file_path).open(encoding=encoding, errors='ignore') as csv_file:
        header = strip_bom(csv_file.readline().strip())

        if not header:
            message = 'File must contain a header row'
            raise serializers.ValidationError(message)

        columns = [col.strip() for col in header.split(delimiter)]

        if not columns or (len(columns) == 1 and not columns[0]):
            message = 'File header is empty or invalid'
            raise serializers.ValidationError(message)

        max_row_checks = 10
        data_rows = []
        for line in csv_file:
            if line.strip():
                data_rows.append(line)
            if len(data_rows) >= max_row_checks:
                break

        if len(data_rows) < 1:
            message = 'File must contain at least 1 data row.'
            raise serializers.ValidationError(message)

    if not datakey and columns[:3] == ['Clientnaam', 'Synoniemen', 'Code']:
        message = 'This appears to be a datakey file (columns: Clientnaam, Synoniemen, Code)'
        raise serializers.ValidationError(message)
    if input_cols and not datakey:
        _validate_required_columns(columns, input_cols)
    if datakey:
        _validate_datakey_columns(columns)

    return {
        'file_type': 'csv',
        'encoding': encoding,
        'delimiter': delimiter,
    }


def _validate_excel(file_path: str, input_cols: str | None) -> FileValidationResult:
    """Validate an Excel file: structure, columns and metadata."""
    df = read_excel(file_path).load_sheet(0, n_rows=1).to_polars()

    if df.is_empty() or len(df) < 1:
        message = 'Excel file must contain at least 1 data row.'
        raise serializers.ValidationError(message)

    columns = [str(col) for col in df.columns]
    if input_cols:
        _validate_required_columns(columns, input_cols)

    return {'file_type': 'excel'}


def validate_file(file: UploadedFile, input_cols: str | None = None, datakey: str = '') -> FileValidationResult:
    """Validate uploaded file and return file with metadata.

    Checks that the file:
      - has an allowed extension.
      - if .csv has valid encoding and delimiter.
      - has atleast 1 data row.
      - if the input columns have the specified columns.
      - if the datakey has the mandatory columns.
    """
    input_extension = Path(file.name or '').suffix.lower()

    if datakey and input_extension != '.csv':
        message = 'Datakey must be a CSV file.'
        raise serializers.ValidationError(message)

    file_path, temp_file = get_file_path(file)

    try:
        if input_extension == '.csv':
            result = _validate_csv(file_path, input_cols, datakey)
        elif input_extension in ('.xls', '.xlsx'):
            result = _validate_excel(file_path, input_cols)
        else:
            message = 'Unsupported file extension.'
            raise serializers.ValidationError(message)
    finally:
        if temp_file and file_path:
            Path(file_path).unlink(missing_ok=True)

    result['file'] = file
    return result


def validate_file_columns(input_cols: str, file: UploadedFile | str) -> None:
    """Validate that specified input columns exist in a file."""
    if isinstance(file, str):
        resolved_path = file
        is_temp = False
        extension = Path(file).suffix.lower()
    else:
        resolved_path, is_temp = get_file_path(file)
        extension = Path(file.name or '').suffix.lower()

    try:
        if extension in ('.xls', '.xlsx'):
            df = read_excel(resolved_path).load_sheet(0, n_rows=0).to_polars()
            columns = [str(col) for col in df.columns]
        elif extension == '.csv':
            properties = detect_csv_properties(Path(resolved_path))
            encoding, delimiter = properties['encoding'], properties['delimiter']

            with Path(resolved_path).open(encoding=encoding, errors='ignore') as csv_file:
                header = strip_bom(csv_file.readline().strip())
                columns = [strip_bom(col.strip()) for col in header.split(delimiter)]
        else:
            message = 'Unsupported file extension.'
            raise serializers.ValidationError(message)

        _validate_required_columns(columns, input_cols)
    finally:
        if is_temp and resolved_path:
            Path(resolved_path).unlink(missing_ok=True)


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
