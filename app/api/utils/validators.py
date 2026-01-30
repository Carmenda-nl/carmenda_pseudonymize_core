# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Validators for checking deidentification jobs API."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.core.files.uploadedfile import UploadedFile

from rest_framework import serializers

from api.utils.file_handling import get_file_path
from core.utils.file_handling import check_file, strip_bom
from core.utils.logger import setup_logging

logger = setup_logging()


def _validate_extension(file: UploadedFile) -> None:
    """Validate that the uploaded file has an allowed extension."""
    filename = (file.name or '').lower()

    if not filename.endswith('.csv'):
        message = 'Only CSV files are allowed.'
        raise serializers.ValidationError(message)


def _validate_content(file_path: str, encoding: str, separator: str) -> None:
    """Validate if a header is present and there is minimal 1 row present."""
    with Path(file_path).open(encoding=encoding) as file:
        header = strip_bom(file.readline().strip())

        if not header:
            message = 'File must contain a header row'
            raise serializers.ValidationError(message)

        columns = [col.strip() for col in header.split(separator)]

        if not columns or (len(columns) == 1 and not columns[0]):
            message = 'File header is empty or invalid'
            raise serializers.ValidationError(message)

        max_row_checks = 10
        data_rows = []

        for line in file:
            if line.strip():
                data_rows.append(line)
            if len(data_rows) >= max_row_checks:
                break
        if len(data_rows) < 1:
            message = 'File must contain at least 1 data row.'
            raise serializers.ValidationError(message)


def _is_datakey(file_path: str, encoding: str, separator: str) -> bool:
    """Check if file appears to be a datakey based on header columns."""
    with Path(file_path).open(encoding=encoding) as file:
        header = strip_bom(file.readline().strip())
        columns = [col.strip() for col in header.split(separator)]

        datakey_columns = ['Clientnaam', 'Synoniemen', 'Code']
        return columns[:3] == datakey_columns


def validate_file_columns(file_path: str, encoding: str, separator: str, input_cols: str) -> None:
    """Validate that specified columns exist in the file."""
    input_cols_dict = dict(column.strip().split('=') for column in input_cols.split(','))

    with Path(file_path).open(encoding=encoding) as file:
        header = strip_bom(file.readline().strip())
        columns = [strip_bom(col.strip()) for col in header.split(separator)]

        for col_value in input_cols_dict.values():
            if col_value not in columns:
                available_cols = ', '.join(columns)
                message = f'Column "{col_value}" not found in input file, available columns: {available_cols}'
                raise serializers.ValidationError(message)


def _validate_datakey_columns(file_path: str, encoding: str, separator: str) -> None:
    """Validate that datakey file has the required columns."""
    with Path(file_path).open(encoding=encoding) as file:
        header = strip_bom(file.readline().strip())
        columns = [col.strip() for col in header.split(separator)]

        required_columns = ['Clientnaam', 'Synoniemen', 'Code']
        missing_columns = [col for col in required_columns if col not in columns]

        if missing_columns:
            message = f'Datakey file must contain columns: {", ".join(required_columns)}.'
            raise serializers.ValidationError(message)


def validate_file(
    uploaded_file: UploadedFile,
    input_cols: str | None = None,
    datakey: str = '',
) -> tuple[UploadedFile, str, str]:
    """Validate uploaded file and return file with metadata.

    Checks that the file:
      - is a csv file
      - has valid encoding and separator.
      - has a header and at least 1 data row.
      - if the input columns have the specified columns.
      - if the datakey has the mandatory columns.
    """
    _validate_extension(uploaded_file)

    file_path, temp_file = get_file_path(uploaded_file)

    encoding, separator = check_file(file_path)
    checks = {'encoding': encoding, 'column separator': separator}
    missing = [check for check, value in checks.items() if not value]

    if missing:
        message = f'Could not determine file {", ".join(missing)}'
        raise serializers.ValidationError(message)

    _validate_content(file_path, encoding, separator)

    if not datakey and _is_datakey(file_path, encoding, separator):
        message = 'This appears to be a datakey file (columns: Clientnaam, Synoniemen, Code)'
        raise serializers.ValidationError(message)

    if input_cols and not datakey:
        validate_file_columns(file_path, encoding, separator, input_cols)

    if datakey:
        _validate_datakey_columns(file_path, encoding, separator)

    # Clean up temporary file if we created one
    if temp_file and file_path:
        Path(file_path).unlink(missing_ok=True)

    return uploaded_file, encoding, separator


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
