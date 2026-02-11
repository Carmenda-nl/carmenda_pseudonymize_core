# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl

from core.utils.csv_handler import detect_csv_properties, normalize_csv, sanitize_csv

from .logger import setup_logging

logger = setup_logging()


def get_environment() -> tuple[str, str]:
    """Get input and output folder paths based on the current environment."""
    if os.environ.get('DOCKER_ENV') == 'true':
        # Docker environment
        input_folder = '/app/data/input'
        output_folder = '/app/data/output'
    elif getattr(sys, 'frozen', False):
        # PyInstaller environment
        base_path = Path(getattr(sys, '_MEIPASS', '.'))
        input_folder = str(base_path / 'data' / 'input')
        output_folder = str(base_path / 'data' / 'output')
    else:
        # Script environment
        input_folder = 'data/input'
        output_folder = 'data/output'

    Path(output_folder).mkdir(parents=True, exist_ok=True)
    return input_folder, output_folder


def load_datafile(input_file: str, output_folder: str) -> pl.DataFrame | None:
    """Load datafile and return as a DataFrame."""
    file_path = Path(input_file)
    if not file_path.is_file():
        return None

    input_extension = file_path.suffix
    file_size = file_path.stat().st_size
    logger.info('%s file of size: %s bytes', input_extension, file_size)

    if input_extension.lower() == '.csv':
        properties = detect_csv_properties(file_path)
        sanitized_csv = sanitize_csv(file_path, properties, output_folder)
        input_file = normalize_csv(Path(sanitized_csv), properties)

        df = pl.read_csv(source=input_file, encoding='utf-8', separator=',')

        # Cleanup temp files
        Path(sanitized_csv).unlink(missing_ok=True)
        Path(input_file).unlink(missing_ok=True)
    elif input_extension.lower() == '.xls' or input_extension.lower() == '.xlsx':
        df = pl.read_excel(source=input_file, raise_if_empty=False)
    else:
        logger.error('Unsupported file type: %s', input_extension)
        return None

    return df


def save_datafile(df: pl.DataFrame, filename: str, output_folder: str) -> None:
    """Save processed DataFrame to file in the specified output folder."""
    filepath = Path(filename)
    stem = filepath.stem
    parent = filepath.parent

    # If filename included a parent (like job_id), write into that subfolder under output.
    target_dir = Path(output_folder) / parent if str(parent) and str(parent) != '.' else Path(output_folder)

    try:
        target_dir.mkdir(parents=True, exist_ok=True)

        input_extension = filepath.suffix
        filepath = target_dir / f'{stem}_deidentified{input_extension}'
        if input_extension.lower() == '.csv':
            df.write_csv(str(filepath))
        elif input_extension.lower() == '.xls' or input_extension.lower() == '.xlsx':
            df.write_excel(str(filepath))
    except OSError:
        logger.warning('Cannot write %s to "%s".', filename, target_dir)


def load_datakey(datakey_path: str) -> pl.DataFrame | None:
    """Grab valid names from file and return as a Polars DataFrame."""
    properties = detect_csv_properties(Path(datakey_path))
    encoding, delimiter = properties['encoding'], properties['delimiter']

    accepted_encodings = ('utf-8', 'ascii', 'cp1252', 'windows-1252', 'ISO-8859-1', 'latin1')

    if encoding not in accepted_encodings:
        logger.warning('Datakey encoding not supported, provided: %s.', encoding)
        return None

    df = pl.read_csv(datakey_path, encoding=encoding, separator=delimiter, eol_char='\n')
    df = df.rename({'Clientnaam': 'clientname', 'Synoniemen': 'synonyms', 'Code': 'code'})

    return df.with_columns(pl.col('clientname').str.strip_chars()).filter(pl.col('clientname') != '')


def save_datakey(datakey: pl.DataFrame, filename: str, output_folder: str, datakey_name: str | None = None) -> None:
    """Save the processed datakey to a CSV file for future use."""
    filepath = Path(filename)
    parent = filepath.parent

    output_filename = datakey_name or 'datakey.csv'

    # If filename included a parent (like job_id), write into that subfolder under output.
    target_dir = Path(output_folder) / parent if str(parent) and str(parent) != '.' else Path(output_folder)
    file_path = target_dir / output_filename

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        datakey = datakey.rename({'clientname': 'Clientnaam', 'synonyms': 'Synoniemen', 'code': 'Code'})
        datakey.write_csv(file_path, separator=',')
        logger.debug('Saving datakey: %s\n%s\n', output_filename, datakey)
    except OSError:
        logger.warning('Cannot write datakey to "%s".', file_path)
