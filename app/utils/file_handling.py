# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import chardet
import polars as pl

from utils.logger import setup_logging

logger = setup_logging()

# Suppress chardet debug logs (noise)
logging.getLogger('chardet').setLevel(logging.WARNING)


def get_environment() -> tuple[str, str]:
    """Get input and output folder paths based on the current environment."""
    if os.environ.get('DOCKER_ENV') == 'true':
        # Docker environment
        input_folder = '/app/data/input'
        output_folder = '/app/data/output'
    elif getattr(sys, 'frozen', False):
        # PyInstaller environment
        base_path = Path(sys._MEIPASS)
        input_folder = str(base_path / 'data' / 'input')
        output_folder = str(base_path / 'data' / 'output')
    else:
        # Script environment
        input_folder = 'data/input'
        output_folder = 'data/output'

    Path(output_folder).mkdir(parents=True, exist_ok=True)
    return input_folder, output_folder


def _check_encoding(input_file: str) -> tuple[str, str]:
    """Determine the encoding and line ending of a file."""
    file_path = Path(input_file)

    with file_path.open('rb') as file:
        # Read the first 10KB to avoid memory issues
        sample_data = file.read(10240)
        encoding_result = chardet.detect(sample_data)
        detected_encoding = encoding_result['encoding'] or 'utf-8'

        # Handle common Windows ANSI detection issues
        if detected_encoding == 'ascii':
            # Try to detect if it's actually Windows ANSI (cp1252)
            try:
                sample_data.decode('cp1252')
                detected_encoding = 'cp1252'  # Windows ANSI
            except UnicodeDecodeError:
                # Keep as ascii if cp1252 fails
                pass

        # Detect line endings - Polars only supports single-byte eol_char
        if b'\r\n' in sample_data:
            line_ending = '\n'    # Use \n for Windows (Polars will handle \r\n automatically)
        elif b'\r' in sample_data:
            line_ending = '\r'    # Macintosh
        elif b'\n' in sample_data:
            line_ending = '\n'    # Unix
        else:
            line_ending = '\n'    # Default

        return detected_encoding, line_ending









def _check_file_header(input_file: str) -> tuple[str, str]:
    """Determine the encoding and separator from the first header row."""
    file_path = Path(input_file)

    with file_path.open('rb') as file:
        # Read the first 10KB to avoid memory issues
        sample_data = file.read(10240)
        encoding_result = chardet.detect(sample_data)
        encoding = encoding_result['encoding'] or 'utf-8'
        logger.info('Detected file encoding: %s', encoding)
    try:
        with file_path.open(encoding=encoding) as file:
            header = file.readline().strip()
    except (UnicodeDecodeError, LookupError) as exception:
        message = f'File cannot be decoded with encoding "{encoding}"'
        logger.exception(message)
        raise ValueError(message) from exception

    if not header:
        message = 'File is empty or has no header'
        logger.exception(message)
        raise ValueError(message)

    candidates = [',', ';', '\t', '|']
    scores = {separator: header.count(separator) for separator in candidates}
    best_separator = max(scores, key=scores.get)

    # Ensure we found at least one separator
    if scores[best_separator] == 0:
        message = f'No valid separator found. Tried: {candidates}'
        logger.exception(message)
        raise ValueError(message)

    return best_separator, encoding




def load_data_file(input_file_path: str) -> pl.DataFrame | None:
    """Check data file availability and log relevant information."""
    if Path(input_file_path).is_file():
        input_extension = Path(input_file_path).suffix
        file_size = Path(input_file_path).stat().st_size
        logger.info('%s file of size: %s', input_extension, file_size)

        if input_extension == '.csv':
            separator, encoding = _check_file_header(input_file_path)
            df = pl.read_csv(input_file_path, separator=separator, encoding=encoding)
        else:
            return None

        # Log columns schema
        schema_str = 'root\n' + '\n'.join([f' |-- {name}: {dtype}' for name, dtype in df.schema.items()])
        logger.debug('%s \n', schema_str)

        df_rowcount = df.height
        logger.info('Row count: %s rows\n', df_rowcount)
        return df

    # File does not exist
    return None






def save_datafile(df: pl.DataFrame, filename: str, output_folder: str) -> None:
    """Save processed DataFrame to file in the specified output folder."""
    filename = Path(filename).stem
    file_path = Path(output_folder) / filename

    try:
        df.write_csv(f'{file_path}.csv')
    except OSError:
        logger.warning('Cannot write %s to "%s".', filename, file_path)


def load_datakey(datakey_path: str) -> pl.DataFrame:
    """Grab valid names from file and return as a Polars DataFrame."""
    file_encoding, line_ending = _check_encoding(datakey_path)
    accepted_encodings = ('utf-8', 'ascii', 'cp1252', 'windows-1252')

    if file_encoding not in accepted_encodings:
        logger.warning('Datakey encoding not supported, provided: %s.', file_encoding)
        return None

    df = pl.read_csv(datakey_path, separator=';', encoding=file_encoding, eol_char=line_ending)
    df = df.rename({'Clientnaam': 'clientname', 'Synoniemen': 'synonyms', 'Code': 'code'})

    return df.with_columns(pl.col('clientname').str.strip_chars()).filter(pl.col('clientname') != '')


def save_datakey(datakey: pl.DataFrame, output_folder: str) -> None:
    """Save the processed datakey to a CSV file for future use."""
    filename = 'datakey.csv'
    file_path = Path(output_folder) / filename

    try:
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        datakey = datakey.rename({'clientname': 'Clientnaam', 'synonyms': 'Synoniemen', 'code': 'Code'})
        datakey.write_csv(file_path, separator=';')
        logger.debug('Saving new datakey: %s\n%s\n', filename, datakey)
    except OSError:
        logger.warning('Cannot write datakey to "%s".', file_path)
