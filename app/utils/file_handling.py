# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import csv
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


def get_environment_paths() -> tuple[str, str]:
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

    # Ensure output directory exists
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    return input_folder, output_folder


def _check_file_header(input_file: str) -> tuple[str, str]:
    """Determine the encoding and separator from the first header row."""
    file_path = Path(input_file)

    with file_path.open('rb') as file:
        # Read the first 10KB to avoid memory issues
        sample_data = file.read(10240)
        encoding_result = chardet.detect(sample_data)
        encoding = encoding_result['encoding'] or 'utf-8'
        logger.info('Detected file encoding: "%s"', encoding)
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
        elif input_extension == '.parquet':
            df = pl.read_parquet(input_file_path)
        else:
            return None

        # Log columns schema
        schema_str = 'root\n' + '\n'.join([f' |-- {name}: {dtype}' for name, dtype in df.schema.items()])
        logger.debug('%s \n', schema_str)

        df_rowcount = df.height
        logger.info('Row count: %d', df_rowcount)
        return df

    # File does not exist
    return None


def save_data_file(df: pl.DataFrame, file_path: str, output_extension: str = '.csv') -> None:
    """Save DataFrame to file in specified format."""
    output_path = Path(file_path).parent
    output_path.mkdir(parents=True, exist_ok=True)
    supported_formats = ['.csv', '.parquet']

    try:
        if output_extension == '.csv':
            df.write_csv(f'{file_path}.csv')
        elif output_extension == '.parquet':
            df.write_parquet(f'{file_path}.parquet')
        else:
            logger.exception('Unsupported output format: %s. Supported: %s', output_extension, supported_formats)
    except (OSError, PermissionError):
        logger.exception('Cannot write file "%s": %s', file_path, output_extension)


def load_data_key(key_file_path: str) -> list[dict[str, str]]:
    """Load data key from file and return list of rows."""
    with key_file_path.open(encoding='utf-8') as file:
        key_file_dict = csv.DictReader(file, delimiter=';')
        key_file_rows = []

        for row in key_file_dict:
            client_name = row.get('Clientnaam')

            # Only add rows with valid client names
            if client_name:
                row['Clientnaam'] = client_name.strip()
                key_file_rows.append(dict(row))

        return key_file_rows


def save_data_key(data_key: list[dict[str, str]], output_folder: str) -> None:
    """Save the processed data key to a CSV file for future use."""
    filename = 'data_key.csv'
    file_path = Path(output_folder) / filename

    try:
        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        with file_path.open('w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(['Clientnaam', 'Synoniemen', 'Code'])

            for entry in data_key:
                writer.writerow([entry.get('Clientnaam'), entry.get('Synoniemen'), entry.get('Code')])

    except (OSError, TypeError, AttributeError):
        logger.exception('Cannot write data key to "%s"', file_path)


def create_output_file_path(output_folder: str, input_filename: str, suffix: str = '') -> str:
    """Create output file path based on input filename."""
    input_path = Path(input_filename)
    base_name = input_path.stem

    if suffix:
        base_name = f'{base_name}{suffix}'
    return str(Path(output_folder) / base_name)
