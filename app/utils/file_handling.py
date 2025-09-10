# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import polars as pl


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


def get_file_extension(file_path: str) -> str:
    """Get file extension from a file path."""
    return Path(file_path).suffix


def get_file_size(file_path: str) -> int:
    """Get the size of a file in bytes."""
    try:
        return Path(file_path).stat().st_size
    except (FileNotFoundError, PermissionError) as e:
        error_msg = f'Cannot access file "{file_path}": {e}'
        raise type(e)(error_msg) from e


def detect_separator(input_file: str) -> str:
    """Determine the separator from the first header row."""
    file_path = Path(input_file)

    try:
        with file_path.open(encoding='utf-8') as file:
            header = file.readline().strip()
    except (FileNotFoundError, PermissionError, UnicodeDecodeError) as e:
        error_msg = f'Cannot read file "{input_file}": {e}'
        raise ValueError(error_msg) from e

    if not header:
        error_msg = f'File "{input_file}" is empty or has no header'
        raise ValueError(error_msg)

    candidates = [',', ';', '\t', '|']
    scores = {separator: header.count(separator) for separator in candidates}
    best_separator = max(scores, key=scores.get)

    # Ensure we found at least one separator
    if scores[best_separator] == 0:
        error_msg = f'No valid separator found in "{input_file}". Tried: {candidates}'
        raise ValueError(error_msg)

    return best_separator


def load_data_file(file_path: str, separator: str | None = None) -> pl.DataFrame:
    """Load data from CSV or Parquet file."""
    file_extension = get_file_extension(file_path)

    if file_extension == '.csv':
        if separator is None:
            separator = detect_separator(file_path)
        return pl.read_csv(file_path, separator=separator)

    if file_extension == '.parquet':
        return pl.read_parquet(file_path)

    supported_formats = ['.csv', '.parquet']
    message = f'Unsupported file format: {file_extension}. Supported: {supported_formats}'
    raise ValueError(message)


def save_data_file(df: pl.DataFrame, file_path: str, output_extension: str = '.parquet') -> None:
    """Save DataFrame to file in specified format."""
    output_path = Path(file_path).parent
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        if output_extension == '.csv':
            df.write_csv(f'{file_path}.csv')
        elif output_extension == '.parquet':
            df.write_parquet(f'{file_path}.parquet')
        else:
            supported_formats = ['.csv', '.parquet']
            error_msg = f'Unsupported output format: {output_extension}. Supported: {supported_formats}'
            raise ValueError(error_msg)
    except (OSError, PermissionError) as e:
        error_msg = f'Cannot write file "{file_path}{output_extension}": {e}'
        raise OSError(error_msg) from e


def load_data_key(key_file_path: str) -> dict[str, str]:
    """Load data key from file."""
    file_path = Path(key_file_path)

    try:
        with file_path.open(encoding='utf-8') as file:
            data = csv.reader(file)
    except FileNotFoundError as error:
        error_msg = f'data key file not found: "{key_file_path}"'
        raise FileNotFoundError(error_msg) from error

    # Validate that it's a string-to-string mapping
    if not isinstance(data, dict):
        error_msg = f'data key must be a CSV object, got {type(data).__name__}'
        raise TypeError(error_msg)

    # Ensure all keys and values are strings
    try:
        return {str(k): str(v) for k, v in data.items()}
    except (TypeError, AttributeError) as error:
        error_msg = f'data key must contain string mappings: {error}'
        raise ValueError(error_msg) from error


def save_data_key(data_key: dict[str, str], output_folder: str, filename: str = 'data_key.csv') -> None:
    """Save data key to CSV file."""
    output_path = Path(output_folder)

    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        error_msg = f'Cannot create output directory "{output_folder}": {error}'
        raise OSError(error_msg) from error

    file_path = output_path / filename
    try:
        with file_path.open('w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['patient', 'synonym', 'data key'])

            for original, pseudonym in data_key.items():
                writer.writerow([original, '', pseudonym])

    except (OSError, TypeError) as error:
        error_msg = f'Cannot write data key to "{file_path}": {error}'
        raise OSError(error_msg) from error


def create_output_file_path(output_folder: str, input_filename: str, suffix: str = '') -> str:
    """Create output file path based on input filename."""
    input_path = Path(input_filename)
    base_name = input_path.stem

    if suffix:
        base_name = f'{base_name}{suffix}'
    return str(Path(output_folder) / base_name)
