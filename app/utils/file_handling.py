# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import sys
import json
from typing import TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    import polars as pl

import polars as pl


def detect_separator(input_file: str) -> str:
    """Determine the separator from the first header row.

    Parameters
    ----------
    input_file : str
        Path to the input file

    Returns
    -------
    str
        The detected separator character

    """
    file_path = Path(input_file)
    with file_path.open(encoding='utf-8') as file:
        header = file.readline()

    candidates = [',', ';', '\t', '|']
    scores = {separator: header.count(separator) for separator in candidates}
    return max(scores, key=scores.get)


def get_environment_paths() -> tuple[str, str]:
    """Get input and output folder paths based on the current environment.

    Returns
    -------
    tuple[str, str]
        Tuple containing (input_folder, output_folder) paths

    """
    if sys.argv[0] == 'manage.py':
        # Django environment
        input_folder = 'data/input/'
        output_folder = 'data/output/'
    elif getattr(sys, 'frozen', False):
        # PyInstaller environment
        base_path = Path(sys._MEIPASS)  # noqa: SLF001
        input_folder = str(base_path / 'data' / 'input')
        output_folder = str(base_path / 'data' / 'output')
    else:
        # Script environment
        input_folder = 'data/input'
        output_folder = 'data/output'

    return input_folder, output_folder


def get_file_extension(file_path: str) -> str:
    """Get file extension from a file path.

    Parameters
    ----------
    file_path : str
        Path to the file

    Returns
    -------
    str
        File extension including the dot (e.g., '.csv', '.parquet')

    """
    return Path(file_path).suffix


def get_file_size(file_path: str) -> int:
    """Get the size of a file in bytes.

    Parameters
    ----------
    file_path : str
        Path to the file

    Returns
    -------
    int
        File size in bytes

    """
    return Path(file_path).stat().st_size


def load_data_file(file_path: str, separator: str | None = None) -> pl.DataFrame:
    """Load data from CSV or Parquet file.

    Parameters
    ----------
    file_path : str
        Path to the input file
    separator : str, optional
        CSV separator character. If None, will be auto-detected for CSV files

    Returns
    -------
    pl.DataFrame
        Loaded data as Polars DataFrame

    Raises
    ------
    ValueError
        If file format is not supported

    """
    file_extension = get_file_extension(file_path)

    if file_extension == '.csv':
        if separator is None:
            separator = detect_separator(file_path)
        return pl.read_csv(file_path, separator=separator)

    if file_extension == '.parquet':
        return pl.read_parquet(file_path)

    supported_formats = ['.csv', '.parquet']
    msg = f'Unsupported file format: {file_extension}. Supported: {supported_formats}'
    raise ValueError(msg)


def save_data_file(
    df: pl.DataFrame,
    file_path: str,
    output_extension: str = '.parquet',
) -> None:
    """Save DataFrame to file in specified format.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to save
    file_path : str
        Output file path (without extension)
    output_extension : str, default '.parquet'
        Output file format ('.csv' or '.parquet')

    """
    if output_extension == '.csv':
        df.write_csv(f'{file_path}.csv')
    else:
        df.write_parquet(f'{file_path}.parquet')


def load_pseudonym_key(key_file_path: str) -> dict[str, str]:
    """Load pseudonym key from JSON file.

    Parameters
    ----------
    key_file_path : str
        Path to the JSON key file

    Returns
    -------
    dict[str, str]
        Dictionary mapping original names to pseudonyms

    """
    file_path = Path(key_file_path)
    with file_path.open(encoding='utf-8') as file:
        return json.load(file)


def save_pseudonym_key(
    pseudonym_key: dict[str, str],
    output_folder: str,
    filename: str = 'pseudonym_key.json',
) -> None:
    """Save pseudonym key to JSON file.

    Parameters
    ----------
    pseudonym_key : dict[str, str]
        Dictionary mapping original names to pseudonyms
    output_folder : str
        Output folder path
    filename : str, default 'pseudonym_key.json'
        Output filename

    """
    output_path = Path(output_folder) / filename
    with output_path.open('w', encoding='utf-8') as outfile:
        json.dump(pseudonym_key, outfile, indent=2)


def create_output_file_path(
    output_folder: str,
    input_filename: str,
    suffix: str = '',
) -> str:
    """Create output file path based on input filename.

    Parameters
    ----------
    output_folder : str
        Output folder path
    input_filename : str
        Original input filename
    suffix : str, default ''
        Optional suffix to add to filename (e.g., '_with_nulls')

    Returns
    -------
    str
        Output file path without extension

    """
    input_path = Path(input_filename)
    base_name = input_path.stem
    if suffix:
        base_name = f'{base_name}{suffix}'
    return str(Path(output_folder) / base_name)


def ensure_directory_exists(directory_path: str) -> None:
    """Ensure that a directory exists, creating it if necessary.

    Parameters
    ----------
    directory_path : str
        Path to the directory

    """
    Path(directory_path).mkdir(parents=True, exist_ok=True)
