# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .logger import setup_logging

if TYPE_CHECKING:
    from core.utils.progress_tracker import ProgressTracker

logger = setup_logging()


def load_datafile(input_file: str, tracker: ProgressTracker) -> pl.DataFrame | None:
    """Load datafile and return as a DataFrame."""
    file_path = Path(input_file)
    if not file_path.is_file():
        return None

    input_extension = file_path.suffix
    file_size = file_path.stat().st_size
    logger.info('%s file of size: %s bytes', input_extension, file_size)

    if input_extension.lower() == '.csv':
        df = pl.read_csv(input_file, encoding='utf-8', separator=',')
    elif input_extension.lower() in ('.xls', '.xlsx'):
        df = pl.read_excel(source=input_file, raise_if_empty=False)
    else:
        logger.error('Unsupported file type: %s', input_extension)
        return None

    tracker.set_progress('file_loaded')
    return df


def save_datafile(df: pl.DataFrame, filename: str, output_folder: str) -> str | None:
    """Save processed DataFrame to file in the specified output folder."""
    filepath = Path(filename)
    stem = filepath.stem
    target_dir = Path(output_folder)

    try:
        target_dir.mkdir(parents=True, exist_ok=True)

        input_extension = filepath.suffix
        filepath = target_dir / f'{stem}_pseudonymised{input_extension}'
        if input_extension.lower() == '.csv':
            df.write_csv(str(filepath))
        elif input_extension.lower() in ('.xls', '.xlsx'):
            df.write_excel(str(filepath))
        return str(filepath)
    except OSError:
        logger.warning('Cannot write %s to "%s".', filename, target_dir)
        return None


def load_datakey(datakey_path: str) -> pl.DataFrame | None:
    """Grab valid names from file and return as a Polars DataFrame."""
    df = pl.read_csv(datakey_path, encoding='utf-8', separator=',', eol_char='\n')
    df = df.rename({'Clientnaam': 'clientname', 'Synoniemen': 'synonyms', 'Code': 'code'})
    return df.with_columns(pl.col('clientname').str.strip_chars()).filter(pl.col('clientname') != '')


def save_datakey(datakey: pl.DataFrame, filename: str, output_folder: str, key_name: str | None = None) -> str | None:
    """Save the processed datakey to a CSV file for future use."""
    filepath = Path(filename)
    output_filename = key_name or f'{filepath.stem}_key.csv'

    target_dir = Path(output_folder)
    file_path = target_dir / output_filename

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        datakey = datakey.rename({'clientname': 'Clientnaam', 'synonyms': 'Synoniemen', 'code': 'Code'})
        datakey.write_csv(file_path, separator=',')
        logger.debug('Saving datakey: %s\n%s\n', output_filename, datakey)
        return str(file_path)
    except OSError:
        logger.warning('Cannot write datakey to "%s".', file_path)
        return None
