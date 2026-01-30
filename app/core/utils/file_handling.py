# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""File utilities for data processing operations."""

from __future__ import annotations

import csv
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd
import polars as pl
from charset_normalizer import from_path

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


def strip_bom(text: str) -> str:
    """Remove BOM (Byte Order Mark) from the header if present in UTF-8 encoding."""
    return text.removeprefix('\ufeff')


def _detect_encoding(file_path: Path) -> str:
    """Detect file encoding, defaults to UTF-8, ascii is treated as UTF-8."""
    try:
        detection = from_path(
            file_path,
            steps=6,
            chunk_size=2 * 1024 * 1024,
            threshold=0.2,
            preemptive_behaviour=True,
            language_threshold=0.3,
            enable_fallback=False,
        )
        best = detection.best()
        encoding = best.encoding if best and getattr(best, 'encoding', None) else 'utf-8'

        if encoding.lower() == 'ascii':
            encoding = 'utf-8'

    except (LookupError, ValueError, TypeError, OSError):
        logger.warning('Encoding detection failed, defaults to UTF-8 encoding.')
        encoding = 'utf-8'

    # Normalize encoding name (utf_8 -> utf-8) for consistency
    return encoding.replace('_', '-')


def _detect_separator(sample: str) -> str:
    """Detect CSV separator from a sample string."""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
    except csv.Error:
        candidates = [',', ';', '\t', '|']
        scores = {sep: sample.count(sep) for sep in candidates}
        separator = max(scores, key=scores.__getitem__)

        if scores[separator] == 0:
            logger.warning('No valid separator found. Tried: %s', candidates)
            return ','

        return separator
    else:
        return dialect.delimiter


def check_file(input_file: str) -> tuple[str, str]:
    """Intergration function to check file encoding, separator and strip BOM."""
    file_path = Path(input_file)
    filename = file_path.name

    encoding = _detect_encoding(file_path)

    try:
        with file_path.open('r', encoding=encoding, errors='replace') as file:
            header = strip_bom(file.readline().strip())
            if not header:
                logger.error('File is empty or has no header.')
    except OSError:
        header = ''
        logger.exception('File cannot be opened: "%s"', file_path)

    separator = _detect_separator(header)

    logger.info('Checked %s: Encoding=%s, separator=%r', filename, encoding, separator)
    return encoding, separator


def _replace_html(text: str, html_tags: str = 'encode') -> str:
    """Replace HTML entities with placeholders or vice versa. (encode/decode)."""
    replacements = {
        '&quot;': '___QUOT___',
        '&nbsp;': '___NBSP___',
        '&#39;': '___APOS___',
        '&euml;': '___EUML___',
        '&lt;': '___LT___',
        '&gt;': '___GT___',
        '&amp;': '___AMP___',
    }

    if html_tags == 'encode':
        for entity, placeholder in replacements.items():
            text = text.replace(entity, placeholder)

        text = text.replace('\n', '___NEWLINE___').replace('\r', '')
    else:
        for entity, placeholder in replacements.items():
            text = text.replace(placeholder, entity)

        text = text.replace('___NEWLINE___', '\n')
    return text


def load_data_file(input_file_path: str, output_folder: str) -> pl.DataFrame | None:
    """Check data file, log and return as a Polars DataFrame."""
    file_path = Path(input_file_path)
    if not file_path.is_file():
        return None

    input_extension = file_path.suffix
    file_size = file_path.stat().st_size
    logger.info('%s file of size: %s', input_extension, file_size)

    encoding, separator = check_file(input_file_path)

    """
    Open 3 files:
        1. rawdata:    A temp text file to read the original data with the detected encoding.
        2. utf8_temp:  A temp binary file to rewrite UTF-8 encoded data.
        3. error_temp: A temp text file to log rows with errors.
    """
    error_count = 0
    with (
        file_path.open('r', encoding=encoding, errors='replace', newline='') as rawdata,
        tempfile.NamedTemporaryFile('wb', delete=False) as utf8_temp,
        tempfile.NamedTemporaryFile('w+', encoding='utf-8', delete=False) as error_temp,
    ):
        read_rawdata = csv.reader(rawdata, delimiter=separator)
        error_writer = csv.writer(error_temp, delimiter=separator)

        for row in read_rawdata:
            has_decode_errors = any('\ufffd' in (field or '') for field in row)  # detect � character
            if has_decode_errors:
                error_count += 1
                error_writer.writerow(row)
                continue

            # Execute if separator is semicolon and row contains HTML that could confuse parser
            if separator == ';':
                processed_row = [
                    _replace_html(field, html_tags='encode') if field and ('&' in field or '\n' in field) else field
                    for field in row
                ]
            else:
                processed_row = row

            # Write to temp file as CSV line
            line = separator.join(processed_row) + '\n'
            utf8_temp.write(line.encode('utf-8'))

    """
    Create a Polars DataFrame from the `utf8_temp` file.
    If Polars fails due to malformed data, fallback to using pandas to read
    the CSV and then convert it to a Polars DataFrame.
    """
    try:
        df = pl.read_csv(source=utf8_temp.name, separator=separator, eol_char='\n', quote_char=None)
    except (pl.exceptions.ComputeError, pl.exceptions.InvalidOperationError) as error:
        logger.warning('Failed to read file: %s.\nFalling back to slower method to try again.', error)

        try:
            df_pandas = pd.read_csv(
                utf8_temp.name,
                sep=separator,
                lineterminator='\n',
                quoting=csv.QUOTE_NONE,
                encoding='utf-8',
            )
            df = pl.from_pandas(df_pandas)
        except (pd.errors.ParserError, pd.errors.EmptyDataError, ValueError):
            logger.exception('Failed to read CSV into model.')
            Path(utf8_temp.name).unlink(missing_ok=True)
            Path(error_temp.name).unlink(missing_ok=True)
            return None

    # Restore HTML entities that were temporarily replaced
    for col in df.columns:
        # Only process string columns to avoid AttributeError on numeric types.
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).map_elements(lambda text: _replace_html(text, html_tags='decode'), return_dtype=pl.Utf8),
            )

    if error_count > 0:
        parent = file_path.parent.relative_to('data/input')
        target_dir = Path(output_folder) / parent if str(parent) and str(parent) != '.' else Path(output_folder)
        error_csv = target_dir / f'{file_path.stem}_errors.csv'
        shutil.move(error_temp.name, error_csv)
        logger.warning('Found %s rows with errors that are written to: %s\n', error_count, error_csv)
    else:
        Path(error_temp.name).unlink()
        logger.info('No errors in rows found.')

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
        filepath = target_dir / f'{stem}_deidentified.csv'

        df.write_csv(str(filepath))
    except OSError:
        logger.warning('Cannot write %s to "%s".', filename, target_dir)


def load_datakey(datakey_path: str) -> pl.DataFrame | None:
    """Grab valid names from file and return as a Polars DataFrame."""
    encoding, separator = check_file(datakey_path)
    accepted_encodings = ('utf-8', 'ascii', 'cp1252', 'windows-1252', 'ISO-8859-1', 'latin1')

    if encoding not in accepted_encodings:
        logger.warning('Datakey encoding not supported, provided: %s.', encoding)
        return None

    df = pl.read_csv(datakey_path, encoding=encoding, eol_char='\n', separator=separator)
    df = df.rename({'Clientnaam': 'clientname', 'Synoniemen': 'synonyms', 'Code': 'code'})

    return df.with_columns(pl.col('clientname').str.strip_chars()).filter(pl.col('clientname') != '')


def save_datakey(datakey: pl.DataFrame, filename: str, output_folder: str, datakey_name: str | None = None) -> None:
    """Save the processed datakey to a CSV file for future use."""
    filepath = Path(filename)
    parent = filepath.parent

    output_filename = datakey_name if datakey_name else 'datakey.csv'

    # If filename included a parent (like job_id), write into that subfolder under output.
    target_dir = Path(output_folder) / parent if str(parent) and str(parent) != '.' else Path(output_folder)
    file_path = target_dir / output_filename

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        datakey = datakey.rename({'clientname': 'Clientnaam', 'synonyms': 'Synoniemen', 'code': 'Code'})
        datakey.write_csv(file_path, separator=';')
        logger.debug('Saving datakey: %s\n%s\n', output_filename, datakey)
    except OSError:
        logger.warning('Cannot write datakey to "%s".', file_path)
