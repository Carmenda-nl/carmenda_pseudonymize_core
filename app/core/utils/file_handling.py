# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
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

import polars as pl
from charset_normalizer import from_bytes
from lxml.etree import ParserError
from lxml.html import fromstring

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
        base_path = Path(sys._MEIPASS)
        input_folder = str(base_path / 'data' / 'input')
        output_folder = str(base_path / 'data' / 'output')
    else:
        # Script environment
        input_folder = 'data/input'
        output_folder = 'data/output'

    Path(output_folder).mkdir(parents=True, exist_ok=True)
    return input_folder, output_folder


def check_file(input_file: str) -> tuple[str, str, str]:
    """Determine the encoding, line ending and separator."""
    file_path = Path(input_file)
    filename = file_path.name

    with file_path.open('rb') as rawdata:
        data_sample = rawdata.read(10240)

        try:
            detection = from_bytes(data_sample)
            best = detection.best()
            encoding = best.encoding if best and getattr(best, 'encoding', None) else 'utf-8'
        except (LookupError, ValueError, TypeError):
            logger.debug('Encoding detection failed, using UTF-8')
            encoding = 'utf-8'

        # Detect line endings - Polars only supports single-byte eol_char
        if b'\r\n' in data_sample:
            line_ending = '\n'
        elif b'\r' in data_sample:
            line_ending = '\r'
        elif b'\n' in data_sample:
            line_ending = '\n'
        else:
            line_ending = '\n'

        header = ''

        try:
            rawdata.seek(0)
            header_bytes = rawdata.readline()
            header = header_bytes.decode(encoding).strip()
        except (UnicodeDecodeError, LookupError):
            logger.warning('File cannot be decoded with encoding "%s"', encoding)

    if not header:
        logger.error('File is empty or has no header.')

    candidates = [',', ';', '\t', '|']
    scores = {sep: header.count(sep) for sep in candidates}
    separator = max(scores, key=scores.get)

    # Ensure we found at least one separator
    if scores[separator] == 0:
        logger.error('No valid separator found. Tried: %s', candidates)

    logger.info('Checked %s: Encoding=%s, line endings=%r, separator=%r', filename, encoding, line_ending, separator)
    return encoding, line_ending, separator


def clean_html(decoded_line: str) -> str:
    """Remove HTML tags from input file."""
    line = decoded_line.strip()

    # Skip empty lines
    if not line:
        return decoded_line

    tree = fromstring(line)
    return tree.text_content()


def load_data_file(input_file_path: str, output_folder: str) -> pl.DataFrame | None:
    """Check data file, log and return as a Polars DataFrame."""
    file_path = Path(input_file_path)
    if not file_path.is_file():
        return None

    input_extension = file_path.suffix
    file_size = file_path.stat().st_size
    logger.info('%s file of size: %s', input_extension, file_size)

    encoding, line_ending, separator = check_file(input_file_path)

    def process_line(line: bytes) -> tuple[bytes | None, str | None]:
        """Process a single line: decode, clean HTML, re-encode."""
        try:
            decoded_line = line.decode(encoding)
            cleaned_line = clean_html(decoded_line)
            return cleaned_line.encode('utf-8'), None
        except (LookupError, UnicodeDecodeError, TypeError, ParserError):
            return None, str(line)

    error_count = 0
    with (
        tempfile.NamedTemporaryFile('w+', encoding='utf-8', delete=False) as error_temp,
        tempfile.NamedTemporaryFile('wb', delete=False) as utf8_temp,
        file_path.open('rb') as rawdata,
    ):
        error_writer = csv.writer(error_temp)
        for line in rawdata:
            cleaned_data, error_data = process_line(line)

            if cleaned_data is not None:
                utf8_temp.write(cleaned_data)
            else:
                error_writer.writerow([error_data])
                error_count += 1

    # Create a Polars DataFrame from the UTF-8 encoded temporary file
    df = pl.read_csv(utf8_temp.name, separator=separator, eol_char=line_ending, use_pyarrow=True)

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


def load_datakey(datakey_path: str) -> pl.DataFrame:
    """Grab valid names from file and return as a Polars DataFrame."""
    encoding, line_ending, separator = check_file(datakey_path)
    accepted_encodings = ('utf-8', 'ascii', 'cp1252', 'windows-1252', 'ISO-8859-1', 'latin1')

    if encoding not in accepted_encodings:
        logger.warning('Datakey encoding not supported, provided: %s.', encoding)
        return None

    df = pl.read_csv(datakey_path, encoding=encoding, eol_char=line_ending, separator=separator)
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
