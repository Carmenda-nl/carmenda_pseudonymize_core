# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""CSV file loading, conversion and sanitization utilities."""

from __future__ import annotations

import csv
import html
import shutil
import tempfile
from pathlib import Path

from charset_normalizer import from_bytes

from core.utils.logger import setup_logging

logger = setup_logging()


def strip_bom(text: str) -> str:
    """Remove BOM (Byte Order Mark) from the header if present in UTF-8 encoding."""
    return text.removeprefix('\ufeff')


def _detect_encoding(data_sample: bytes) -> str:
    """Detect CSV file encoding, defaults to UTF-8."""
    if data_sample.startswith(b'\xef\xbb\xbf'):
        return 'utf-8'  # <- UTF-8 BOM detected

    try:
        results = from_bytes(data_sample)
        best_match = results.best()
        encoding = best_match.encoding if best_match and getattr(best_match, 'encoding', None) else 'utf-8'

        # ascii detection is treated as UTF-8
        if encoding.lower() == 'ascii':
            encoding = 'utf-8'

    except (LookupError, ValueError, TypeError, OSError):
        logger.warning('Encoding detection failed, defaults to UTF-8 encoding.')
        encoding = 'utf-8'

    # Normalize encoding name (utf_8 -> utf-8) for consistency
    return encoding.replace('_', '-')


def _detect_delimiter(data_sample: str) -> str:
    """Detect CSV delimiter from sample data."""
    try:
        dialect = csv.Sniffer().sniff(data_sample, delimiters=',;\t|')
    except csv.Error:
        candidates = [',', ';', '\t', '|']
        scores = {delimiter: data_sample.count(delimiter) for delimiter in candidates}
        detected_delimiter = max(scores, key=scores.__getitem__)
        return detected_delimiter if scores[detected_delimiter] > 0 else ','
    else:
        return dialect.delimiter


def detect_csv_properties(file_path: Path) -> dict[str, str]:
    """Detect the CSV encoding, delimiter and header in a CSV file."""
    with file_path.open('rb') as rawdata:
        data_sample = rawdata.read(2 * 1024 * 1024)

    encoding = _detect_encoding(data_sample)

    with file_path.open('r', encoding=encoding, errors='ignore') as file:
        header = strip_bom(next(file).strip())

    delimiter = _detect_delimiter(header)

    logger.info('Detected %s: Encoding=%s, delimiter=%r', file_path.name, encoding, delimiter)

    return {
        'header': header,
        'encoding': encoding,
        'delimiter': delimiter,
    }


def _collect_errors(file_path: Path, error_temp: str, output_folder: str) -> None:
    """Collect encoding errors and write them to a separate CSV file."""
    error_file = Path(error_temp)
    error_count = sum(1 for _ in error_file.open(encoding='utf-8')) - 1

    if error_count > 0:
        try:
            input_base = Path(output_folder).parent / 'input'
            parent = file_path.parent.relative_to(input_base)
            target_dir = Path(output_folder) / parent
        except ValueError:
            target_dir = Path(output_folder)

        target_dir.mkdir(parents=True, exist_ok=True)
        error_csv = target_dir / f'{file_path.stem}_errors.csv'
        shutil.move(error_temp, error_csv)
        logger.warning('%d errors in rows found.', error_count)
    else:
        error_file.unlink()
        logger.info('No errors in rows found.')


def sanitize_csv(file_path: Path, properties: dict[str, str], output_folder: str) -> str:
    """Convert CSV to UTF-8 and sanitize content.

    When the delimiter is `;` replace &quot; with space and convert HTML character entities
    or Polars gets confused and treats it as delimiters,
    which can lead to parsing errors.
    """
    header, encoding, delimiter = properties['header'], properties['encoding'], properties['delimiter']
    replace_html = delimiter == ';'

    with (
        file_path.open('rb') as file,
        tempfile.NamedTemporaryFile('w', encoding='utf-8', newline='', delete=False) as temp_file,
        tempfile.NamedTemporaryFile('w', encoding='utf-8', newline='', delete=False, suffix='.csv') as error_temp,
    ):
        chunk_size = 8 * 1024 * 1024
        buffer = b''

        # Add a header to the error file
        error_temp.write(header.replace(delimiter, ',') + '\n')

        while True:
            chunk = file.read(chunk_size)
            if not chunk and not buffer:
                break

            buffer += chunk

            try:
                text = buffer.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                for raw_line in buffer.split(b'\n'):
                    try:
                        # handle errors line by line if the chunk contains invalid sequences
                        line = raw_line.decode(encoding)
                    except (UnicodeDecodeError, LookupError):
                        error_line = raw_line.decode(encoding, errors='replace')
                        error_line = error_line.replace(delimiter, ',')
                        error_temp.write(error_line + '\n')
                        continue

                    if replace_html:
                        line = line.replace('&quot;', ' ')
                        line = html.unescape(line)
                    temp_file.write(line + '\n')
                buffer = b''
                continue

            # Check if the buffer is safe to flush (even number of quotes = not inside a quoted field)
            quote_count = text.count('"') - text.count('\\"')
            if not chunk or quote_count % 2 == 0:
                if replace_html:
                    text = text.replace('&quot;', ' ')
                    text = html.unescape(text)
                temp_file.write(text)
                buffer = b''

    _collect_errors(file_path, error_temp.name, output_folder)

    return temp_file.name


def normalize_csv(file_path: Path, properties: dict[str, str]) -> str:
    """Convert delimiter to comma and remove empty rows."""
    with (
        file_path.open('r', encoding='utf-8', newline='') as file,
        tempfile.NamedTemporaryFile('w', encoding='utf-8', newline='', delete=False) as csv_temp,
    ):
        delimiter = properties['delimiter']
        reader = csv.reader(file, delimiter=delimiter)
        writer = csv.writer(csv_temp, delimiter=',')

        empty_rows = 0

        for row in reader:
            if any(field.strip() for field in row):
                writer.writerow(row)
            else:
                empty_rows += 1

        if empty_rows > 0:
            logger.warning('Found %d empty rows.\n', empty_rows)

    return csv_temp.name
