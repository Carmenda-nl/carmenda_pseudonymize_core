# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Tests for CSV handling utilities."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

import pytest

from core.utils.csv_handler import (
    _detect_delimiter,
    _detect_encoding,
    detect_csv_properties,
    normalize_csv,
    sanitize_csv,
    strip_bom,
)

# -------------------------------- STRIP BOM TESTS -------------------------------- #


class TestStripBom:
    """Test BOM stripping from strings."""

    @pytest.mark.parametrize(
        ('input_text', 'expected'),
        [
            ('\ufeffPatientname, Report', 'Patientname, Report'),
            ('Patientname, Report', 'Patientname, Report'),
            ('\ufeff', ''),
            ('', ''),
        ],
    )
    def test_strip_bom(self, input_text: str, expected: str) -> None:
        """BOM is stripped when present, unchanged otherwise."""
        assert strip_bom(input_text) == expected


# ----------------------------- DETECT ENCODING TESTS ----------------------------- #


class TestDetectEncoding:
    """Tests for encoding detection."""

    def test_utf8_file(self, tmp_path: Path) -> None:
        """UTF-8 file is correctly detected."""
        file = tmp_path / 'test.txt'
        file.write_text('Héllo wörld €', encoding='utf-8')

        with file.open('rb') as f:
            data_sample = f.read()

        assert _detect_encoding(data_sample) == 'utf-8'

    def test_ascii_returns_utf8(self, tmp_path: Path) -> None:
        """ASCII is converted to UTF-8 (superset)."""
        file = tmp_path / 'test.txt'
        file.write_text('Hello world 123', encoding='ascii')

        with file.open('rb') as f:
            data_sample = f.read()

        assert _detect_encoding(data_sample) == 'utf-8'

    def test_windows1252_file(self, tmp_path: Path) -> None:
        """Windows-1252 or similar Latin encoding is detected."""
        file = tmp_path / 'test.txt'
        file.write_bytes(b'Caf\xe9 na\xefve r\xe9sum\xe9')

        with file.open('rb') as f:
            data_sample = f.read()

        result = _detect_encoding(data_sample)
        latin_encodings = ('cp1250', 'cp1252', 'windows-1252', 'iso-8859-1', 'iso-8859-2', 'latin-1')
        assert result in latin_encodings or result.startswith('iso-8859')

    @pytest.mark.parametrize('content', [b''])
    def test_returns_utf8_default(self, tmp_path: Path, content: bytes) -> None:
        """Empty data returns UTF-8 default."""
        file = tmp_path / 'test.txt'
        file.write_bytes(content)

        with file.open('rb') as f:
            data_sample = f.read()

        assert _detect_encoding(data_sample) == 'utf-8'

    def test_utf8_with_bom(self, tmp_path: Path) -> None:
        """UTF-8 file with BOM is detected correctly."""
        file = tmp_path / 'test.txt'
        file.write_bytes(b'\xef\xbb\xbfHello world')

        with file.open('rb') as f:
            data_sample = f.read()

        assert _detect_encoding(data_sample) == 'utf-8'


# ---------------------------- DETECT DELIMITER TESTS ----------------------------- #


class TestDetectDelimiter:
    """Tests delimiter detection from sample strings."""

    @pytest.mark.parametrize(
        ('sample', 'expected'),
        [
            ('name,age,city\nJohn,30,Amsterdam', ','),
            ('name;age;city\nJohn;30;Amsterdam', ';'),
            ('name\tage\tcity\nJohn\t30\tAmsterdam', '\t'),
            ('name|age|city\nJohn|30|Amsterdam', '|'),
            ('', ','),  # Empty returns default
            ('just some text without delimiters', ','),  # No delimiter returns default
            ('a;b;c;d\n1;2;3;4', ';'),
            ('a]b]c;d;e;f', ';'),  # Sniffer fallback
        ],
    )
    def test_delimiter_detection(self, sample: str, expected: str) -> None:
        """Delimiter is correctly detected from sample."""
        assert _detect_delimiter(sample) == expected

    def test_sniffer_fails_uses_counting(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When Sniffer fails, fallback to counting delimiters."""

        def mock_sniff(*_args: object, **_kwargs: object) -> None:
            msg = 'Sniffer failed'
            raise csv.Error(msg)

        monkeypatch.setattr(csv.Sniffer, 'sniff', mock_sniff)

        assert _detect_delimiter('col1;col2;col3\nval1;val2;val3') == ';'


# ---------------------------- DETECT PROPERTIES TESTS ---------------------------- #


class TestDetectProperties:
    """Integration tests for detect_csv_properties function."""

    @pytest.mark.parametrize(
        ('content', 'expected_sep'),
        [
            ('name,age,city\nJohn,30,Amsterdam', ','),
            ('naam;leeftijd;stad\nJan;30;Amsterdam', ';'),
            ('name\tage\tcity\nJohn\t30\tAmsterdam', '\t'),
            ('name|age|city\nJohn|30|Amsterdam', '|'),
        ],
    )
    def test_delimiter_detection(self, tmp_path: Path, content: str, expected_sep: str) -> None:
        """Different delimiters are correctly detected."""
        file = tmp_path / 'test.csv'
        file.write_text(content, encoding='utf-8')

        properties = detect_csv_properties(file)

        assert properties['encoding'] == 'utf-8'
        assert properties['delimiter'] == expected_sep
        assert properties['header'] == content.split('\n', 1)[0]

    def test_utf8_with_bom(self, tmp_path: Path) -> None:
        """UTF-8 file with BOM is correctly detected and BOM stripped from header."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'\xef\xbb\xbfname,age\nJohn,30')

        properties = detect_csv_properties(file)

        assert properties['encoding'] == 'utf-8'
        assert properties['delimiter'] == ','
        assert properties['header'] == 'name,age'  # BOM should be stripped

    def test_windows1252_encoding(self, tmp_path: Path) -> None:
        """Windows-1252 encoded file is detected as Latin encoding."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'naam;stad\nCaf\xe9;Br\xfcssel')

        properties = detect_csv_properties(file)

        latin_encodings = ('cp1250', 'cp1252', 'windows-1252', 'iso-8859-1', 'iso-8859-2', 'latin-1')
        assert properties['encoding'] in latin_encodings or properties['encoding'].startswith('iso-8859')
        assert properties['delimiter'] == ';'


# ----------------------------- SANITIZE CSV TESTS ----------------------------- #


class TestSanitizeCsv:
    """Tests for sanitize_csv function."""

    def test_converts_to_utf8(self, tmp_path: Path) -> None:
        """CSV is converted to UTF-8."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'naam;stad\nCaf\xe9;Amsterdam')

        properties = {
            'encoding': 'cp1252',
            'delimiter': ';',
            'header': 'naam;stad',
        }

        output_folder = tmp_path / 'output'
        output_folder.mkdir()

        sanitized_path = sanitize_csv(file, properties, str(output_folder))
        sanitized_file = Path(sanitized_path)

        assert sanitized_file.exists()
        content = sanitized_file.read_text(encoding='utf-8')
        assert 'Café' in content

    def test_replaces_html_entities_with_semicolon(self, tmp_path: Path) -> None:
        """HTML entities are replaced when delimiter is semicolon."""
        file = tmp_path / 'test.csv'
        file.write_text('name;text\nTest;fish &amp; chips', encoding='utf-8')

        properties = {
            'encoding': 'utf-8',
            'delimiter': ';',
            'header': 'name;text',
        }

        output_folder = tmp_path / 'output'
        output_folder.mkdir()

        sanitized_path = sanitize_csv(file, properties, str(output_folder))
        content = Path(sanitized_path).read_text(encoding='utf-8')

        assert 'fish & chips' in content
        assert '&amp;' not in content

    def test_preserves_html_with_comma(self, tmp_path: Path) -> None:
        """HTML entities are NOT replaced when delimiter is comma."""
        file = tmp_path / 'test.csv'
        file.write_text('name,text\nTest,fish &amp; chips', encoding='utf-8')

        properties = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'header': 'name,text',
        }

        output_folder = tmp_path / 'output'
        output_folder.mkdir()

        sanitized_path = sanitize_csv(file, properties, str(output_folder))
        content = Path(sanitized_path).read_text(encoding='utf-8')

        # With comma delimiter, HTML should NOT be unescaped
        assert 'fish &amp; chips' in content


# ----------------------------- NORMALIZE CSV TESTS ----------------------------- #


class TestNormalizeCsv:
    """Tests for normalize_csv function."""

    def test_converts_delimiter_to_comma(self, tmp_path: Path) -> None:
        """Semicolon delimiter is converted to comma."""
        file = tmp_path / 'test.csv'
        file.write_text('name;age\nAlice;30\nBob;25', encoding='utf-8')

        properties = {
            'encoding': 'utf-8',
            'delimiter': ';',
            'header': 'name;age',
        }

        normalized_path = normalize_csv(file, properties)
        content = Path(normalized_path).read_text(encoding='utf-8')

        assert 'name,age' in content
        assert 'Alice,30' in content
        assert ';' not in content

    def test_removes_empty_rows(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Empty rows are removed and warning is logged."""
        file = tmp_path / 'test.csv'
        file.write_text('name,age\nAlice,30\n\n  \nBob,25\n,\n', encoding='utf-8')

        properties = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'header': 'name,age',
        }

        with caplog.at_level(logging.WARNING):
            normalized_path = normalize_csv(file, properties)

        content = Path(normalized_path).read_text(encoding='utf-8')
        lines = [line for line in content.split('\n') if line.strip()]

        # Should have header + 2 data rows
        assert len(lines) == 3  # noqa: PLR2004
        assert 'empty rows' in caplog.text

    def test_preserves_valid_rows(self, tmp_path: Path) -> None:
        """Valid rows are preserved correctly."""
        file = tmp_path / 'test.csv'
        file.write_text('name,age,city\nAlice,30,Amsterdam\nBob,25,Rotterdam', encoding='utf-8')

        properties = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'header': 'name,age,city',
        }

        normalized_path = normalize_csv(file, properties)
        content = Path(normalized_path).read_text(encoding='utf-8')

        assert 'Alice,30,Amsterdam' in content
        assert 'Bob,25,Rotterdam' in content
