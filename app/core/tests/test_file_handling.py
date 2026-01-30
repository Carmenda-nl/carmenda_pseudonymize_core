# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Tests for file handling utilities."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import polars as pl
import pytest

from core.utils.file_handling import (
    _detect_encoding,
    _detect_separator,
    _replace_html,
    check_file,
    get_environment,
    load_data_file,
    load_datakey,
    save_datafile,
    save_datakey,
    strip_bom,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from io import IOBase


# ----------------------------------- FIXTURES ------------------------------------ #


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Create standard test CSV file structure."""
    csv_path = tmp_path / 'data' / 'input' / 'test.csv'
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return csv_path


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Create output directory."""
    out = tmp_path / 'output'
    out.mkdir(parents=True, exist_ok=True)
    return out


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

        assert _detect_encoding(file) == 'utf-8'

    def test_ascii_returns_utf8(self, tmp_path: Path) -> None:
        """ASCII is converted to UTF-8 (superset)."""
        file = tmp_path / 'test.txt'
        file.write_text('Hello world 123', encoding='ascii')

        assert _detect_encoding(file) == 'utf-8'

    def test_windows1252_file(self, tmp_path: Path) -> None:
        """Windows-1252 or similar Latin encoding is detected."""
        file = tmp_path / 'test.txt'
        file.write_bytes(b'Caf\xe9 na\xefve r\xe9sum\xe9')

        result = _detect_encoding(file)
        latin_encodings = ('cp1250', 'cp1252', 'windows-1252', 'iso-8859-1', 'iso-8859-2', 'latin-1')
        assert result in latin_encodings or result.startswith('iso-8859')

    @pytest.mark.parametrize('content', [None, b''])
    def test_returns_utf8_default(self, tmp_path: Path, content: bytes | None) -> None:
        """Non-existent or empty file returns UTF-8 default."""
        file = tmp_path / 'test.txt'
        if content is not None:
            file.write_bytes(content)

        assert _detect_encoding(file) == 'utf-8'

    def test_utf8_with_bom(self, tmp_path: Path) -> None:
        """UTF-8 file with BOM is detected correctly."""
        file = tmp_path / 'test.txt'
        file.write_bytes(b'\xef\xbb\xbfHello world')

        assert _detect_encoding(file) in ('utf-8', 'utf-8-sig')


# ---------------------------- DETECT SEPARATOR TESTS ----------------------------- #


class TestDetectSeparator:
    """Tests separator detection from sample strings."""

    @pytest.mark.parametrize(
        ('sample', 'expected'),
        [
            ('name,age,city\nJohn,30,Amsterdam', ','),
            ('name;age;city\nJohn;30;Amsterdam', ';'),
            ('name\tage\tcity\nJohn\t30\tAmsterdam', '\t'),
            ('name|age|city\nJohn|30|Amsterdam', '|'),
            ('', ','),  # Empty returns default
            ('just some text without separators', ','),  # No separator returns default
            ('a;b;c;d\n1;2;3;4', ';'),  # Mixed: most consistent wins
            ('a]b]c;d;e;f', ';'),  # Sniffer fallback
        ],
    )
    def test_separator_detection(self, sample: str, expected: str) -> None:
        """Separator is correctly detected from sample."""
        assert _detect_separator(sample) == expected

    def test_sniffer_fails_uses_counting(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When Sniffer fails, fallback to counting separators."""

        def mock_sniff(*_args: object, **_kwargs: object) -> None:
            msg = 'Sniffer failed'
            raise csv.Error(msg)

        monkeypatch.setattr(csv.Sniffer, 'sniff', mock_sniff)

        assert _detect_separator('col1;col2;col3\nval1;val2;val3') == ';'


# -------------------------------- CHECK FILE TESTS -------------------------------- #


class TestCheckFile:
    """Integration tests for check_file function."""

    @pytest.mark.parametrize(
        ('content', 'expected_sep'),
        [
            ('name,age,city\nJohn,30,Amsterdam', ','),
            ('naam;leeftijd;stad\nJan;30;Amsterdam', ';'),
            ('name\tage\tcity\nJohn\t30\tAmsterdam', '\t'),
            ('name|age|city\nJohn|30|Amsterdam', '|'),
        ],
    )
    def test_separator_detection(self, tmp_path: Path, content: str, expected_sep: str) -> None:
        """Different separators are correctly detected."""
        file = tmp_path / 'test.csv'
        file.write_text(content, encoding='utf-8')

        encoding, separator = check_file(str(file))

        assert encoding == 'utf-8'
        assert separator == expected_sep

    def test_utf8_with_bom(self, tmp_path: Path) -> None:
        """UTF-8 file with BOM is correctly detected."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'\xef\xbb\xbfname,age\nJohn,30')

        encoding, separator = check_file(str(file))

        assert encoding in ('utf-8', 'utf-8-sig')
        assert separator == ','

    def test_windows1252_encoding(self, tmp_path: Path) -> None:
        """Windows-1252 encoded file is detected as Latin encoding."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'naam;stad\nCaf\xe9;Br\xfcssel')

        encoding, separator = check_file(str(file))

        latin_encodings = ('cp1250', 'cp1252', 'windows-1252', 'iso-8859-1', 'iso-8859-2', 'latin-1')
        assert encoding in latin_encodings or encoding.startswith('iso-8859')
        assert separator == ';'

    def test_empty_file_returns_defaults(self, tmp_path: Path) -> None:
        """Empty file returns UTF-8 and comma defaults."""
        file = tmp_path / 'test.csv'
        file.write_bytes(b'')

        encoding, separator = check_file(str(file))

        assert encoding == 'utf-8'
        assert separator == ','

    def test_missing_file_returns_defaults(self, tmp_path: Path) -> None:
        """Non-existent file returns UTF-8 and comma defaults."""
        file = tmp_path / 'test.csv'

        encoding, separator = check_file(str(file))

        assert encoding == 'utf-8'
        assert separator == ','

    def test_oserror_returns_defaults(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """OSError when opening file logs error and returns defaults."""
        file = tmp_path / 'test.csv'
        file.write_text('col1,col2\nval1,val2', encoding='utf-8')

        _original: Callable[..., IOBase] = Path.open  # type: ignore[assignment]

        def mock_open(path_self: Path, mode: str = 'r', **kwargs: object) -> IOBase:
            if path_self.name == 'test.csv':
                msg = 'Permission denied'
                raise OSError(msg)
            return _original(path_self, mode, **kwargs)  # pragma: no cover

        monkeypatch.setattr(Path, 'open', mock_open)

        with caplog.at_level(logging.ERROR):
            _encoding, separator = check_file(str(file))

        assert 'File cannot be opened' in caplog.text
        assert separator == ','


# ------------------------------- REPLACE HTML TESTS ------------------------------- #


class TestReplaceHtml:
    """Tests for _replace_html function."""

    @pytest.mark.parametrize(
        ('input_text', 'expected'),
        [
            ('He said &quot;hello&quot;', 'He said ___QUOT___hello___QUOT___'),
            ('word&nbsp;word', 'word___NBSP___word'),
            ('it&#39;s', 'it___APOS___s'),
            ('na&euml;ve', 'na___EUML___ve'),
            ('&lt;tag&gt;', '___LT___tag___GT___'),
            ('fish &amp; chips', 'fish ___AMP___ chips'),
            ('line1\nline2', 'line1___NEWLINE___line2'),
            ('line1\r\nline2', 'line1___NEWLINE___line2'),
            ('Hello world', 'Hello world'),
            ('', ''),
        ],
    )
    def test_encode(self, input_text: str, expected: str) -> None:
        """HTML entities are encoded to placeholders."""
        assert _replace_html(input_text) == expected

    @pytest.mark.parametrize(
        ('input_text', 'expected'),
        [
            ('___QUOT___hello___QUOT___', '&quot;hello&quot;'),
            ('word___NBSP___word', 'word&nbsp;word'),
            ('line1___NEWLINE___line2', 'line1\nline2'),
            ('Hello world', 'Hello world'),
            ('', ''),
        ],
    )
    def test_decode(self, input_text: str, expected: str) -> None:
        """Placeholders are decoded back to HTML entities."""
        assert _replace_html(input_text, html_tags='decode') == expected

    def test_roundtrip(self) -> None:
        r"""Encode then decode returns original (except \r)."""
        original = 'Text with &quot;quotes&quot; and &amp; symbol\nNew line'
        encoded = _replace_html(original, html_tags='encode')
        decoded = _replace_html(encoded, html_tags='decode')

        assert decoded == original


# ------------------------------ LOAD DATA FILE TESTS ------------------------------ #


class TestLoadDataFile:
    """Tests for load_data_file function."""

    def test_file_not_exists_returns_none(self, tmp_path: Path) -> None:
        """Non-existent file returns None."""
        assert load_data_file(str(tmp_path / 'nonexistent.csv'), str(tmp_path)) is None

    def test_basic_csv_loads_correctly(self, csv_file: Path, output_dir: Path) -> None:
        """Basic CSV file returns a Polars DataFrame with correct structure."""
        csv_file.write_text('name,age,city\nAlice,30,Amsterdam\nBob,25,Rotterdam', encoding='utf-8')

        result = load_data_file(str(csv_file), str(output_dir))

        assert isinstance(result, pl.DataFrame)
        assert result.columns == ['name', 'age', 'city']
        assert len(result) == 2  # noqa: PLR2004

    @pytest.mark.parametrize(
        ('content', 'sep'),
        [
            ('name,age\nAlice,30', ','),
            ('name;age\nAlice;30', ';'),
            ('name\tage\nAlice\t30', '\t'),
        ],
    )
    def test_different_separators(self, csv_file: Path, output_dir: Path, content: str, sep: str) -> None:
        """Different CSV separators are correctly parsed."""
        csv_file.write_text(content, encoding='utf-8')

        result = load_data_file(str(csv_file), str(output_dir))

        assert result is not None
        assert result['name'][0] == 'Alice'

    def test_utf8_with_bom(self, csv_file: Path, output_dir: Path) -> None:
        """UTF-8 file with BOM is correctly read (BOM stripped from header)."""
        csv_file.write_bytes(b'\xef\xbb\xbfname,age\nAlice,30')

        result = load_data_file(str(csv_file), str(output_dir))

        assert result is not None
        assert 'name' in result.columns

    def test_decode_errors_filtered_out(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Rows with replacement character (decode errors) are filtered out."""
        monkeypatch.chdir(tmp_path)

        csv_file = tmp_path / 'data' / 'input' / 'test.csv'
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        output_dir = tmp_path / 'output'
        output_dir.mkdir(parents=True, exist_ok=True)

        content = b'\xef\xbb\xbf'
        content += 'name,age\nAlice,30\nBad\ufffdName,25\nCharlie,35'.encode()
        csv_file.write_bytes(content)

        result = load_data_file('data/input/test.csv', 'output')

        assert result is not None
        assert len(result) == 2  # noqa: PLR2004
        assert 'Alice' in result['name'].to_list()
        assert 'Charlie' in result['name'].to_list()

        # Error file should be created
        assert len(list(output_dir.rglob('*_errors.csv'))) == 1

    def test_html_entities_preserved(self, csv_file: Path, output_dir: Path) -> None:
        """HTML entities in semicolon-separated files are preserved."""
        with csv_file.open('w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['name', 'text'])
            writer.writerow(['Test', 'fish &amp; chips'])

        result = load_data_file(str(csv_file), str(output_dir))

        assert result is not None
        assert result['text'][0] == 'fish &amp; chips'

    def test_pandas_fallback(
        self,
        csv_file: Path,
        output_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When Polars fails, pandas fallback returns valid DataFrame."""
        csv_file.write_text('name,age\nAlice,30\nBob,25', encoding='utf-8')

        def mock_read_csv(*_args: object, **_kwargs: object) -> None:
            msg = 'Forced failure'
            raise pl.exceptions.ComputeError(msg)

        monkeypatch.setattr(pl, 'read_csv', mock_read_csv)

        result = load_data_file(str(csv_file), str(output_dir))

        assert result is not None
        assert isinstance(result, pl.DataFrame)

    def test_both_parsers_fail_returns_none(
        self,
        csv_file: Path,
        output_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When both Polars and Pandas fail, returns None."""
        csv_file.write_text('col1,col2\nval1,val2', encoding='utf-8')

        def mock_pl(*_a: object, **_k: object) -> None:
            msg = 'fail'
            raise pl.exceptions.ComputeError(msg)

        def mock_pd(*_a: object, **_k: object) -> None:
            msg = 'fail'
            raise pd.errors.ParserError(msg)

        monkeypatch.setattr(pl, 'read_csv', mock_pl)
        monkeypatch.setattr(pd, 'read_csv', mock_pd)

        with caplog.at_level(logging.ERROR):
            result = load_data_file(str(csv_file), str(output_dir))

        assert result is None
        assert 'Failed to read CSV into model' in caplog.text


# ------------------------------ SAVE DATAFILE TESTS ------------------------------- #


class TestSaveDatafile:
    """Tests for save_datafile function."""

    def test_saves_with_deidentified_suffix(self, tmp_path: Path) -> None:
        """Output file is created with _deidentified suffix."""
        df = pl.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
        output_folder = tmp_path / 'output'

        save_datafile(df, 'test.csv', str(output_folder))

        saved = output_folder / 'test_deidentified.csv'
        assert saved.exists()

        result = pl.read_csv(saved)
        assert result['name'].to_list() == ['Alice', 'Bob']

    def test_creates_output_folder(self, tmp_path: Path) -> None:
        """Output folder is created if it doesn't exist."""
        df = pl.DataFrame({'name': ['Alice']})
        output_folder = tmp_path / 'new_folder' / 'subfolder'

        assert not output_folder.exists()
        save_datafile(df, 'test.csv', str(output_folder))

        assert (output_folder / 'test_deidentified.csv').exists()

    def test_with_parent_creates_subfolder(self, tmp_path: Path) -> None:
        """Filename with parent path creates subfolder in output."""
        df = pl.DataFrame({'name': ['Alice']})

        save_datafile(df, 'job123/data.csv', str(tmp_path / 'output'))

        assert (tmp_path / 'output' / 'job123' / 'data_deidentified.csv').exists()

    def test_oserror_logs_warning(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """OSError is caught and logged as warning."""
        df = pl.DataFrame({'name': ['Alice']})

        def mock_mkdir(_self: Path, *_args: object, **_kwargs: object) -> None:
            raise OSError

        monkeypatch.setattr(Path, 'mkdir', mock_mkdir)

        with caplog.at_level(logging.WARNING):
            save_datafile(df, 'test.csv', str(tmp_path / 'output'))

        assert 'Cannot write' in caplog.text


# ----------------------------- GET ENVIRONMENT TESTS ------------------------------ #


class TestGetEnvironment:
    """Tests for get_environment function."""

    @pytest.fixture(autouse=True)
    def _mock_mkdir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Mock Path.mkdir for all tests in this class."""
        monkeypatch.setattr(Path, 'mkdir', lambda _self, **_kwargs: None)

    def test_docker_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Docker environment returns Docker-specific paths."""
        monkeypatch.setenv('DOCKER_ENV', 'true')

        input_folder, output_folder = get_environment()

        assert input_folder == '/app/data/input'
        assert output_folder == '/app/data/output'

    def test_script_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Script environment returns relative paths."""
        monkeypatch.delenv('DOCKER_ENV', raising=False)
        monkeypatch.delattr('sys.frozen', raising=False)

        input_folder, output_folder = get_environment()

        assert input_folder == 'data/input'
        assert output_folder == 'data/output'

    def test_pyinstaller_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PyInstaller environment returns _MEIPASS-based paths."""
        monkeypatch.delenv('DOCKER_ENV', raising=False)
        monkeypatch.setattr('sys.frozen', True, raising=False)
        monkeypatch.setattr('sys._MEIPASS', '/fake/meipass', raising=False)

        input_folder, output_folder = get_environment()

        assert input_folder == str(Path('/fake/meipass') / 'data' / 'input')
        assert output_folder == str(Path('/fake/meipass') / 'data' / 'output')


# ------------------------------- LOAD DATAKEY TESTS ------------------------------- #


class TestLoadDatakey:
    """Tests for load_datakey function."""

    def test_valid_datakey(self, tmp_path: Path) -> None:
        """Valid datakey is loaded with correct column mapping."""
        file = tmp_path / 'datakey.csv'
        file.write_text(
            'Clientnaam;Synoniemen;Code\nJan Jansen;Jan;C001\nPiet Pietersen;Piet;C002\n',
            encoding='utf-8',
        )

        df = load_datakey(str(file))

        assert df is not None
        assert list(df.columns) == ['clientname', 'synonyms', 'code']
        assert len(df) == 2  # noqa: PLR2004
        assert df['clientname'][0] == 'Jan Jansen'

    def test_strips_whitespace_and_filters_empty(self, tmp_path: Path) -> None:
        """Whitespace is stripped and empty clientnames are filtered."""
        file = tmp_path / 'datakey.csv'
        file.write_text(
            'Clientnaam;Synoniemen;Code\n  Jan  ;Jan;C001\n;empty;C002\n  ;spaces;C003\n',
            encoding='utf-8',
        )

        df = load_datakey(str(file))

        assert df is not None
        assert len(df) == 1
        assert df['clientname'][0] == 'Jan'

    def test_unsupported_encoding_returns_none(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Unsupported encoding returns None and logs warning."""
        file = tmp_path / 'datakey.csv'
        file.write_text('Clientnaam;Synoniemen;Code\nTest;test;C001\n', encoding='utf-16')

        with caplog.at_level(logging.WARNING):
            df = load_datakey(str(file))

        assert df is None
        assert 'encoding not supported' in caplog.text


# ------------------------------- SAVE DATAKEY TESTS ------------------------------- #


class TestSaveDatakey:
    """Tests for save_datakey function."""

    def test_saves_with_dutch_columns(self, tmp_path: Path) -> None:
        """Datakey is saved with Dutch column names and semicolon separator."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        save_datakey(df, 'test.csv', str(tmp_path))

        content = (tmp_path / 'datakey.csv').read_text(encoding='utf-8')
        assert 'Clientnaam;Synoniemen;Code' in content
        assert 'Jan;J;C001' in content

    def test_custom_datakey_name(self, tmp_path: Path) -> None:
        """Custom datakey_name is used as output filename."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        save_datakey(df, 'test.csv', str(tmp_path), datakey_name='custom.csv')

        assert (tmp_path / 'custom.csv').exists()
        assert not (tmp_path / 'datakey.csv').exists()

    def test_oserror_logs_warning(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """OSError is caught and logged as warning."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        def mock_write(*_args: object, **_kwargs: object) -> None:
            raise OSError

        monkeypatch.setattr(pl.DataFrame, 'write_csv', mock_write)

        with caplog.at_level(logging.WARNING):
            save_datakey(df, 'test.csv', str(tmp_path))

        assert 'Cannot write datakey' in caplog.text
