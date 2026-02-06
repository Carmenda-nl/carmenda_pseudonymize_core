# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Tests for file handling utilities."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import polars as pl
import pytest

from core.utils import csv_handler, file_handling
from core.utils.file_handling import (
    get_environment,
    load_datafile,
    load_datakey,
    save_datafile,
    save_datakey,
)

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


# ----------------------------- GET ENVIRONMENT TESTS ----------------------------- #


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
        monkeypatch.delattr(sys, 'frozen', raising=False)

        input_folder, output_folder = get_environment()

        assert input_folder == 'data/input'
        assert output_folder == 'data/output'

    def test_pyinstaller_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PyInstaller environment returns _MEIPASS-based paths."""
        monkeypatch.delenv('DOCKER_ENV', raising=False)
        monkeypatch.setattr(sys, 'frozen', True, raising=False)
        monkeypatch.setattr(sys, '_MEIPASS', '/fake/meipass', raising=False)

        input_folder, output_folder = get_environment()

        assert input_folder == str(Path('/fake/meipass') / 'data' / 'input')
        assert output_folder == str(Path('/fake/meipass') / 'data' / 'output')


# ------------------------------ LOAD DATAFILE TESTS ------------------------------ #


class TestLoadDatafile:
    """Tests for load_datafile function."""

    def test_file_not_exists_returns_none(self, tmp_path: Path) -> None:
        """Non-existent file returns None."""
        assert load_datafile(str(tmp_path / 'nonexistent.csv'), str(tmp_path)) is None

    def test_basic_csv_loads_correctly(self, csv_file: Path, output_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Basic CSV file returns a Polars DataFrame with correct structure."""
        csv_file.write_text('name,age,city\nAlice,30,Amsterdam\nBob,25,Rotterdam', encoding='utf-8')

        def mock_detect_properties(_path: Path) -> dict[str, str]:
            return {'encoding': 'utf-8', 'delimiter': ',', 'header': 'name,age,city'}

        def mock_sanitize(_path: Path, _props: dict, _out: str) -> str:
            return str(csv_file)

        def mock_normalize(_path: Path, _props: dict) -> str:
            return str(csv_file)

        monkeypatch.setattr(csv_handler, 'detect_properties', mock_detect_properties)
        monkeypatch.setattr(csv_handler, 'sanitize_csv', mock_sanitize)
        monkeypatch.setattr(csv_handler, 'normalize_csv', mock_normalize)

        result = load_datafile(str(csv_file), str(output_dir))

        assert isinstance(result, pl.DataFrame)
        assert result.columns == ['name', 'age', 'city']
        assert len(result) == 2  # noqa: PLR2004

    def test_non_csv_file_returns_none(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Non-CSV file returns None and logs warning."""
        txt_file = tmp_path / 'test.txt'
        txt_file.write_text('some text', encoding='utf-8')

        with caplog.at_level(logging.WARNING):
            result = load_datafile(str(txt_file), str(tmp_path))

        assert result is None
        assert 'Unsupported file type' in caplog.text


# ------------------------------ SAVE DATAFILE TESTS ------------------------------ #


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


# ------------------------------- LOAD DATAKEY TESTS ------------------------------- #


class TestLoadDatakey:
    """Tests for load_datakey function."""

    def test_valid_datakey(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid datakey is loaded with correct column mapping."""
        file = tmp_path / 'datakey.csv'
        file.write_text(
            'Clientnaam,Synoniemen,Code\nJan Jansen,Jan,C001\nPiet Pietersen,Piet,C002\n',
            encoding='utf-8',
        )

        # Mock detect_properties
        def mock_detect_properties(_path: Path) -> dict[str, str]:
            return {'encoding': 'utf-8', 'delimiter': ',', 'header': 'Clientnaam,Synoniemen,Code'}

        monkeypatch.setattr(csv_handler, 'detect_properties', mock_detect_properties)

        df = load_datakey(str(file))

        assert df is not None
        assert list(df.columns) == ['clientname', 'synonyms', 'code']
        assert len(df) == 2  # noqa: PLR2004
        assert df['clientname'][0] == 'Jan Jansen'

    def test_strips_whitespace_and_filters_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Whitespace is stripped and empty clientnames are filtered."""
        file = tmp_path / 'datakey.csv'
        file.write_text(
            'Clientnaam,Synoniemen,Code\n  Jan  ,Jan,C001\n,empty,C002\n  ,spaces,C003\n',
            encoding='utf-8',
        )

        # Mock detect_properties
        def mock_detect_properties(_path: Path) -> dict[str, str]:
            return {'encoding': 'utf-8', 'delimiter': ',', 'header': 'Clientnaam,Synoniemen,Code'}

        monkeypatch.setattr(csv_handler, 'detect_properties', mock_detect_properties)

        df = load_datakey(str(file))

        assert df is not None
        assert len(df) == 1
        assert df['clientname'][0] == 'Jan'

    def test_unsupported_encoding_returns_none(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Unsupported encoding returns None and logs warning."""
        file = tmp_path / 'datakey.csv'
        file.write_text('Clientnaam,Synoniemen,Code\nTest,test,C001\n', encoding='utf-8')

        # Mock detect_properties in the file_handling module where it's imported
        def mock_detect_properties(_path: Path) -> dict[str, str]:
            return {'encoding': 'utf-16', 'delimiter': ',', 'header': 'Clientnaam,Synoniemen,Code'}

        monkeypatch.setattr(file_handling, 'detect_properties', mock_detect_properties)

        with caplog.at_level(logging.WARNING):
            df = load_datakey(str(file))

        assert df is None
        assert 'encoding not supported' in caplog.text


# ------------------------------- SAVE DATAKEY TESTS ------------------------------- #


class TestSaveDatakey:
    """Tests for save_datakey function."""

    def test_saves_with_dutch_columns(self, tmp_path: Path) -> None:
        """Datakey is saved with Dutch column names and comma separator."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        save_datakey(df, 'test.csv', str(tmp_path))

        content = (tmp_path / 'datakey.csv').read_text(encoding='utf-8')
        assert 'Clientnaam,Synoniemen,Code' in content
        assert 'Jan,J,C001' in content

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
