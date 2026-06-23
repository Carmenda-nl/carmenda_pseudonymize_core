# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Tests for file handling utilities."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from core.utils.file_handling import save_datafile, save_datakey

# ----------------------------------- FIXTURES ------------------------------------ #


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Create output directory."""
    out = tmp_path / 'output'
    out.mkdir(parents=True, exist_ok=True)
    return out


# ------------------------------ SAVE DATAFILE TESTS ------------------------------ #


class TestSaveDatafile:
    """Tests for save_datafile function."""

    def test_saves_with_deidentified_suffix(self, tmp_path: Path) -> None:
        """Output file is created with _pseudonymised suffix."""
        df = pl.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
        output_folder = tmp_path / 'output'

        save_datafile(df, 'test.csv', str(output_folder))

        saved = output_folder / 'test_pseudonymised.csv'
        assert saved.exists()

        result = pl.read_csv(saved)
        assert result['name'].to_list() == ['Alice', 'Bob']

    def test_creates_output_folder(self, tmp_path: Path) -> None:
        """Output folder is created if it doesn't exist."""
        df = pl.DataFrame({'name': ['Alice']})
        output_folder = tmp_path / 'new_folder' / 'subfolder'

        assert not output_folder.exists()
        save_datafile(df, 'test.csv', str(output_folder))

        assert (output_folder / 'test_pseudonymised.csv').exists()

    def test_ignores_parent_writes_flat_into_output(self, tmp_path: Path) -> None:
        """Filename with a parent path is written flat into the output folder, ignoring the parent."""
        df = pl.DataFrame({'name': ['Alice']})

        save_datafile(df, 'job123/data.csv', str(tmp_path / 'output'))

        assert (tmp_path / 'output' / 'data_pseudonymised.csv').exists()
        assert not (tmp_path / 'output' / 'job123').exists()

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


# ------------------------------- SAVE DATAKEY TESTS ------------------------------- #


class TestSaveDatakey:
    """Tests for save_datakey function."""

    def test_saves_with_dutch_columns(self, tmp_path: Path) -> None:
        """Datakey is saved with Dutch column names and comma delimiter."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        save_datakey(df, 'test.csv', str(tmp_path))

        content = (tmp_path / 'test_key.csv').read_text(encoding='utf-8')
        assert 'Clientnaam,Synoniemen,Code' in content
        assert 'Jan,J,C001' in content

    def test_custom_datakey_name(self, tmp_path: Path) -> None:
        """Custom key_name is used as output filename."""
        df = pl.DataFrame({'clientname': ['Jan'], 'synonyms': ['J'], 'code': ['C001']})

        save_datakey(df, 'test.csv', str(tmp_path), key_name='custom.csv')

        assert (tmp_path / 'custom.csv').exists()
        assert not (tmp_path / 'test_key.csv').exists()

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
