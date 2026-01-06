# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Manager for Deduce instance configuration and lookup tables.

This module provides the DeduceInstanceManager class for configuring
and managing Deduce instances with custom lookup tables.
"""

from __future__ import annotations

import os
import pickle
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import deduce

from .utils.logger import setup_logging

logger = setup_logging()


class DeduceInstanceManager:
    """Configuring Deduce instances."""

    def __init__(self, lookup_data_path: str | None = None) -> None:
        """Initialize the Deduce instance manager."""
        self.lookup_data_path = self._resolve_lookup_path(lookup_data_path)
        self.deduce_instance = None

    def _resolve_lookup_path(self, provided_path: str | None) -> str | None:
        """Resolve the lookup tables path in order of priority."""
        if provided_path:
            return provided_path

        custom_lookup = Path(__file__).parent / 'lookup_tables'

        # If frozen, use the bundled lookup tables
        if hasattr(sys, '_MEIPASS'):
            bundle_lookup = Path(sys._MEIPASS) / 'lookup_tables'
            custom_lookup = bundle_lookup

        # Check in current working directory (for manual deployment)
        if not custom_lookup.exists():
            cwd_lookup = Path.cwd() / 'lookup_tables'
            if cwd_lookup.exists():
                custom_lookup = cwd_lookup

        if custom_lookup.exists():
            logger.debug('Using custom lookup tables from: %s', custom_lookup)
            return str(custom_lookup)

        logger.warning('No custom lookup tables found, using default lookup tables')
        return None

    def create_instance(self) -> deduce.Deduce:
        """Create a configured Deduce instance with lookup data."""
        self._cleanup_temp_directory()

        if not self.lookup_data_path:
            self.deduce_instance = deduce.Deduce()
            return self.deduce_instance

        cache_path = Path(self.lookup_data_path)
        lookup_structs_file = cache_path / 'cache' / 'lookup_structs.pickle'

        self._log_cache_info(lookup_structs_file)

        # For frozen, use temp directory approach to avoid path issues
        if hasattr(sys, '_MEIPASS') and lookup_structs_file.exists():
            self.deduce_instance = self._create_frozen_instance(cache_path)
            return self.deduce_instance

        self.deduce_instance = deduce.Deduce(lookup_data_path=self.lookup_data_path, cache_path=cache_path)
        return self.deduce_instance

    def _cleanup_temp_directory(self) -> None:
        """Remove stale deduce_lookup folder before initializing Deduce."""
        temp_dir = Path(tempfile.gettempdir()) / 'deduce_lookup'

        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                logger.debug('Removed stale lookup folder: %s', temp_dir)
            except (OSError, shutil.Error) as e:
                logger.warning('Failed to remove stale lookup folder: %s, error: %s', temp_dir, e)

    def _log_cache_info(self, lookup_structs_file: Path) -> None:
        """Log information about the cache file."""
        logger.debug(
            'Cache file exists: %s, size: %d bytes',
            lookup_structs_file.exists(),
            lookup_structs_file.stat().st_size if lookup_structs_file.exists() else 0,
        )

    def _create_frozen_instance(self, cache_path: Path) -> deduce.Deduce:
        """Create Deduce instance for frozen application with temp directory setup."""
        try:
            temp_lookup_path = Path(tempfile.gettempdir()) / 'deduce_lookup'
            setup_needed = self._check_temp_setup_needed(temp_lookup_path)

            if setup_needed:
                self._setup_temp_lookup_directory(cache_path, temp_lookup_path)

            logger.debug('Copied lookup structures to: %s', temp_lookup_path)

            return deduce.Deduce(
                lookup_data_path=str(temp_lookup_path),
                cache_path=temp_lookup_path,
                build_lookup_structs=False,
            )

        except (OSError, shutil.Error) as e:
            logger.warning('Failed to copy lookup structures to temp directory: %s', e)
            logger.warning('Temp directory approach failed, using original paths')

            return deduce.Deduce(
                lookup_data_path=self.lookup_data_path,
                cache_path=cache_path,
                build_lookup_structs=False,
            )

    def _check_temp_setup_needed(self, temp_lookup_path: Path) -> bool:
        """Check if temp directory setup is needed."""
        if not temp_lookup_path.exists():
            temp_lookup_path.mkdir(parents=True, exist_ok=True)
            return True

        temp_src_dir = temp_lookup_path / 'src'
        temp_cache_file = temp_lookup_path / 'cache' / 'lookup_structs.pickle'
        return not (temp_src_dir.exists() and temp_cache_file.exists())

    def _setup_temp_lookup_directory(self, cache_path: Path, temp_lookup_path: Path) -> None:
        """Set up the temporary lookup directory with source files and cache."""
        self._copy_source_files(cache_path, temp_lookup_path)
        self._copy_and_update_cache_file(cache_path, temp_lookup_path)
        self._update_file_timestamps(temp_lookup_path)

    def _copy_source_files(self, cache_path: Path, temp_lookup_path: Path) -> None:
        """Copy source files to temp directory."""
        original_src = cache_path / 'src'
        temp_src_dir = temp_lookup_path / 'src'

        if original_src.exists():
            shutil.copytree(original_src, temp_src_dir, dirs_exist_ok=True, copy_function=shutil.copy2)

    def _copy_and_update_cache_file(self, cache_path: Path, temp_lookup_path: Path) -> None:
        """Copy cache file to temp directory and update its timestamp."""
        original_cache_file = cache_path / 'cache' / 'lookup_structs.pickle'
        temp_cache_dir = temp_lookup_path / 'cache'
        temp_cache_dir.mkdir(parents=True, exist_ok=True)

        if not original_cache_file.exists():
            return

        shutil.copy2(original_cache_file, temp_cache_dir / 'lookup_structs.pickle')
        self._update_cache_datetime(temp_cache_dir / 'lookup_structs.pickle')

    def _update_cache_datetime(self, cache_file_path: Path) -> None:
        """Update cache file datetime to prevent rebuilding."""
        try:
            with cache_file_path.open('rb') as file:
                cache_data = pickle.load(file)

            cache_data['saved_datetime'] = datetime.now().isoformat()

            with cache_file_path.open('wb') as file:
                pickle.dump(cache_data, file)

            logger.debug('Updated cache saved_datetime to prevent rebuilding')

        except (FileNotFoundError, pickle.PickleError, KeyError) as e:
            logger.warning('Failed to update cache datetime: %s', e)

    def _update_file_timestamps(self, temp_lookup_path: Path) -> None:
        """Update file timestamps to prevent cache rebuilding."""
        current_time = time.time()
        old_time = current_time - 630720000  # 20 years ago
        temp_src_dir = temp_lookup_path / 'src'

        if not temp_src_dir.exists():
            return

        # Set all source files to very old timestamp
        for txt_file in temp_src_dir.rglob('*.txt'):
            os.utime(txt_file, (old_time, old_time))

        # Set directory timestamps
        os.utime(temp_src_dir, (old_time, old_time))
        os.utime(temp_lookup_path, (old_time, old_time))

        # Set cache file timestamp to current time
        cache_file_path = temp_lookup_path / 'cache' / 'lookup_structs.pickle'
        if cache_file_path.exists():
            os.utime(cache_file_path, (current_time, current_time))

    def load_lookup_tables(self) -> dict[str, set[str]]:
        """Load lookup tables based on the configured paths."""
        if self.lookup_data_path:
            base_path = Path(self.lookup_data_path) / 'src' / 'names'
            whitelist_path = Path(self.lookup_data_path) / 'src' / 'whitelist'
        else:
            deduce_lookup = Path(self.deduce_instance.lookup_data_path)
            base_path = deduce_lookup / 'src' / 'names'
            whitelist_path = deduce_lookup / 'src' / 'whitelist'

        lookup_files = {
            'first_names': base_path / 'lst_first_name' / 'items.txt',
            'surnames': base_path / 'lst_surname' / 'items.txt',
            'interfix_surnames': base_path / 'lst_interfix_surname' / 'items.txt',
            'name_prefixes': base_path / 'lst_prefix' / 'items.txt',
            'interfixes': base_path / 'lst_interfix' / 'items.txt',
            'common_words': whitelist_path / 'lst_common_word' / 'items.txt',
            'stop_words': whitelist_path / 'lst_stop_word' / 'items.txt',
        }

        def load_word_set(path: Path) -> set[str]:
            """Convert lookup table words into a lowercase set."""
            if path.exists():
                with path.open(encoding='utf-8') as lookup_file:
                    # Skip empty lines, strip whitespace & lowercase
                    return {clean.lower() for line in lookup_file if (clean := line.strip())}
            return set()

        return {attr: load_word_set(file_path) for attr, file_path in lookup_files.items()}
