# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Handler module for text de-identification.

This module provides functions for processing medical text data
with enhanced case-insensitive name detection. It includes:

    - deidentify_text: De-identify text with or without patient name information
    - debug_deidentify_text: Log detailed debug information for processed texts
"""

from __future__ import annotations

import logging
import os
import pickle
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

import deduce
import polars as pl
from deduce.person import Person

from core.name_detector import DutchNameDetector, NameAnnotation

from .utils.logger import setup_logging
from .utils.progress_tracker import tracker
from .utils.terminal import colorize_tags, log_block

DataKey = list[dict[str, str]]  # Type alias
logger = setup_logging()


class DeduceInstanceManager:
    """Configuring Deduce instances."""

    def __init__(self, lookup_data_path: str | None) -> None:
        """Initialize the Deduce instance manager."""
        self.lookup_data_path = lookup_data_path

    def create_instance(self) -> deduce.Deduce:
        """Create a configured Deduce instance with lookup data."""
        self._cleanup_temp_directory()

        if not self.lookup_data_path:
            return deduce.Deduce()

        cache_path = Path(self.lookup_data_path)
        lookup_structs_file = cache_path / 'cache' / 'lookup_structs.pickle'

        self._log_cache_info(lookup_structs_file)

        # For frozen, use temp directory approach to avoid path issues
        if hasattr(sys, '_MEIPASS') and lookup_structs_file.exists():
            return self._create_frozen_instance(cache_path)

        return deduce.Deduce(lookup_data_path=self.lookup_data_path, cache_path=cache_path)

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


class DeidentifyHandler:
    """Handler class for de-identification operations."""

    def __init__(self) -> None:
        """Initialize handler with a configured Deduce instance."""
        self.processed_reports = []
        self.total_processed = 0

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
            self.lookup_data_path = str(custom_lookup)
            logger.debug('Using custom lookup tables from: %s', self.lookup_data_path)
        else:
            self.lookup_data_path = None
            logger.warning('No custom lookup tables found, using default lookup tables')

        # Use DeduceInstanceManager for Deduce setup
        deduce_manager = DeduceInstanceManager(self.lookup_data_path)
        self.deduce_instance = deduce_manager.create_instance()
        self.lookup_sets = self._load_lookup_tables()
        self.name_detector = DutchNameDetector(self.lookup_sets)















    def _load_lookup_tables(self) -> dict[str, set[str]]:
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



















    def _client_object(self, clientname: str) -> Person:
        """Create a Person object from clientname string."""
        name_parts = clientname.split(' ', maxsplit=1)
        client_initials = ''.join([name[0] for name in name_parts])

        if len(name_parts) == 1:
            return Person(first_names=[name_parts[0]])

        return Person(first_names=[name_parts[0]], surname=name_parts[1], initials=client_initials)

    def _deduce_with_patient(self, report_text: str, clientname: str) -> str:
        """Apply Deduce detection WITH clientname."""
        patient = self._client_object(clientname)
        result = self.deduce_instance.deidentify(report_text, metadata={'patient': patient})
        return result.deidentified_text

    def _deduce_without_patient(self, report_text: str) -> str:
        """Apply Deduce detection WITHOUT clientname."""
        result = self.deduce_instance.deidentify(report_text)
        return result.deidentified_text
























    def _merge_detections(self, report_text: str, deduce_result: str, extend_result: list[NameAnnotation]) -> str:
        """Merge Deduce and custom detections."""
        result_text = deduce_result
        missed_detections = []

        for detection in extend_result:
            original_segment = report_text[detection.start : detection.end]
            if original_segment.lower() in result_text.lower():
                missed_detections.append(detection)

        person_counter = self._count_existing_tags(result_text) + 1

        for detection in sorted(missed_detections, key=lambda x: x.start):
            name_to_replace = report_text[detection.start : detection.end]
            pattern = re.escape(name_to_replace)
            replacement = f'[PERSOON-{person_counter}]'
            new_text = re.sub(pattern, replacement, result_text, count=1, flags=re.IGNORECASE)

            if new_text != result_text:
                result_text = new_text
                person_counter += 1

        return result_text

    def _count_existing_tags(self, text: str) -> int:
        """Count existing [PERSON-X] tags in text."""
        pattern = r'\[PERSOON-\d+\]'
        return len(re.findall(pattern, text))




    def debug_deidentify_text(self) -> None:
        """Only show de-identification results if logger is in debug mode."""
        max_reports = 10

        for rule in self.processed_reports[:max_reports]:
            title = 'De-identification Report'
            sections = {
                'ORIGINAL': colorize_tags(rule['report_text']),
                'DEDUCE': colorize_tags(rule['deduce_result']),
                'EXTENDED': colorize_tags(rule['merge_results']),
            }
            log_block(title, sections)

























    def _process_report(self, report_text: str, clientname: str | None = None) -> str:
        """Process a single report with optional progress tracking."""
        try:
            # Update progress counter if tracking is enabled
            if hasattr(self, 'processed_count'):
                self.processed_count += 1

                # Update tracker every 100 rows to avoid overhead
                if self.processed_count - self.last_update >= 100:
                    progress = (self.processed_count / self.total_count) * 100
                    tracker.update_with_percentage(
                        'Data transformation',
                        f'Processed {self.processed_count:,}/{self.total_count:,} rows',
                        progress,
                    )
                    self.last_update = self.processed_count

            if clientname:
                deduce_result = self._deduce_with_patient(report_text, clientname)
            else:
                deduce_result = self._deduce_without_patient(report_text)

            extend_result = self.name_detector.names_case_insensitive(report_text)
            merged_result = self._merge_detections(report_text, deduce_result, extend_result)

            if logger.level == logging.DEBUG:
                self.total_processed += 1
                self.processed_reports.append({
                    'report_text': report_text,
                    'deduce_result': deduce_result,
                    'merge_results': merged_result,
                })

            return merged_result

        except (KeyError, IndexError, AttributeError, ValueError, TypeError) as e:
            logger.warning('Failed to process report: %s', e)
            return ''












    def replace_synonym(self, df: pl.DataFrame, datakey: pl.DataFrame, input_cols: dict[str, str]) -> pl.DataFrame:
        """Replace all synonyms in the report text with their main names.
        
        Args:
            df: DataFrame with report text
            datakey: DataFrame with clientname and synonyms mapping
            input_cols: Input column mapping
            
        Returns:
            DataFrame with synonyms replaced in report text
        """
        synonym_df = (
            datakey.with_columns(pl.col('synonyms').str.split(','))
            .explode('synonyms')
            .with_columns(pl.col('synonyms').str.strip_chars())
            .filter(pl.col('synonyms') != '')
            .select([pl.col('clientname'), pl.col('synonyms').alias('synonyms')])
        )

        report_col = input_cols['report']
        synonym_pairs = synonym_df.select(['synonyms', 'clientname']).rows()

        replaced_synonyms = reduce(
            lambda expr, pair: expr.str.replace_all(r'\b' + re.escape(pair[0]) + r'\b', pair[1], literal=False),
            synonym_pairs,
            pl.col(report_col),
        )

        return df.with_columns(replaced_synonyms)

    def add_clientcodes(
        self,
        df: pl.DataFrame,
        datakey: pl.DataFrame,
        input_cols: dict,
    ) -> pl.DataFrame:
        """Add patient codes to DataFrame and replace [PATIENT] tags in processed reports.

        Args:
            df: DataFrame with processed reports
            datakey: DataFrame with clientname to code mapping
            input_cols: Input column mapping

        Returns:
            DataFrame with patient codes added and [PATIENT] tags replaced
        """
        clientname_col = input_cols['clientname']

        # Join de datakey om patient codes toe te voegen (volledige Polars, geen Python conversie)
        df = df.join(
            datakey.select(['clientname', 'code']),
            left_on=clientname_col,
            right_on='clientname',
            how='left',
            coalesce=True,
        ).rename({'code': 'clientcode'}).select('clientcode', pl.all().exclude('clientcode'))

        # Replace [PATIENT] tags in `processed_report` with datakeys
        return df.with_columns(
            pl.col('processed_report').str.replace_all(
                r'\[PATIENT\]',
                pl.format('[{}]', pl.col('clientcode')),
            ),
        )
















    def deidentify_text(self, df: pl.DataFrame, datakey: pl.DataFrame, input_cols: dict) -> pl.DataFrame:
        """De-identify report text with or without patient name column."""
        report_col = input_cols['report']
        has_clientname = 'clientname' in input_cols and input_cols['clientname'] in df.columns
        total_rows = df.height

        clientname_message = 'with clientname' if has_clientname else ''
        logger.info('Processing %d rows %s\n', total_rows, clientname_message)

        # Initialize progress counter
        self.processed_count = 0
        self.total_count = total_rows
        self.last_update = 0

        if has_clientname:
            clientname_col = input_cols['clientname']

            processed_reports = df.select([
                pl.struct([
                    pl.col(report_col).alias('report'),
                    pl.col(clientname_col).alias('clientname'),
                ])
                .map_elements(
                    lambda row: self._process_report(row['report'], row['clientname']),
                    return_dtype=pl.String,
                )
                .alias('processed_report'),
            ])
        else:
            processed_reports = df.select([
                pl.col(report_col)
                .map_elements(
                    lambda text: self._process_report(text, clientname=None),
                    return_dtype=pl.String,
                )
                .alias('processed_report'),
            ])

        result = df.with_columns(processed_reports)

        tracker.update('Data transformation', f'Completed {total_rows:,} rows')
        tracker.finalize_progress()

        return result







