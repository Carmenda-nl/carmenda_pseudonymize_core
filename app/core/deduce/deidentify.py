# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Handler module for de-identification of medical text."""

from __future__ import annotations

import logging
import re
from functools import reduce
from typing import TYPE_CHECKING

import polars as pl
from deduce.person import Person  # type: ignore[import-untyped]

from core.deduce.instance import DeduceInstanceManager
from core.deduce.name_detector import DutchNameDetector, NameAnnotation
from core.utils.logger import setup_logging
from core.utils.terminal import colorize_tags, log_block

if TYPE_CHECKING:
    from core.utils.progress_tracker import ProgressTracker

logger = setup_logging()


class DeidentifyHandler:
    """Handler class for de-identification operations."""

    def __init__(self, tracker: ProgressTracker) -> None:
        """Initialize a configured Deduce instance."""
        self.tracker = tracker
        self.deduce_manager = DeduceInstanceManager()

        self.tracker.set_progress('init_deduce')
        self.deduce_instance = self.deduce_manager.create_instance()

        self.tracker.set_progress('init_tables')
        self.lookup_sets = self.deduce_manager.load_lookup_tables()

        self.tracker.set_progress('init_names')
        self.name_detection = DutchNameDetector(self.lookup_sets)

        # For debug logging of de-identification results
        self.processed_reports: list[dict[str, str]] = []
        self.total_processed = 0

        # Progress tracking (progress bar)
        self.processed_count = 0
        self.total_count = 0
        self.last_update = 0

    def replace_synonym(self, df: pl.DataFrame, datakey: pl.DataFrame, report_cols: list[str]) -> pl.DataFrame:
        """Replace all synonyms in the report text with their main names."""
        synonym_df = (
            datakey.with_columns(pl.col('synonyms').str.split(','))
            .explode('synonyms')
            .with_columns(pl.col('synonyms').str.strip_chars())
            .filter(pl.col('synonyms') != '')
            .select([pl.col('clientname'), pl.col('synonyms')])
        )

        synonym_pairs = list(zip(synonym_df['synonyms'], synonym_df['clientname'], strict=True))

        replaced_synonyms = [
            reduce(
                lambda expr, pair: expr.str.replace_all(r'\b' + re.escape(pair[0]) + r'\b', pair[1], literal=False),
                synonym_pairs,
                pl.col(column),
            ).alias(column)
            for column in report_cols
        ]
        return df.with_columns(replaced_synonyms)

    def _deduce_detection(self, report_text: str, clientname: str | None = None) -> str:
        """Apply Deduce detection with or without clientname (case-insensitive)."""
        metadata = {}
        preprocessed_text = report_text

        if clientname:
            name_parts = clientname.split()
            first_name = name_parts[0] if name_parts else None

            full_name_pattern = r'\b' + re.escape(clientname) + r'\b'
            preprocessed_text = re.sub(full_name_pattern, clientname, report_text, flags=re.IGNORECASE)

            if first_name and len(first_name) > 1:
                first_name_pattern = r'\b' + re.escape(first_name) + r'\b'
                preprocessed_text = re.sub(first_name_pattern, first_name, preprocessed_text, flags=re.IGNORECASE)

            name_parts_with_surname = clientname.split(' ', maxsplit=1)
            client_initials = ''.join([name[0] for name in name_parts_with_surname])

            if len(name_parts_with_surname) == 1:
                client = Person(first_names=[name_parts_with_surname[0]])
            else:
                client = Person(
                    first_names=[name_parts_with_surname[0]],
                    surname=name_parts_with_surname[1],
                    initials=client_initials,
                )

            metadata['patient'] = client

        result = self.deduce_instance.deidentify(preprocessed_text, metadata=metadata)
        return result.deidentified_text

    def _merge_detections(self, report_text: str, deduce_result: str, extend_result: list[NameAnnotation]) -> str:
        """Merge Deduce and custom detections."""
        result_text = deduce_result
        missed_detections = []

        for detection in extend_result:
            original_segment = report_text[detection.start : detection.end]
            if original_segment.lower() in result_text.lower():
                missed_detections.append(detection)

        # Count existing [PERSOON-X] tags in text
        person_counter = len(re.findall(r'\[PERSOON-\d+\]', result_text)) + 1

        for detection in sorted(missed_detections, key=lambda detect: detect.start):
            name_to_replace = report_text[detection.start : detection.end]
            pattern = re.escape(name_to_replace)
            replacement = f'[PERSOON-{person_counter}]'
            new_text = re.sub(pattern, replacement, result_text, count=1, flags=re.IGNORECASE)

            if new_text != result_text:
                result_text = new_text
                person_counter += 1

        return result_text

    def _process_report(self, report_text: str, clientname: str | None = None) -> str:
        """Process a single report (row) with or without clientname."""
        if report_text is None or not isinstance(report_text, str):
            return ''  # <-- Return empty text if not a valid string.

        deduce_result = self._deduce_detection(report_text, clientname)
        extend_result = self.name_detection.names_case_insensitive(report_text)
        merged_result = self._merge_detections(report_text, deduce_result, extend_result)

        if logger.level == logging.DEBUG:
            self.total_processed += 1
            self.processed_reports.append({'report': report_text, 'deduce': deduce_result, 'merged': merged_result})

        return merged_result

    def _deidentify_batch(self, batch: pl.Series) -> pl.Series:
        """Collect a batch of report texts and process them per row, while tracking progress."""
        results = []

        for row in batch.to_list():
            self.tracker.check_cancelled()
            report_text = row.get('report')
            clientname = row.get('clientname') or None

            # Skip empty/null rows that may appear in batches
            if not report_text:
                results.append('')
            else:
                result = self._process_report(report_text, clientname)
                results.append(result)

            max_percentage = 100
            self.processed_count += 1

            if self.processed_count - self.last_update >= max_percentage:
                step_progress = (self.processed_count / self.total_count) * 100
                self.tracker.set_row_progress(
                    'pseudonymize',
                    self.processed_count,
                    self.total_count,
                    int(step_progress),
                    overall=(20, 85),
                )
                self.last_update = self.processed_count

        return pl.Series(results)

    def deidentify_text(self, df: pl.DataFrame, input_cols: dict) -> pl.DataFrame:
        """De-identify report text with or without clientname."""
        reports_cols = [value.strip() for key, value in input_cols.items() if key.startswith('report')]

        has_clientname = 'clientname' in input_cols and input_cols['clientname'] in df.columns
        total_rows = df.height

        clientname_message = 'with clientname' if has_clientname else ''
        logger.info('Processing %d rows %s\n', total_rows, clientname_message)

        # Initialize a clean progress bar
        self.last_update = 0
        self.processed_count = 0
        self.total_count = total_rows * len(reports_cols)

        df = df.with_row_index('_idx')
        other_columns = [column for column in df.columns if column not in reports_cols]

        melted = df.unpivot(index=other_columns, on=reports_cols, variable_name='_col', value_name='report')

        struct_fields = [pl.col('report').str.strip_chars().alias('report')]
        if has_clientname:
            struct_fields.append(pl.col(input_cols['clientname']).alias('clientname'))

        melted = melted.with_columns(
            pl.struct(struct_fields).map_batches(self._deidentify_batch, return_dtype=pl.Utf8).alias('processed'),
        )

        pivoted = melted.pivot(values='processed', index='_idx', on='_col')

        rename_map = {col: f'processed_report_{number}' for number, col in enumerate(reports_cols, start=1)}
        pivoted = pivoted.select(['_idx', *reports_cols]).rename(rename_map)
        df_result = df.join(pivoted, on='_idx').drop('_idx')

        self.tracker.clean_progress_bar()

        return df_result

    def add_clientcodes(self, df: pl.DataFrame, datakey: pl.DataFrame, input_cols: dict[str, str]) -> pl.DataFrame:
        """Add patient codes to DataFrame and replace [PATIENT] tags in processed reports."""
        clientname_col = input_cols['clientname']

        df = (
            df.join(
                datakey.select(['clientname', 'code']),
                left_on=clientname_col,
                right_on='clientname',
                how='left',
                coalesce=True,
            )
            .rename({'code': 'clientcode'})
            .select('clientcode', pl.all().exclude('clientcode'))
        )

        processed_cols = [col for col in df.columns if col.startswith('processed_report_')]
        return df.with_columns(
            [
                pl.col(col).str.replace_all(r'\[PATIENT\]', pl.format('[{}]', pl.col('clientcode')))
                for col in processed_cols
            ],
        )

    def deidentify_text_debug(self) -> None:
        """Only show de-identification results if logger is in debug mode."""
        max_reports = 10

        for rule in self.processed_reports[:max_reports]:
            title = 'De-identification Report'
            sections = {
                'ORIGINAL': colorize_tags(rule['report']),
                'DEDUCE': colorize_tags(rule['deduce']),
                'EXTENDED': colorize_tags(rule['merged']),
            }
            log_block(title, sections)
