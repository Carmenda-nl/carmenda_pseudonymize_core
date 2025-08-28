# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Handler module for text de-identification.

This module provides functions for processing medical text data
with enhanced case-insensitive name detection. It includes:

    - deidentify_text: De-identify text with or without patient name information
    - replace_tags: Replace patient tags with pseudonymized values
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import deduce
from deduce.person import Person

from core.name_detector import DutchNameDetector, NameAnnotation
from utils.logger import setup_logging

if TYPE_CHECKING:
    from docdeid.document import Document


class DeidentifyHandler:
    """Handler class for de-identification operations."""

    def __init__(self) -> None:
        """Initialize handler with a configured Deduce instance."""
        self.logger = setup_logging()

        # Log report results for debugging (only if debug mode is enabled)
        self.processed_reports = []
        self.total_processed = 0

        custom_lookup = Path(__file__).parent / 'lookup_tables'

        # For PyInstaller, also check in the bundle root directory
        if not custom_lookup.exists() and hasattr(sys, '_MEIPASS'):
            bundle_lookup = Path(sys._MEIPASS) / 'lookup_tables'
            if bundle_lookup.exists():
                custom_lookup = bundle_lookup

        # Check in current working directory (for manual deployment)
        if not custom_lookup.exists():
            cwd_lookup = Path.cwd() / 'lookup_tables'
            if cwd_lookup.exists():
                custom_lookup = cwd_lookup

        if custom_lookup.exists():
            self.lookup_data_path = str(custom_lookup)
            self.logger.debug('Using custom lookup tables from: %s', self.lookup_data_path)
        else:
            self.lookup_data_path = None
            self.logger.debug('No custom lookup tables found, using default lookup tables')

        self.deduce_instance = self._get_deduce_instance()
        self.lookup_sets = self._load_lookup_tables()
        self.name_detector = DutchNameDetector(self.lookup_sets)

    def _get_deduce_instance(self) -> deduce.Deduce:
        """Get a configured Deduce instance with lookup data."""
        if self.lookup_data_path:
            cache_path = Path(self.lookup_data_path)
            lookup_structs_file = cache_path / 'cache' / 'lookup_structs.pickle'

            self.logger.debug(
                'Cache file exists: %s, size: %d bytes',
                lookup_structs_file.exists(),
                lookup_structs_file.stat().st_size if lookup_structs_file.exists() else 0,
            )
            return deduce.Deduce(lookup_data_path=self.lookup_data_path, cache_path=cache_path)

        # Use default Deduce lookup tables
        return deduce.Deduce()

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

    def deidentify_text(self, input_cols: dict) -> Callable[[dict], str]:
        """De-identify report text with or without patient name column."""

        def inner_func(row: dict) -> str:
            try:
                report_text = row[input_cols['report']]

                # Process 1: Apply Deduce detection
                deduce_result = self._deduce_detector(row, input_cols, report_text)

                # Process 2: Apply extended custom name detection
                extend_result = self.name_detector.names_case_insensitive(report_text)

                # Process 3: Merge results of process 1 and 2
                merged_result = self._merge_detections(report_text, deduce_result.deidentified_text, extend_result)

                # Optional process: Collect debug data if needed
                if self.logger.level == self.logger.level:
                    self.total_processed += 1
                    self.processed_reports.append(
                        {
                            'report_text': report_text,
                            'deduce_result': deduce_result.deidentified_text,
                            'merge_results': merged_result,
                        },
                    )
            except (KeyError, IndexError, AttributeError, ValueError, TypeError):
                return ''  # No error log as null rows will be collected later
            else:
                return merged_result

        return inner_func

    def _deduce_detector(self, row: dict, input_cols: dict, report_text: str) -> Document:
        """Apply Deduce detection with or without patient context."""
        if 'patientName' in input_cols:
            patient = self._patient_object(row[input_cols['patientName']])
            return self.deduce_instance.deidentify(report_text, metadata={'patient': patient})

        return self.deduce_instance.deidentify(report_text)

    def _patient_object(self, patient_name_str: str) -> Person:
        """Create a Person object from patient name string."""
        patient_name = patient_name_str.split(' ', maxsplit=1)
        patient_initials = ''.join([name[0] for name in patient_name])

        if len(patient_name) == 1:
            return Person(first_names=[patient_name[0]])

        return Person(first_names=[patient_name[0]], surname=patient_name[1], initials=patient_initials)

    def _merge_detections(self, report_text: str, deduce_result: str, extend_result: list[NameAnnotation]) -> str:
        """Merge Deduce and custom detections."""
        result_text = deduce_result

        # Find positions of `process 1` missed names that `process 2` detector found
        missed_detections = []

        for detection in extend_result:
            # Check if this position was already handled by Deduce
            original_segment = report_text[detection.start : detection.end]

            # If the text in first result still contains the original name
            # (meaning Deduce didn't detect it), we should replace it
            if original_segment.lower() in result_text.lower():
                missed_detections.append(detection)

        # Replace missed names with [PERSON-X] tags
        person_counter = self._count_existing_tags(result_text) + 1

        for detection in sorted(missed_detections, key=lambda x: x.start):
            # Find the name in the current result text
            name_to_replace = report_text[detection.start : detection.end]

            # Case-insensitive search and replace
            pattern = re.escape(name_to_replace)
            replacement = f'[PERSOON-{person_counter}]'

            # Replace first occurrence
            new_text = re.sub(pattern, replacement, result_text, count=1, flags=re.IGNORECASE)

            if new_text != result_text:
                result_text = new_text
                person_counter += 1

        return result_text

    def _count_existing_tags(self, text: str) -> int:
        """Count existing [PERSON-X] tags in text."""
        pattern = r'\[PERSOON-\d+\]'
        return len(re.findall(pattern, text))

    @staticmethod
    def replace_tags(text: str, new_value: str, tag: str = '[PATIENT]') -> str:
        """Replace patient tags with pseudonymized values."""
        try:
            return str.replace(text, tag, f'[{new_value}]')
        except (TypeError, AttributeError):
            return None

    def debug_deidentify_text(self) -> None:
        """Only show de-identification results if logger is in debug mode."""
        if self.logger.level == self.logger.level:
            self.logger.debug('De-identification results for %d processed report rules:\n', self.total_processed)

            for report, rule in enumerate(self.processed_reports, 1):
                self.logger.debug(
                    ' Report rule %d\n  ORIGINAL: %s\n  DEDUCE: %s\n  EXTENDED: %s\n',
                    report,
                    rule['report_text'],
                    rule['deduce_result'],
                    rule['merge_results'],
                )
