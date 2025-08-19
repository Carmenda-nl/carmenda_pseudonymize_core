# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Deduce handler module for text de-identification.

This module provides functions for processing medical text data using the Deduce
de-identification library with enhanced case-insensitive name detection. It includes:

    - deduce_with_id: De-identify text using patient name information (enhanced)
    - replace_tags: Replace patient tags with pseudonymized values
    - deduce_report_only: De-identify text without patient context
"""

from __future__ import annotations

import deduce
import re

from pathlib import Path
from typing import Callable
from deduce.person import Person


class NameAnnotation:
    """Represents a detected matched name annotation."""

    def __init__(self, start: int, end: int, text: str, tag: str, confidence: float) -> None:
        """Initialize a name annotation with the given parameters."""
        self.start = start
        self.end = end
        self.text = text
        self.tag = tag
        self.confidence = confidence


class DutchNameDetector:
    """Case-insensitive Dutch name detector using lookup tables and patterns."""

    def __init__(self, lookup_sets: dict[str, set[str]]) -> None:
        """Initialize with lookup data."""
        self.first_names = lookup_sets.get('first_names', set())
        self.surnames = lookup_sets.get('surnames', set())
        self.interfix_surnames = lookup_sets.get('interfix_surnames', set())
        self.interfixes = lookup_sets.get('interfixes', set())
        self.common_words = lookup_sets.get('common_words', set())
        self.stop_words = lookup_sets.get('stop_words', set())

    def names_case_insensitive(self, text: str) -> list[NameAnnotation]:
        """Detect Dutch names in text case-insensitively."""
        annotations: list[NameAnnotation] = []

        def is_overlapping(match_start: int, match_end: int) -> bool:
            return any(ann.start <= match_start < ann.end or ann.start < match_end <= ann.end for ann in annotations)

        max_characters = 3



        patterns = [
            {
                # Pattern 1: First name + interfix + surname
                'regex': r'\b(\w+)\s+((?:de|van|den|te|ter|ten|tot)(?:\s+der|\s+den)?)\s+(\w+)\b',
                'confidence': 0.95,
                'validate': lambda g: (
                    self._first_name(g[0].lower())
                    and self._interfix(g[1].lower())
                    and self._interfix_surname(g[2].lower())
                ),
            },
            {
                # Pattern 2: First name + surname (only match surname when there is a first name)
                'regex': r"\b([A-Za-zÀ-ÖØ-öø-ÿ]+)\s+([A-Za-zÀ-ÖØ-öø-ÿ'\-]+)\b",
                'confidence': 0.85,
                'validate': lambda g: (
                    self._first_name(g[0].lower())
                    and self._surname(g[1].lower())
                    and len(g[0]) >= max_characters
                    and g[0].lower() not in self.common_words
                    and g[0].lower() not in self.stop_words
                ),
            },

            {
                # Pattern 4: Alleen achternamen
                'regex': r"\b([A-Za-zÀ-ÖØ-öø-ÿ'\-]+)\b",
                'confidence': 0.80,
                'validate': lambda g: (
                    self._surname(g[0].lower())
                    and len(g[0]) >= max_characters
                    and g[0].lower() not in self.common_words
                    and g[0].lower() not in self.stop_words
                ),
            },


            # {
            #     # Pattern 3: Only detect first names that are:
            #     #   1. In the first names lookup table
            #     #   2. At least 3 characters long (to avoid false positives)
            #     #   3. Not common words
            #     'regex': r'\b(\w+)\b',
            #     'confidence': 0.70,
            #     'validate': lambda g: (
            #         self._first_name(g[0].lower())
            #         and len(g[0]) >= max_characters
            #         and not self._common_word(g[0].lower())
            #     ),
            #     'regex': r'\b([A-Za-zÀ-ÖØ-öø-ÿ]+)\b',  # alleen letters, geen cijfers
            #     'confidence': 0.70,
            #     'validate': lambda g: (
            #         self._first_name(g[0].lower())
            #         and not self._common_word(g[0].lower())
            #         and not self._stop_word(g[0].lower())
            #     ),
            # },
        ]



        print(f"\nSCANNING TEXT: {text}\n{'-'*60}")

        for idx, pattern_info in enumerate(patterns):


            print(f'applying pattern {idx + 1}: {text}')
            for match in re.finditer(pattern_info['regex'], text, re.IGNORECASE):
                # print(f'  {match}')




                if idx > 0 and is_overlapping(match.start(), match.end()):
                    continue

                if pattern_info['validate'](match.groups()):
                    print(f"  NAME: {match.group(0)!r} @ {match.span()}")
                    annotations.append(
                        NameAnnotation(
                            start=match.start(),
                            end=match.end(),
                            text=match.group(0),
                            tag='persoon',
                            confidence=pattern_info['confidence'],
                        ),
                    )

        # Sort by confidence and position
        annotations.sort(key=lambda x: (-x.confidence, x.start))
        return annotations

    def _first_name(self, name: str) -> bool:
        """Check if a name is in the first names lookup (case-insensitive) or a known variant."""
        return name.lower() in self.first_names

    def _surname(self, name: str) -> bool:
        """Check if a name is in the surnames lookup (case-insensitive) or a known variant."""
        return name.lower() in self.surnames

    def _interfix_surname(self, name: str) -> bool:
        """Check if a name is in the interfix surnames lookup (case-insensitive)."""
        return name.lower() in self.interfix_surnames

    def _interfix(self, word: str) -> bool:
        """Check if a word is an interfix (case-insensitive)."""
        return word.lower() in self.interfixes

    def _common_word(self, word: str) -> bool:
        """Check if a word is a common Dutch word that should not be treated as a name."""
        return word.lower() in self.common_words

    def _stop_word(self, word: str) -> bool:
        """Check if a word is a stop word (case-insensitive)."""
        return word.lower() in self.stop_words


class DeduceHandler:
    """Handler class for enhanced Deduce de-identification operations."""

    def __init__(self) -> None:
        """Initialize DeduceHandler with a configured Deduce instance."""
        custom_lookup = Path(__file__).parent.parent / 'deduce' / 'lookup'

        # Always use custom lookup path first
        if custom_lookup.exists():
            self.lookup_data_path = str(custom_lookup)
        else:
            self.lookup_data_path = None

        self.deduce_instance = self._get_deduce_instance()

        # Load lookup tables once
        self.lookup_sets = self._load_lookup_tables()

        # Initialize custom detector using the same lookup path
        self.detector = DutchNameDetector(self.lookup_sets)

    def _get_deduce_instance(self) -> deduce.Deduce:
        """Get a configured Deduce instance with lookup data."""
        if self.lookup_data_path:
            return deduce.Deduce(lookup_data_path=self.lookup_data_path, cache_path=Path(self.lookup_data_path))

        # Use default Deduce lookup tables
        return deduce.Deduce()

    def _load_lookup_tables(self) -> dict[str, set[str]]:
        """Load all deduce name lookup tables and lowercase them."""
        if self.lookup_data_path:
            base_path = Path(self.lookup_data_path) / 'src' / 'names'
            common_words_path = Path(self.lookup_data_path) / 'src' / 'whitelist'
        else:
            deduce_lookup = Path(self.deduce_instance.lookup_data_path)
            base_path = deduce_lookup / 'src' / 'names'
            common_words_path = deduce_lookup / 'src' / 'whitelist'

        # Map attribute names to relative file paths
        lookup_files = {
            'first_names': base_path / 'lst_first_name' / 'items.txt',
            'surnames': base_path / 'lst_surname' / 'items.txt',
            'interfix_surnames': base_path / 'lst_interfix_surname' / 'items.txt',
            'interfixes': base_path / 'lst_interfix' / 'items.txt',
            'common_words': common_words_path / 'lst_common_word' / 'items.txt',
            'stop_words': common_words_path / 'lst_stop_word' / 'items.txt',
        }

        def load_word_set(path: Path) -> set[str]:
            """Load file lines into a lowercase set."""
            if path.exists():
                with path.open(encoding='utf-8') as file:
                    return {line.strip().lower() for line in file if line.strip()}
            return set()

        return {attr: load_word_set(file_path) for attr, file_path in lookup_files.items()}

    def deduce_with_id(self, input_cols: dict) -> Callable[[dict], str]:
        """Return an inner function configured to process rows with name detection."""

        def inner_func(row: dict) -> str:
            try:
                # First create a Deduce person based on patient name
                patient_name = str.split(row[input_cols['patientName']], ' ', maxsplit=1)
                patient_initials = ''.join([name[0] for name in patient_name])

                # Best results with input data that contains columns for: first names, surname, initials
                if len(patient_name) == 1:
                    deduce_patient = Person(first_names=[patient_name[0]])
                else:
                    deduce_patient = Person(
                        first_names=[patient_name[0]],
                        surname=patient_name[1],
                        initials=patient_initials,
                    )






                # Step 1: Apply standard Deduce processing
                report_text = row[input_cols['report']]

                test = report_text.lower()





                # deduce_result = self.deduce_instance.deidentify(
                #     report_text,
                #     metadata={'patient': deduce_patient},
                # )




                # print ('=' * 80)
                # print(report_text)


                # print('*' * 80)
                # print(deduce_result.deidentified_text)
                # print('- - - - - - - - - - - - - - - - - - - - - - - - - - -')
                # print(deduce_result.annotations)
                # print('*' * 80)


                # Step 2: Apply extended custom name detection
                custom_annotations = self.detector.names_case_insensitive(report_text)


                deduce_result = self.deduce_instance.deidentify(
                    report_text,
                    metadata={'patient': deduce_patient},
                )





                # Step 3: Merge results intelligently
                return self._merge_annotations(
                    deduce_result.deidentified_text,
                    custom_annotations,
                    report_text,
                )

            except (KeyError, IndexError, AttributeError, ValueError):
                # No error log as null rows will be collected later
                return ''

        return inner_func

    def _merge_annotations(self, deduce_text: str, custom_annotations: list[NameAnnotation], original_text: str) -> str:
        """Merge Deduce and custom annotations intelligently."""
        result_text = deduce_text

        # Find positions where Deduce missed names that custom detector found
        missed_annotations = []

        for annotation in custom_annotations:
            # Check if this position was already handled by Deduce
            original_segment = original_text[annotation.start : annotation.end]

            # If the text in deduce result still contains the original name
            # (meaning Deduce didn't detect it), we should replace it
            if original_segment.lower() in result_text.lower():
                missed_annotations.append(annotation)

        # Replace missed names with [PERSOON-X] tags
        person_counter = self._count_existing_tags(result_text) + 1

        for annotation in sorted(missed_annotations, key=lambda x: x.start):
            # Find the name in the current result text
            name_to_replace = original_text[annotation.start : annotation.end]

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
        """Count existing [PERSOON-X] tags in text."""
        pattern = r'\[PERSOON-\d+\]'
        return len(re.findall(pattern, text))

    def deduce_report_only(self, input_cols: dict) -> Callable[[dict], str]:
        """Apply the Deduce algorithm without patient info."""

        def inner_func(row: dict) -> str:
            try:
                report_deid = self.deduce_instance.deidentify(row[input_cols['report']])
            except (KeyError, AttributeError, ValueError):
                # No error log as null rows will be collected later
                return None
            else:
                return report_deid.deidentified_text

        return inner_func

    @staticmethod
    def replace_tags(text: str, new_value: str, tag: str = '[PATIENT]') -> str:
        """Replace patient tags with pseudonymized values."""
        try:
            return str.replace(text, tag, f'[{new_value}]')
        except (TypeError, AttributeError):
            return None
