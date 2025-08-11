# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Deduce handler module for text de-identification and pseudonymization.

This module provides functions for processing medical text data using the Deduce
de-identification library with enhanced case-insensitive name detection. It includes:

    - deduce_with_id: De-identify text using patient name information (enhanced)
    - replace_tags: Replace patient tags with pseudonymized values
    - deduce_report_only: De-identify text without patient context
"""

from __future__ import annotations

import sys
import shutil
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

    def __init__(self, lookup_data_path: str | None = None) -> None:
        """Initialize with lookup data."""
        self.lookup_data_path = lookup_data_path
        self.first_names: set[str] = set()
        self.surnames: set[str] = set()
        self.interfix_surnames: set[str] = set()
        self.interfixes: set[str] = set()

        self._load_lookup_tables()

    def _load_lookup_tables(self) -> None:
        """Load all deduce name lookup tables and lowercase them."""
        if self.lookup_data_path:
            # Use custom lookup path if provided
            base_path = Path(self.lookup_data_path) / 'src' / 'names'
        else:
            # Use default Deduce lookup path
            deduce_init = deduce.Deduce()
            base_path = Path(deduce_init.lookup_data_path) / 'src' / 'names'

        # Load first names (case-insensitive)
        first_name_file = base_path / 'lst_first_name' / 'items.txt'
        if first_name_file.exists():
            with first_name_file.open(encoding='utf-8') as name_file:
                # Stripping whitespace and converting to lowercase
                self.first_names = {line.strip().lower() for line in name_file if line.strip()}

        # Load surnames (case-insensitive)
        surname_file = base_path / 'lst_surname' / 'items.txt'
        if surname_file.exists():
            with surname_file.open(encoding='utf-8') as surname_file:
                # Stripping whitespace and converting to lowercase
                self.surnames = {line.strip().lower() for line in surname_file if line.strip()}

        # Load interfix surnames (case-insensitive)
        interfix_surname_file = base_path / 'lst_interfix_surname' / 'items.txt'
        if interfix_surname_file.exists():
            with interfix_surname_file.open(encoding='utf-8') as surname_interfix_file:
                # Stripping whitespace and converting to lowercase
                self.interfix_surnames = {line.strip().lower() for line in surname_interfix_file if line.strip()}

        # Load interfixes (case-insensitive)
        interfix_file = base_path / 'lst_interfix' / 'items.txt'
        if interfix_file.exists():
            with interfix_file.open(encoding='utf-8') as interfix_file:
                # Stripping whitespace and converting to lowercase
                self.interfixes = {line.strip().lower() for line in interfix_file if line.strip()}

    def names_case_insensitive(self, text: str) -> list[NameAnnotation]:
        """Detect Dutch names in text case-insensitively."""
        print(f'REPORT TEXT: {text}')
        annotations = []

        # Pattern 1: First name + interfix + surname (e.g., "truus de rooij")
        pattern_1 = r'\b(\w+)\s+(de|van|der|den|van\s+der|van\s+den|te|ter|ten|tot)\s+(\w+)\b'

        for match in re.finditer(pattern_1, text, re.IGNORECASE):
            first_name = match.group(1).lower()
            interfix = match.group(2).lower()
            last_name = match.group(3).lower()

            # print('*******************************************************')
            # print(f'found match {first_name}')
            # print(f'found match {interfix}')
            # print(f'found match {last_name}')
            # print('*******************************************************')

            if (self._first_name(first_name) and
                self._interfix(interfix) and
                self._interfix_surname(last_name)):

                annotations.append(NameAnnotation(
                    start=match.start(),
                    end=match.end(),
                    text=match.group(0),
                    tag='persoon',
                    confidence=0.95,
                ))

            # print(f"Match found: {match.group(0)} at positions {match.start()}-{match.end()}")

        # Pattern 2: First name + surname (e.g., "truus bakker")
        pattern_2 = r'\b(\w+)\s+(\w+)\b'

        for match in re.finditer(pattern_2, text, re.IGNORECASE):
            # Skip if already matched by pattern 1
            if any(ann.start <= match.start() < ann.end or
                   ann.start < match.end() <= ann.end for ann in annotations):
                continue

            first_name = match.group(1).lower()
            last_name = match.group(2).lower()

            if (self._is_first_name(first_name) and
                self._is_surname(last_name)):

                annotations.append(NameAnnotation(
                    start=match.start(),
                    end=match.end(),
                    text=match.group(0),
                    tag='persoon',
                    confidence=0.85,
                ))

        # Pattern 3: Single first names (e.g., "piet", "mieke")
        pattern_3 = r'\b(\w+)\b'

        for match in re.finditer(pattern_3, text, re.IGNORECASE):
            # Skip if already matched by previous patterns
            if any(ann.start <= match.start() < ann.end or
                   ann.start < match.end() <= ann.end for ann in annotations):
                continue

            word = match.group(1).lower()

            # Only detect single first names that are:
            # 1. In the first names lookup table
            # 2. At least 3 characters long (to avoid false positives)
            # 3. Not common words

            max_characters = 3

            if (self._first_name(word) and
                len(word) >= max_characters and
                not self._common_word(word)):

                annotations.append(NameAnnotation(
                    start=match.start(),
                    end=match.end(),
                    text=match.group(0),
                    tag='persoon',
                    confidence=0.70,
                ))

        # Sort by confidence and position
        annotations.sort(key=lambda x: (-x.confidence, x.start))
        return annotations

    def _first_name(self, name: str) -> bool:
        """Check if a name is in the first names lookup (case-insensitive)."""
        return name.lower() in self.first_names

    def _surname(self, name: str) -> bool:
        """Check if a name is in the surnames lookup (case-insensitive)."""
        return name.lower() in self.surnames

    def _interfix_surname(self, name: str) -> bool:
        """Check if a name is in the interfix surnames lookup (case-insensitive)."""
        return name.lower() in self.interfix_surnames

    def _interfix(self, word: str) -> bool:
        """Check if a word is an interfix (case-insensitive)."""
        return word.lower() in self.interfixes

    def _common_word(self, word: str) -> bool:
        """Check if a word is a common Dutch word that should not be treated as a name."""
        common_words = {
            'de', 'het', 'een', 'en', 'van', 'te', 'dat', 'die', 'in', 'op', 'is', 'aan', 'als', 'voor',
            'met', 'was', 'hij', 'ze', 'haar', 'hem', 'hebben', 'had', 'dit', 'wat', 'er', 'maar',
            'om', 'worden', 'nog', 'zal', 'bij', 'jaar', 'werd', 'zeer', 'onder', 'tegen', 'na',
            'ook', 'tot', 'over', 'dan', 'uit', 'kan', 'niet', 'wel', 'door', 'naar', 'zou',
            'patient', 'patiënt', 'mevrouw', 'meneer', 'dokter', 'zuster', 'arts', 'verpleegster',
            'gaat', 'komt', 'heeft', 'zijn', 'waren', 'hadden', 'ging', 'gingen', 'kwam', 'kwamen',
            'wandelen', 'fietsen', 'eten', 'drinken', 'vrolijk', 'mooi', 'goed', 'dag', 'tijd', 'pijn',
            'ijs', 'patat', 'hier', 'daar', 'vandaag', 'morgen', 'gisteren', 'weer', 'wonen', 'woont',
        }
        return word.lower() in common_words


class DeduceHandler:
    """Handler class for enhanced Deduce de-identification operations."""

    def __init__(self) -> None:
        """Initialize DeduceHandler with a configured Deduce instance."""
        self.deduce_instance = self._get_deduce_instance()

        # Initialize custom detector using default Deduce lookup tables
        self.detector = DutchNameDetector()

    def _get_deduce_instance(self) -> deduce.Deduce:
        """Get a configured Deduce instance with lookup data."""
        if getattr(sys, 'frozen', False):
            lookup_data = Path('data') / 'deduce' / 'data' / 'lookup'

            if not lookup_data.exists():
                lookup_cached = Path(getattr(sys, '_MEIPASS', '')) / 'deduce' / 'data' / 'lookup'
                shutil.copytree(lookup_cached, lookup_data, dirs_exist_ok=True)

            return deduce.Deduce(lookup_data_path=str(lookup_data), cache_path=str(lookup_data))

        # Use default Deduce lookup tables
        return deduce.Deduce()

    def deduce_with_id(self, input_cols: dict) -> Callable[[dict], str]:
        """Return an inner function configured to process rows with name detection."""

        def inner_func(row: dict) -> str:
            try:
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
                deduce_result = self.deduce_instance.deidentify(
                    report_text,
                    metadata={'patient': deduce_patient},
                )

                # Step 2: Apply extended custom name detection
                custom_annotations = self.detector.names_case_insensitive(report_text)

                # Step 3: Merge results intelligently

                # print(deduce_result.deidentified_text)

                # print('===========')
                # print(report_text)

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
            original_segment = original_text[annotation.start:annotation.end]

            # If the text in deduce result still contains the original name
            # (meaning Deduce didn't detect it), we should replace it
            if original_segment.lower() in result_text.lower():
                missed_annotations.append(custom_annotations)

        # Replace missed names with [PERSOON-X] tags
        person_counter = self._count_existing_tags(result_text) + 1

        for annotation in sorted(missed_annotations, key=lambda x: x.start):
            # Find the name in the current result text
            name_to_replace = original_text[annotation.start:annotation.end]

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
