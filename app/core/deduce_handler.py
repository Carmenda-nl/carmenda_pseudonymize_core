# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Deduce handler module for text de-identification and pseudonymization.

This module provides functions for processing medical text data using the Deduce
de-identification library. It includes:

    - deduce_with_id: De-identify text using patient name information
    - replace_tags: Replace patient tags with pseudonymized values
    - deduce_report_only: De-identify text without patient context
"""

import sys
import shutil
import deduce

from pathlib import Path
from typing import Callable
from deduce.person import Person


class DeduceHandler:
    """Handler class for Deduce de-identification operations."""

    def __init__(self) -> None:
        """Initialize DeduceHandler with a configured Deduce instance."""
        self.deduce_instance = self._get_deduce_instance()

    def _get_deduce_instance(self) -> deduce.Deduce:
        if getattr(sys, 'frozen', False):
            lookup_data = Path('data') / 'deduce' / 'data' / 'lookup'

            if not lookup_data.exists():
                lookup_cached = Path(getattr(sys, '_MEIPASS', '')) / 'deduce' / 'data' / 'lookup'
                shutil.copytree(lookup_cached, lookup_data, dirs_exist_ok=True)

            return deduce.Deduce(lookup_data_path=str(lookup_data), cache_path=str(lookup_data))

        return deduce.Deduce()

    def deduce_with_id(self, input_cols: dict) -> Callable[[dict], str]:
        """Return an inner function configured to process rows with patient name context."""

        def inner_func(row: dict) -> str:
            try:
                patient_name = str.split(row[input_cols['patientName']], ' ', maxsplit=1)
                patient_initials = ''.join([name[0] for name in patient_name])

                # best results with input data that contains columns for: first names, surname, initials
                if len(patient_name) == 1:
                    deduce_patient = Person(first_names=[patient_name[0]])
                else:
                    deduce_patient = Person(
                        first_names=[patient_name[0]],
                        surname=patient_name[1],
                        initials=patient_initials,
                    )

                report_deid = self.deduce_instance.deidentify(
                    row[input_cols['report']],
                    metadata={'patient': deduce_patient},
                )

            except (KeyError, IndexError, AttributeError, ValueError):
                # no error log as null rows will be collected later
                return ''
            else:
                return report_deid.deidentified_text

        return inner_func

    def deduce_report_only(self, input_cols: dict) -> Callable[[dict], str]:
        """Apply the Deduce algorithm without patient info."""

        def inner_func(row: dict) -> str:
            try:
                report_deid = self.deduce_instance.deidentify(row[input_cols['report']])
            except (KeyError, AttributeError, ValueError):
                # no error log as null rows will be collected later
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
