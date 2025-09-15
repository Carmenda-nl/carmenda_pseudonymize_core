# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Pseudonymization utilities for generating random string mappings.

This module provides functionality to create pseudonyms (random strings) for
unique names while maintaining consistent mappings.
"""

from __future__ import annotations

import secrets
import string

from utils.logger import setup_logging

DataKey = list[dict[str, str]]  # Type alias
logger = setup_logging()


class Pseudonymizer:
    """Generates and manages pseudonyms for patient names with synonym support."""

    def __init__(self, pseudonym_length: int = 14, max_iterations: int = 15) -> None:
        """Initialize the Pseudonymizer with configuration options."""
        self.pseudonym_length = pseudonym_length
        self.max_iterations = max_iterations

    def get_existing_key(self, data_key: DataKey, missing_names: list[str] | None = None) -> DataKey:
        """Load existing data key and add missing names."""
        for name in data_key:
            # Ensure all patients have a synonym field and pseudonym field
            name['synonym'] = name.get('synonym') or ''
            name['pseudonym'] = name.get('pseudonym') or ''

        # Add missing names to the data key
        if missing_names:
            for name in missing_names:
                logger.debug('Adding missing name to key: %s', name)
                data_key.append({'patient': name, 'synonym': '', 'pseudonym': ''})

        # Merge duplicate patient names and combine their synonyms
        merged_patient_names = {}

        for entry in data_key:
            patient = entry['patient']
            synonym = entry['synonym']
            pseudonym = entry['pseudonym']

            if patient in merged_patient_names:
                if synonym and synonym not in merged_patient_names[patient]['synonym'].split(', '):
                    if merged_patient_names[patient]['synonym']:
                        merged_patient_names[patient]['synonym'] += ', ' + synonym
                    else:
                        merged_patient_names[patient]['synonym'] = synonym

                # Only update pseudonym if it's not already set
                if not merged_patient_names[patient]['pseudonym'] and pseudonym:
                    merged_patient_names[patient]['pseudonym'] = pseudonym
            else:
                merged_patient_names[patient] = entry.copy()

        return list(merged_patient_names.values())

    def pseudonymize(self, data_key: DataKey) -> DataKey:
        """Generate missing pseudonyms for unique names."""
        chars = string.ascii_uppercase + string.digits

        # Keep track of existing pseudonyms to ensure duplicate prevention
        existing = {patient['pseudonym'] for patient in data_key if patient['pseudonym']}

        for patient in data_key:
            if not patient['pseudonym']:
                for _ in range(self.max_iterations):
                    pseudonym = ''.join(secrets.choice(chars) for _ in range(self.pseudonym_length))
                    if pseudonym not in existing:
                        patient['pseudonym'] = pseudonym
                        existing.add(pseudonym)  # <- Add to prevent new duplicates
                        break
                else:
                    logger.error('Failed to generate unique pseudonym.')

        return data_key
