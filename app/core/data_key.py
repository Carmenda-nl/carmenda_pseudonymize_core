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
    """Generates and manages pseudonyms for client names with synonym support."""

    def __init__(self, pseudonym_length: int = 14, max_iterations: int = 15) -> None:
        """Initialize the Pseudonymizer with configuration options."""
        self.pseudonym_length = pseudonym_length
        self.max_iterations = max_iterations

    def get_existing_key(self, data_key: DataKey, missing_names: list[str] | None = None) -> DataKey:
        """Load existing data key and add missing names."""
        for name in data_key:
            # Ensure all clients have a synonym field and pseudonym field
            name['Synoniemen'] = name.get('Synoniemen') or ''
            name['Code'] = name.get('Code') or ''

        # Add missing names to the data key
        if missing_names:
            for name in missing_names:
                logger.debug('Adding missing name to key: %s', name)
                data_key.append({'Clientnaam': name, 'Synoniemen': '', 'Code': ''})

        # Merge duplicate client names and combine their synonyms
        merged_client_names = {}

        for entry in data_key:
            clientname = entry['Clientnaam']
            synonym = entry['Synoniemen']
            pseudonym = entry['Code']

            if clientname in merged_client_names:
                if synonym and synonym not in merged_client_names[clientname]['Synoniemen'].split(', '):
                    if merged_client_names[clientname]['Synoniemen']:
                        merged_client_names[clientname]['Synoniemen'] += ', ' + synonym
                    else:
                        merged_client_names[clientname]['Synoniemen'] = synonym

                # Only update pseudonym if it's not already set
                if not merged_client_names[clientname]['Code'] and pseudonym:
                    merged_client_names[clientname]['Code'] = pseudonym
            else:
                merged_client_names[clientname] = entry.copy()

        return list(merged_client_names.values())

    def pseudonymize(self, data_key: DataKey) -> DataKey:
        """Generate missing pseudonyms for unique names."""
        chars = string.ascii_uppercase + string.digits

        # Keep track of existing pseudonyms to ensure duplicate prevention
        existing = {client['Code'] for client in data_key if client['Code']}

        for client in data_key:
            if not client['Code']:
                for _ in range(self.max_iterations):
                    pseudonym = ''.join(secrets.choice(chars) for _ in range(self.pseudonym_length))
                    if pseudonym not in existing:
                        client['Code'] = pseudonym
                        existing.add(pseudonym)  # <- Add to prevent new duplicates
                        break
                else:
                    logger.error('Failed to generate unique pseudonym.')

        return data_key
