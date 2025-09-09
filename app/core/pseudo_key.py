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

logger = setup_logging()


class Pseudonymizer:
    """Generates and manages pseudonyms for patient names."""

    def __init__(self, pseudonym_length: int = 14, max_iterations: int = 15) -> None:
        """Initialize the Pseudonymizer with configuration options."""
        self.pseudonym_length = pseudonym_length
        self.max_iterations = max_iterations
        self.pseudonym_key: dict[str, str] = {}

    def get_existing_key(self, existing_key: dict[str, str] | None = None) -> None:
        """Load existing pseudonym key."""
        if existing_key:
            self.pseudonym_key = existing_key.copy()
            if logger:
                logger.info('Loaded existing key with %d entries', len(self.pseudonym_key))
        else:
            self.pseudonym_key = {}
            if logger:
                logger.info('Building new key')

    def _generate_candidate(self) -> str:
        """Generate a random pseudonym candidate."""
        chars = string.ascii_uppercase + string.digits
        return ''.join(secrets.choice(chars) for _ in range(self.pseudonym_length))

    def _is_unique(self, candidate: str) -> bool:
        """Check if pseudonym candidate is unique."""
        return candidate not in self.pseudonym_key.values()

    def pseudonymize(self, unique_names: list[str]) -> dict[str, str]:
        """Generate pseudonyms for unique names."""
        filtered_names = list(unique_names)

        for name in filtered_names:
            if name not in self.pseudonym_key:
                self._create_pseudonym_for_name(name)

        self._validate_result(filtered_names)
        return self.pseudonym_key.copy()

    def _create_pseudonym_for_name(self, name: str) -> None:
        """Create unique pseudonym for a single name."""
        for _ in range(self.max_iterations):
            candidate = self._generate_candidate()

            if self._is_unique(candidate):
                self.pseudonym_key[name] = candidate
                return

        error_msg = f'Failed to generate unique pseudonym for "{name}" after {self.max_iterations} attempts'
        raise RuntimeError(error_msg)

    def _validate_result(self, expected_names: list[str]) -> None:
        """Validate that all names have pseudonyms."""
        if len(expected_names) != len(self.pseudonym_key):
            error_msg = 'Unique_names (input) and pseudonym_key (output) do not have the same length'
            raise AssertionError(error_msg)
