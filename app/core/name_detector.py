# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Dutch name detector module for case-insensitive name detection.

Functionality for detecting Dutch names in text using lookup tables
and pattern matching with case-insensitive support.
"""

from __future__ import annotations

import re


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
        self.name_prefixes = lookup_sets.get('name_prefixes', set())
        self.first_names = lookup_sets.get('first_names', set())
        self.surnames = lookup_sets.get('surnames', set())
        self.interfix_surnames = lookup_sets.get('interfix_surnames', set())
        self.interfixes = lookup_sets.get('interfixes', set())
        self.common_words = lookup_sets.get('common_words', set())
        self.stop_words = lookup_sets.get('stop_words', set())

        # Load interfix patterns for regex patterns
        self.interfix_pattern = self._interfix_patterns()

    def _prefix_before_surname(self, text: str, match_start: int) -> bool:
        """Check if there is a valid name prefix before a potential surname."""
        prefix_text = text[:match_start].strip()

        # Split on whitespace and get the last few words
        words = prefix_text.split()
        if not words:
            return False

        # Check the last word (most likely to be a prefix)
        last_word = words[-1].lower().rstrip('.,;:!?')
        if last_word in self.name_prefixes:
            return True

        # Also check the second-to-last word in case there's punctuation
        min_words_for_second_check = 2
        if len(words) >= min_words_for_second_check:
            second_last = words[-2].lower().rstrip('.,;:!?')
            if second_last in self.name_prefixes:
                return True

        return False

    def _surname_with_prefix(self, match: re.Match[str], text: str, max_characters: int) -> bool:
        """Validate a surname match by checking if it has a valid prefix."""
        groups = match.groups()
        surname = groups[0].lower()

        # First check if it's actually a surname and meets basic criteria
        if not (
            self._surname(surname)
            and len(groups[0]) >= max_characters
            and surname not in self.common_words
            and surname not in self.stop_words
        ):
            return False

        # If it's also a first name, only detect it as surname if it has a prefix
        if self._first_name(surname):
            return self._prefix_before_surname(text, match.start())

        # For pure surnames (not first names), still require prefix
        return self._prefix_before_surname(text, match.start())

    def _interfix_patterns(self) -> str:
        """Build regex pattern for interfixes from lookup data."""
        sorted_interfixes = sorted(self.interfixes, key=len, reverse=True)

        # Escape special regex characters and join with |
        escaped_interfixes = [re.escape(interfix) for interfix in sorted_interfixes]

        return '(?:' + '|'.join(escaped_interfixes) + ')'

    def _regex_patterns(self, max_characters: int) -> list[dict]:
        """Regex patterns validation logic for name detection."""
        return [
            {
                # Pattern 1: First name + interfix + surname
                'regex': rf'\b(\w+)\s+({self.interfix_pattern})\s+(\w+)\b',
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
                # Pattern 3: Only detect first names that are:
                #   1. In the first names lookup table
                #   2. At least 3 characters long (to avoid false positives)
                #   3. Not common words or stop words
                'regex': r'\b([A-Za-zÀ-ÖØ-öø-ÿ]+)\b',
                'confidence': 0.70,
                'validate': lambda g: (
                    self._first_name(g[0].lower())
                    and len(g[0]) >= max_characters
                    and not self._common_word(g[0].lower())
                    and not self._stop_word(g[0].lower())
                ),
            },
            {
                # Pattern 4: Only detect surnames when they have a valid prefix
                'regex': r"\b([A-Za-zÀ-ÖØ-öø-ÿ'\-]+)\b",
                'confidence': 0.80,
                'validate': 'surname_with_prefix',
            },
        ]

    def _overlapping(self, match_start: int, match_end: int, annotations: list) -> bool:
        """Check if a match overlaps with any existing annotation."""
        return any(ann.start <= match_start < ann.end or ann.start < match_end <= ann.end for ann in annotations)

    def names_case_insensitive(self, text: str) -> list[NameAnnotation]:
        """Detect Dutch names in text case-insensitively."""
        annotations: list[NameAnnotation] = []

        max_characters = 3
        patterns = self._regex_patterns(max_characters)

        # Detect multi-word names steps
        self._detect_multi_words(text, annotations, lambda start, end: self._overlapping(start, end, annotations))

        for pattern_info in patterns:
            for match in re.finditer(pattern_info['regex'], text, re.IGNORECASE):
                # Skip if overlapping with existing annotations (including multi-word)
                if self._overlapping(match.start(), match.end(), annotations):
                    continue

                # Handle special validation for surnames with prefix
                if pattern_info['validate'] == 'surname_with_prefix':
                    if self._surname_with_prefix(match, text, max_characters):
                        annotations.append(
                            NameAnnotation(
                                start=match.start(),
                                end=match.end(),
                                text=match.group(0),
                                tag='persoon',
                                confidence=pattern_info['confidence'],
                            ),
                        )
                # Handle regular lambda validation
                elif callable(pattern_info['validate']) and pattern_info['validate'](match.groups()):
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
        annotations.sort(key=lambda annotation: (-annotation.confidence, annotation.start))

        return annotations

    def _detect_multi_words(self, text: str, annotations: list, _overlapping: callable) -> None:
        """Detect multi-word name combinations (2+ words) and add them to annotations."""
        separators = [',', ';', '&', ' en ', ' EN ', ' of ', ' OF ', ' met ', ' MET ']
        max_characters = 3

        # Split text into segments on punctuation and conjunctions
        segments = [text]
        for separator in separators:
            segments = [seg for segment in segments for seg in segment.split(separator)]

        # Process each segment separately for multi-word names
        for segment_text in segments:
            clean_segment = segment_text.strip()
            if len(clean_segment) < max_characters:  # Skip tiny segments
                continue

            # Find the start position of this segment in the original text
            segment_start = text.find(clean_segment)
            if segment_start == -1:
                continue

            self._words_segment(clean_segment, segment_start, annotations, _overlapping)

    def _words_segment(self, segment: str, segment_start: int, annotations: list, _overlapping: callable) -> None:
        """Detect multi-word names within a single segment."""
        word_positions = [
            {
                'word': match.group().lower(),
                'original': match.group(),
                'start': segment_start + match.start(),
                'end': segment_start + match.end(),
            }
            for match in re.finditer(r'\b[A-Za-zÀ-ÖØ-öø-ÿ]+\b', segment)
        ]

        # Look for sequences of 2+ words that could be names, (longer sequences first)
        for length in range(min(4, len(word_positions)), 1, -1):  # Max 4 words per name
            for item in range(len(word_positions) - length + 1):
                sequence = word_positions[item : item + length]

                # Check if this sequence forms a valid name combination
                if self._valid_name_sequence(sequence, length):
                    start_pos = sequence[0]['start']
                    end_pos = sequence[-1]['end']

                    # Check for overlap with existing annotations
                    if not _overlapping(start_pos, end_pos):
                        combined_text = segment[start_pos - segment_start : end_pos - segment_start]
                        confidence = self._sequence_confidence(sequence, length)

                        annotations.append(
                            NameAnnotation(
                                start=start_pos,
                                end=end_pos,
                                text=combined_text,
                                tag='persoon',
                                confidence=confidence,
                            ),
                        )

    def _valid_name_sequence(self, sequence: list, length: int) -> bool:
        """Check if a sequence of words forms a valid name combination."""
        two_words = 2
        three_words = 3

        if length == two_words:
            word_1, word_2 = sequence[0]['word'], sequence[1]['word']

            # Skip very short words or common words
            if (
                len(word_1) < three_words
                or len(word_2) < three_words
                or self._common_word(word_1)
                or self._common_word(word_2)
                or self._stop_word(word_1)
                or self._stop_word(word_2)
            ):
                return False

            # At least one must be a known name
            first_known = self._first_name(word_1) or self._surname(word_1)
            second_known = self._first_name(word_2) or self._surname(word_2)
            return first_known and second_known

        if length >= three_words:
            # Check each word in the sequence
            known_names = 0
            unknown_but_valid = 0

            for word_info in sequence:
                word = word_info['word']
                original = word_info['original']

                if len(word) < three_words or self._common_word(word) or self._stop_word(word):
                    return False

                if self._first_name(word) or self._surname(word):
                    known_names += 1
                elif self._possible_name(original):
                    unknown_but_valid += 1
                else:
                    return False

            total_valid = known_names + unknown_but_valid
            return known_names >= two_words or (
                known_names >= 1 and unknown_but_valid >= known_names and total_valid == length
            )

        return False

    def _possible_name(self, original_word: str) -> bool:
        """Check if word looks like it could be a name."""
        small_word = 3
        big_word = 20

        if not original_word[0].isupper():
            return False
        if not re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ\-]+$', original_word):
            return False
        return not (len(original_word) < small_word or len(original_word) > big_word)

    def _sequence_confidence(self, sequence: list, length: int = 0) -> float:
        """Calculate confidence score for a name sequence."""
        base_confidence = 0.75
        first_name_count = 0
        surname_count = 0
        max_words = 2

        for word_info in sequence:
            word = word_info['word']

            if self._first_name(word):
                first_name_count += 1
            elif self._surname(word):
                surname_count += 1

        # Higher confidence for 2-word combinations that are both known names
        if length == max_words and first_name_count >= 1 and surname_count >= 1:
            base_confidence = 0.85

        # Boost confidence if we have both first names and surnames
        if first_name_count >= 1 and surname_count >= 1:
            base_confidence += 0.1

        # Boost confidence for more known names
        total_known = first_name_count + surname_count
        knowledge_ratio = total_known / len(sequence)
        base_confidence += knowledge_ratio * 0.1

        return min(base_confidence, 0.95)  # cap at highest score

    # ---------------------------- LOOKUP TABLE QUERIES ---------------------------- #

    def _in_lookup(self, word: str, lookup_set: set[str]) -> bool:
        """Check if a word exists in the given lookup set (case-insensitive)."""
        return word.lower() in lookup_set

    def _first_name(self, name: str) -> bool:
        """Check if a name is a first name."""
        return self._in_lookup(name, self.first_names)

    def _surname(self, name: str) -> bool:
        """Check if a name is a surname."""
        return self._in_lookup(name, self.surnames)

    def _interfix_surname(self, name: str) -> bool:
        """Check if a name is an interfix surname."""
        return self._in_lookup(name, self.interfix_surnames)

    def _interfix(self, word: str) -> bool:
        """Check if a word is an interfix."""
        return self._in_lookup(word, self.interfixes)

    def _common_word(self, word: str) -> bool:
        """Check if a word is a common Dutch word."""
        return self._in_lookup(word, self.common_words)

    def _stop_word(self, word: str) -> bool:
        """Check if a word is a stop word."""
        return self._in_lookup(word, self.stop_words)
