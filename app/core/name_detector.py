# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Dutch name detector module for case-insensitive name detection.

This module provides the DutchNameDetector class for detecting Dutch names
in text using lookup tables and pattern matching with case-insensitive support.
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
                # Pattern 3: Only detect first names that are:
                #   1. In the first names lookup table
                #   2. At least 3 characters long (to avoid false positives)
                #   3. Not common words or stop words
                'regex': r'\b([A-Za-zÀ-ÖØ-öø-ÿ]+)\b',  # alleen letters, geen cijfers
                'confidence': 0.70,
                'validate': lambda g: (
                    self._first_name(g[0].lower())
                    and len(g[0]) >= max_characters
                    and not self._common_word(g[0].lower())
                    and not self._stop_word(g[0].lower())
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
        ]

        # FIRST: Detect multi-word name combinations (3+ words) before single patterns
        self._detect_multi_word_names(text, annotations, is_overlapping)
        
        for idx, pattern_info in enumerate(patterns):
            for match in re.finditer(pattern_info['regex'], text, re.IGNORECASE):
                # Skip if overlapping with existing annotations (including multi-word)
                if is_overlapping(match.start(), match.end()):
                    continue

                if pattern_info['validate'](match.groups()):
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

    def _detect_multi_word_names(self, text: str, annotations: list, is_overlapping) -> None:
        """Detect multi-word name combinations (2+ words) and add them to annotations."""
        
        # Split text into segments on punctuation and conjunctions
        # This prevents names from spanning across multiple people
        separators = [',', ';', ' en ', ' EN ', ' and ', ' AND ', ' &', ' + ']
        segments = [text]
        
        # Split text into segments
        for separator in separators:
            new_segments = []
            for segment in segments:
                new_segments.extend(segment.split(separator))
            segments = new_segments
        
        # Process each segment separately for multi-word names
        for segment_text in segments:
            segment_text = segment_text.strip()
            if len(segment_text) < 3:  # Skip tiny segments
                continue
                
            # Find the start position of this segment in the original text
            segment_start = text.find(segment_text)
            if segment_start == -1:
                continue
            
            self._detect_names_in_segment(segment_text, segment_start, annotations, is_overlapping)
    
    def _detect_names_in_segment(self, segment_text: str, segment_start: int, annotations: list, is_overlapping) -> None:
        """Detect multi-word names within a single segment."""
        word_positions = []
        
        # Build word position map for this segment
        for match in re.finditer(r'\b[A-Za-zÀ-ÖØ-öø-ÿ]+\b', segment_text):
            word_positions.append({
                'word': match.group().lower(),
                'original': match.group(),
                'start': segment_start + match.start(),
                'end': segment_start + match.end(),
            })
        
        # Look for sequences of 2+ words that could be names
        # Start with longer sequences first (higher priority)
        for length in range(min(4, len(word_positions)), 1, -1):  # Max 4 words per name
            for i in range(len(word_positions) - length + 1):
                sequence = word_positions[i:i + length]
                
                # Check if this sequence forms a valid name combination
                if self._is_valid_name_sequence_flexible(sequence, length):
                    start_pos = sequence[0]['start']
                    end_pos = sequence[-1]['end']
                    
                    # Check for overlap with existing annotations
                    if not is_overlapping(start_pos, end_pos):
                        combined_text = segment_text[start_pos - segment_start:end_pos - segment_start]
                        confidence = self._calculate_sequence_confidence(sequence, length)
                        
                        annotations.append(
                            NameAnnotation(
                                start=start_pos,
                                end=end_pos,
                                text=combined_text,
                                tag='persoon',
                                confidence=confidence,
                            )
                        )
    
    def _is_valid_name_sequence_flexible(self, sequence: list, length: int) -> bool:
        """Check if a sequence of words forms a valid name combination with flexible criteria."""
        # For 2-word combinations (more strict)
        if length == 2:
            word1, word2 = sequence[0]['word'], sequence[1]['word']
            
            # Skip very short words or common words
            if (len(word1) < 3 or len(word2) < 3 or 
                self._common_word(word1) or self._common_word(word2) or
                self._stop_word(word1) or self._stop_word(word2)):
                return False
            
            # At least one must be a known name
            is_first_known = self._first_name(word1) or self._surname(word1)
            is_second_known = self._first_name(word2) or self._surname(word2)
            
            return is_first_known and is_second_known
        
        # For 3+ word combinations - stricter validation
        if length >= 3:
            # Check each word in the sequence
            known_names = 0
            unknown_but_valid = 0
            
            for word_info in sequence:
                word = word_info['word']
                original = word_info['original']
                
                # Skip very short words or obvious common words
                if len(word) < 3:
                    return False
                    
                # Check for obvious non-name words (verbs, adjectives, etc.)
                if self._is_likely_non_name(word):
                    return False
                
                if self._common_word(word) or self._stop_word(word):
                    return False
                
                if self._first_name(word) or self._surname(word):
                    known_names += 1
                elif self._looks_like_name(original):
                    unknown_but_valid += 1
                else:
                    return False  # Unknown word that doesn't look like a name
            
            # Need at least 2 known names OR 1 known + majority unknown-but-valid
            total_valid = known_names + unknown_but_valid
            return known_names >= 2 or (known_names >= 1 and unknown_but_valid >= known_names and total_valid == length)
            
        return False
    
    def _is_likely_non_name(self, word: str) -> bool:
        """Check if word is likely NOT a name (verb, adjective, etc.)."""
        # Common Dutch verbs, adjectives, and other non-name words
        non_name_words = {
            'hebben', 'zijn', 'was', 'waren', 'heeft', 'had', 'gaan', 'gaat', 'ging', 'gingen',
            'komt', 'kwam', 'komen', 'doen', 'doet', 'deed', 'deden', 'zien', 'ziet', 'zag', 'zagen',
            'wonen', 'woont', 'woonde', 'woonden', 'leven', 'leeft', 'leefde', 'leefden',
            'werken', 'werkt', 'werkte', 'werkten', 'lopen', 'loopt', 'liep', 'liepen',
            'groot', 'kleine', 'goed', 'goede', 'nieuwe', 'oude', 'lang', 'lange', 'kort', 'korte',
            'mooi', 'mooie', 'lelijk', 'lelijke', 'vrolijk', 'vrolijke', 'blij', 'blije',
            'huis', 'huizen', 'auto', 'autos', 'werk', 'school', 'straat', 'stad', 'land',
            'dag', 'dagen', 'week', 'weken', 'maand', 'maanden', 'jaar', 'jaren', 'tijd',
        }
        return word.lower() in non_name_words
    
    def _looks_like_name(self, original_word: str) -> bool:
        """Check if word looks like it could be a name."""
        # Basic criteria for name-like appearance
        if not original_word[0].isupper():
            return False
            
        if not re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ\-]+$', original_word):
            return False
            
        if len(original_word) < 3 or len(original_word) > 20:
            return False
            
        return True
    
    def _is_valid_name_sequence(self, sequence: list) -> bool:
        """Check if a sequence of words forms a valid name combination."""
        if len(sequence) < 3:
            return False
            
        # Count known names in the sequence
        first_name_count = 0
        surname_count = 0
        unknown_count = 0
        
        for word_info in sequence:
            word = word_info['word']
            
            # Skip very short words or common words
            if len(word) < 3 or self._common_word(word) or self._stop_word(word):
                return False
                
            if self._first_name(word):
                first_name_count += 1
            elif self._surname(word):
                surname_count += 1
            else:
                unknown_count += 1
        
        # Valid combinations (more flexible criteria):
        # 1. At least 1 known first name AND at least 1 known surname
        # 2. OR at least 2 known names (first or surname) with at most 1 unknown
        # 3. OR at least 1 known name with all unknown names being valid name-like patterns
        
        total_known = first_name_count + surname_count
        
        if first_name_count >= 1 and surname_count >= 1:
            return True  # Clear first name + surname pattern
            
        if total_known >= 2 and unknown_count <= 1:
            return True  # Mostly known names with maybe one unknown
            
        # NEW: Allow 1 known name if all unknown names look name-like
        if total_known >= 1 and self._all_words_name_like(sequence):
            return True  # At least one known name with name-like unknowns
            
        return False
    
    def _all_words_name_like(self, sequence: list) -> bool:
        """Check if all words in sequence appear to be name-like."""
        for word_info in sequence:
            word = word_info['word']
            original = word_info['original']
            
            # Name-like criteria:
            # 1. Starts with capital letter in original text
            # 2. Contains only letters (no numbers or special chars except hyphens)
            # 3. Not a common Dutch word
            # 4. Reasonable length (3-20 characters)
            
            if not original[0].isupper():
                return False
                
            if not re.match(r'^[A-Za-zÀ-ÖØ-öø-ÿ\-]+$', original):
                return False
                
            if self._common_word(word) or self._stop_word(word):
                return False
                
            if len(word) < 3 or len(word) > 20:
                return False
                
        return True
    
    def _calculate_sequence_confidence(self, sequence: list, length: int = 0) -> float:
        """Calculate confidence score for a name sequence."""
        base_confidence = 0.75
        
        first_name_count = 0
        surname_count = 0
        
        for word_info in sequence:
            word = word_info['word']
            if self._first_name(word):
                first_name_count += 1
            elif self._surname(word):
                surname_count += 1
        
        # Higher confidence for 2-word combinations that are both known names
        if length == 2 and first_name_count >= 1 and surname_count >= 1:
            base_confidence = 0.85
        
        # Boost confidence if we have both first names and surnames
        if first_name_count >= 1 and surname_count >= 1:
            base_confidence += 0.1
            
        # Boost confidence for more known names
        total_known = first_name_count + surname_count
        knowledge_ratio = total_known / len(sequence)
        base_confidence += knowledge_ratio * 0.1
        
        return min(base_confidence, 0.95)  # Cap at 0.95

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
