# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Unit tests for case-insensitive name detection.

For complete results, run with arguments.
`pytest -s -v`
"""

import sys
from pathlib import Path
from services.logger import setup_test_logging
from core.deduce_handler import DeduceHandler
from utils.terminal import get_separator_line


# Add the source directory to the Python path
source_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(source_dir))


def test_case_insensitive() -> None:
    """Test the case-insensitive name detection."""
    handler = DeduceHandler()
    logger = setup_test_logging()

    test_sentences = [
        # Add test sentences here for case-insensitive detection
        'truus de rooij, henk de vries, en piet gingen fietsen',
        'monique naaldenberg, anja van der haar en gers wonen mooi',
        'joup, maarten van der poel, pim janssen hadden een goede dag',
    ]

    logger.info('(start test)')
    logger.info(get_separator_line())

    for sentence in test_sentences:
        logger.info("\nTesting: '%s'", sentence)

        # Test custom case-insensitive detector
        custom_annotations = handler.detector.names_case_insensitive(sentence)

        if custom_annotations:
            for ann in custom_annotations:
                logger.info("  Found: '%s' at %d-%d (confidence: %.2f)", ann.text, ann.start, ann.end, ann.confidence)
        else:
            logger.info('  Found: nothing')

    logger.info('\n%s', get_separator_line())


if __name__ == '__main__':
    test_case_insensitive()
