# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Unit tests for surname detection.

For complete results, run with arguments.
`pytest -s -v`
"""

import sys
from pathlib import Path

from core.deidentify import DeidentifyHandler
from utils.logger import setup_test_logging
from utils.terminal import get_separator_line

# Add the source directory to the Python path
source_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(source_dir))


def test_surname_detection() -> None:
    """Test if input is properly detected as a surname."""
    handler = DeidentifyHandler()
    logger = setup_test_logging()

    test_names = [
        # Add test surnames for surname detection
        'naaldenberg',
        'naaldenberch',
        'vroegindeweij',
        'jassen',
        'janssen',
    ]

    logger.info('(start test)')
    logger.info('%s\n', get_separator_line())

    results = []
    for name in test_names:
        is_surname = handler.name_detector._surname(name)
        result_text = '✓ SURNAME' if is_surname else '✗ NOT SURNAME'
        logger.info('%s → %s', f'{name:<15}', result_text)
        results.append(is_surname)

    total_surnames = sum(results)
    logger.info('\nFound %d surnames out of %d names tested.\n', total_surnames, len(test_names))
    logger.info(get_separator_line())


if __name__ == '__main__':
    test_surname_detection()
