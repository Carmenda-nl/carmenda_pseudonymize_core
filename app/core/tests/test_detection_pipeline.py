# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Deduce complete pipeline names detection test.

For complete results, run with arguments.
`pytest -s -v`
"""

import sys
from pathlib import Path

from core.deidentify_handler import DeidentifyHandler
from utils.logger import setup_test_logging
from utils.terminal import get_separator_line

# Add the source directory to the Python path
source_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(source_dir))


def test_name_detection_pipeline() -> None:
    """Test the full extended pipeline on different sentences."""
    handler = DeidentifyHandler()
    logger = setup_test_logging()

    test_data = [
        {
            'patientName': 'Truus de Rooij',
            'patientInitials': 'TR',
            'report': 'truus de rooij, henk de vries, en piet gingen fietsen naar het ziekenhuis',
        },
        {
            'patientName': 'Monique Naaldenberg',
            'patientInitials': 'MN',
            'report': 'monique naaldenberg, anja van der haar en gers wonen mooi in amsterdam',
        },
        {
            'patientName': 'Joup Janssen',
            'patientInitials': 'JJ',
            'report': 'joup, maarten van der poel, pim hadden een goede dag in de kliniek',
        },
    ]

    # Column mapping for the deduce handler
    input_cols = {
        'patientName': 'patientName',
        'patientInitials': 'patientInitials',
        'report': 'report',
    }

    # Get the deduce function for the full pipeline
    deidentify_text = handler.deidentify_text(input_cols)

    logger.info('(start test)')
    logger.info(get_separator_line())

    for case_number, test_case in enumerate(test_data, 1):
        logger.info('\nTEST CASE %d:', case_number)
        logger.info('  Patient:  Patient name: %s (%s)', test_case['patientName'], test_case['patientInitials'])
        logger.info("  Input:   '%s'", test_case['report'])

        # Apply the full deduce pipeline
        deidentified_text = deidentify_text(test_case)
        logger.info("  Output:  '%s'\n", deidentified_text)

        # Show what the detector alone would find for comparison
        custom_annotations = handler.name_detector.names_case_insensitive(test_case['report'])
        if custom_annotations:
            logger.info('  Detector found:')
            for ann in custom_annotations:
                logger.info("    '%s' at %d-%d (confidence: %.2f)", ann.text, ann.start, ann.end, ann.confidence)
        else:
            logger.info('    Detector found no additional names')

    logger.info('\n%s', get_separator_line())


if __name__ == '__main__':
    test_name_detection_pipeline()
