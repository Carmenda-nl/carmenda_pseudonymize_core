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

import polars as pl

from core.deidentify import DeidentifyHandler
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
            'clientname': 'Truus de Rooij',
            'client_initials': 'TR',
            'report': 'truus de rooij, henk de vries, en piet gingen fietsen naar het ziekenhuis',
        },
        {
            'clientname': 'Monique Naaldenberg',
            'client_initials': 'MN',
            'report': 'monique naaldenberg, anja van der haar en gers wonen mooi in amsterdam',
        },
        {
            'clientname': 'Joup Janssen',
            'client_initials': 'JJ',
            'report': 'joup, maarten van der poel, pim hadden een goede dag in de kliniek',
        },
    ]

    # Column mapping for the deduce handler
    input_cols = {
        'clientname': 'clientname',
        'report': 'report',
    }

    # Create DataFrame from test data
    df = pl.DataFrame(test_data)

    # Create an empty datakey (not needed for this test)
    datakey = pl.DataFrame()

    logger.info('(start test)')
    logger.info(get_separator_line())

    # Process the dataframe
    result_df = handler.deidentify_text(df, datakey, input_cols)

    # Iterate through results and log them
    for case_number, row in enumerate(result_df.iter_rows(named=True), 1):
        logger.info('\nTEST CASE %d:', case_number)
        logger.info('  Client:  Client name: %s (%s)', row['clientname'], row['client_initials'])
        logger.info("  Input:   '%s'", row['report'])
        logger.info("  Output:  '%s'\n", row['processed_report'])

        # Show what the detector alone would find for comparison
        custom_annotations = handler.name_detector.names_case_insensitive(row['report'])
        if custom_annotations:
            logger.info('  Detector found:')
            for ann in custom_annotations:
                logger.info("    '%s' at %d-%d (confidence: %.2f)", ann.text, ann.start, ann.end, ann.confidence)
        else:
            logger.info('    Detector found no additional names')

    logger.info('\n%s', get_separator_line())


if __name__ == '__main__':
    test_name_detection_pipeline()
