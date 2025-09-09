# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Anonymize Dutch report texts.

Authors: Django Heimgartner, Joep Tummers, Pim van Oirschot

Description:
    Deidentifies Dutch report texts (unstructured data) using Deduce
    (Menger et al. 2018, DOI: https://doi.org/10.1016/j.tele.2017.08.002., also on GitHub).
    The column/field in source data marked as patient names is used to generate unique codes for each value.
    This makes it possible to use the same code across entries.

Disclaimer:
    This script applying Deduce is a best attempt to pseudonymize data and while we are dedicated to
    improve performance we cannot guarantee an absence of false negatives.
    In the current state, we consider this method useful in that it reduces the chance of a scenario where a researcher
    recognizes a person in a dataset.
    Datasets should always be handled with care because individuals are likely (re-)identifiable both through false
    negatives and from context in the unredacted parts of text.

Script logic:
    - Load relevant module elements
    - Define functions
    - Load data
    - Apply transformations
        - Identify unique names based on patientName column and map them to randomized patientIDs.
        - Apply algorithm to report text column, making use of patientName to increase likelihood of at least
          de-identifying the main subject.
        - Replace the generated [PATIENT] tags with the new patientID
    - Collect logging information
    - Write output to disk
"""

from __future__ import annotations

import argparse

from core.data_processor import process_data
from utils.logger import setup_logging


def parse_cli_arguments() -> argparse.Namespace:
    """Parse command-line arguments for CLI usage."""
    parser = argparse.ArgumentParser(
        description='Pseudonymize Dutch medical report texts using the Deduce algorithm.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog='Example: python main.py --input_fofi data.csv --output_extension .parquet',
    )

    parser.add_argument(
        '--input_fofi',
        nargs='?',
        default='dummy_input.csv',
        help='Name of the input file. Supported formats: .csv and .parquet',
    )
    parser.add_argument(
        '--input_cols',
        nargs='?',
        default='patientName=Cliëntnaam, report=rapport',
        help='Input column mappings as comma-separated key=value pairs. Required keys: patientName and report.',
    )
    parser.add_argument(
        '--output_cols',
        nargs='?',
        default='patientID=patientID, processed_report=processed_report',
        help='Output column mappings as comma-separated key=value pairs. Maps to: patientID and processed_report.',
    )
    parser.add_argument(
        '--pseudonym_key',
        nargs='?',
        default=None,
        help='Path to existing pseudonymization key (JSON file) to extend. Format: {"original_name": "pseudonym"}',
    )
    parser.add_argument(
        '--output_extension',
        nargs='?',
        default='.csv',
        choices=['.csv', '.parquet'],
        help='Output file format. Parquet recommended for large datasets.',
    )
    parser.add_argument(
        '--log_level',
        nargs='?',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level. If not specified, uses LOG_LEVEL environment variable or defaults to INFO.',
    )
    # Parse and process arguments
    return parser.parse_args()


def main() -> None:
    """Parse command-line arguments, and call the main processing function."""
    args = parse_cli_arguments()

    if args.log_level:
        log_level = args.log_level
    setup_logging(log_level)

    process_data(
        input_fofi=args.input_fofi,
        input_cols=args.input_cols,
        output_cols=args.output_cols,
        pseudonym_key=args.pseudonym_key,
        output_extension=args.output_extension,
    )


if __name__ == '__main__':
    main()
