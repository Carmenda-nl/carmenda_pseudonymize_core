# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Anonymize Dutch report texts using Deduce with Polars for performance.

Script Name: polars_deduce.py
Authors: Django Heimgartner, Joep Tummers, Pim van Oirschot
Date: 2025-08-06
Description:
    This script deidentifies Dutch report texts (unstructured data) using Deduce
    (Menger et al. 2018, DOI: https://doi.org/10.1016/j.tele.2017.08.002., also on GitHub).
    The column/field in source data marked as patient names is used to generate unique codes for each value.
    This makes it possible to use the same code across entries.
    Polars allows for performance on large input data sizes.
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
    - Load data and when using dummy data, amplify to simulate real-world scenario
    - Apply transformations
        - Identify unique names based on patientName column and map them to randomized patientIDs.
        - Apply Deduce algorithm to report text column, making use of patientName to increase likelihood of at least
          de-identifying the main subject.
        - Replace the generated [PATIENT] tags with the new patientID
    - Write output to disk
"""

import argparse
from core.data_processor import process_data


def parse_cli_arguments() -> argparse.Namespace:
    """Parse command-line arguments for CLI usage.

    Returns:
        Parsed arguments

    """
    parser = argparse.ArgumentParser(
        description='Input parameters for the python program.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        '--input_fofi',
        nargs='?',
        default='dummy_input.csv',
        help="""
             Name of the input file. Currently .csv and .parquet files are supported.
             """,
    )
    parser.add_argument(
        '--input_cols',
        nargs='?',
        default='patientName=Cliëntnaam, report=rapport',
        help="""
             Input column names as a single string with comma separated key=value format e.g.
             Whitespaces are removed, so column names currently can't contain them.
             Keys patientName and report with values existing in the data are essential.
             """,
    )
    parser.add_argument(
        '--output_cols',
        nargs='?',
        default='patientID=patientID, processed_report=processed_report',
        help="""
             Output column names with same structure as input_cols. Take care when adding the reports column
             from input_cols, as this likely results in doubling of large data.
             """,
    )
    parser.add_argument(
        '--pseudonym_key',
        nargs='?',
        default=None,
        help="""
             Existing pseudonymization key to expand. Supply as JSON file with format {\'name\': \'pseudonym\'}.
             """,
    )
    parser.add_argument(
        '--output_extension',
        nargs='?',
        default='.parquet',
        help="""
             Select output format, currently only parquet (default) and csv are supported.
             CSV is supported to provide a human readable format, but is not recommended for (large) datasets
             with potential irregular fields (e.g. containing data that can be misinterpreted such as
             early string closure symbols followed by the separator symbol).
             """,
    )
    # parse and process arguments
    return parser.parse_args()


def main() -> None:
    """Parse command-line arguments, and call the main processing function.

    Command-line arguments:
      --input_fofi: Specifies the input folder or file
      --input_cols: Defines the mapping of input column names
      --output_cols: Defines the mapping of output column names
      --output_extension: Specifies the output format extensions

      --pseudonym_key: Path to an existing pseudonymization key in JSON format (optional)
    """
    args = parse_cli_arguments()

    process_data(
        input_fofi=args.input_fofi,
        input_cols=args.input_cols,
        output_cols=args.output_cols,
        pseudonym_key=args.pseudonym_key,
        output_extension=args.output_extension,
    )


if __name__ == '__main__':
    main()
