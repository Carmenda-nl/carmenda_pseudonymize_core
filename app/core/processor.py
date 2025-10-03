# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Data processing module for pseudonymizing data.

This module provides functionality for:
    - Loading data from files
    - Creating datakeys for patient names
    - Transforming and pseudonymizing patient data
    - Filtering null values and handling data irregularities
    - Writing processed data to output files
"""

from __future__ import annotations

import json
import logging
import sys
import time

import polars as pl

from core.datakey import process_datakey
from core.deidentify import DeidentifyHandler, replace_synonyms
from utils.file_handling import create_output_path, get_environment, load_data_file, save_data_file, save_datakey
from utils.logger import setup_logging
from utils.progress_tracker import tracker, performance_metrics

DataKey = list[dict[str, str]]  # Type alias
logger = setup_logging()


def _prepare_output_data(df: pl.DataFrame, input_cols: dict, output_cols: dict) -> pl.DataFrame:
    """Prepare data for output by selecting and renaming columns."""
    select_cols = [col for col in output_cols.values() if col in df.columns]
    df = df.select(select_cols)
    logger.info('Output columns: %s\n', df.columns)

    # Rename headers to their original input name
    rename_headers = {}

    if 'patientID' in df.columns and 'patientName' in output_cols:
        rename_headers['patientID'] = input_cols['patientName']
    if 'processed_report' in df.columns and 'report' in input_cols:
        rename_headers['processed_report'] = input_cols['report']

    if rename_headers:
        df = df.rename(rename_headers)

    return df


def _filter_null_rows(df: pl.DataFrame, input_cols: dict, output_folder: str) -> pl.DataFrame:
    """Filter out rows with null values in reports column and save them separately."""
    report_col = input_cols['report']

    # Only filter on report column
    filter_condition = pl.col(report_col).is_null()
    logger.info('Filtering rows with NULL only in report column: %s', report_col)

    # Collecting rows with problems
    df_with_nulls = df.filter(filter_condition)

    # If null rows found, collect rows and build file
    if not df_with_nulls.is_empty():
        try:
            logger.warning('Number of rows with problems in report column: %d\n', df_with_nulls.height)
            logger.debug('%s\n', df_with_nulls)

            logger.info('Attempting to write dataframe of rows with nulls to file.')
            output_file = create_output_path(output_folder, 'processed_with_nulls')
            save_data_file(df_with_nulls, output_file)

        except (OSError, PermissionError, ValueError):
            logger.exception('Problematic rows detected. Continuing with valid rows.')

        # Cleanup the problematic rows and keep the good ones
        if 'df_with_nulls' in locals():
            del df_with_nulls

        df = df.filter(~filter_condition)
        logger.info('Remaining rows after filtering rows with empty values: %d', df.height)
    else:
        logger.info('No problematic rows found!')

    return df










def process_data(input_file: str, input_cols: str, output_cols: str, datakey: str) -> str:
    """Process and pseudonymize data from input files and return in Json."""
    params = dict(locals().items())
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())
    logger.debug('Parsed arguments:\n%s\n', params_str)

    input_folder, output_folder = get_environment()

    # Start progress tracking
    tracker.update('Loading data')

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    input_file_path = f'{input_folder}/{input_file}' if not input_file.startswith('/') else input_file
    df = load_data_file(input_file_path)

    if df is not None:
        tracker.update('Pre-processing')

        # Convert string mappings to dictionaries
        input_cols_dict = dict(item.strip().split('=') for item in input_cols.split(','))
        output_cols_dict = dict(item.strip().split('=') for item in output_cols.split(','))

        # Check if the `clientname` column is available
        clientname_col = input_cols_dict.get('clientname')
        has_clientname = clientname_col in df.columns

        # Check if the `report` column is available
        report_col = input_cols_dict.get('report')
        has_report = report_col in df.columns

        start_time = time.time()
    else:
        msg = f'Input file "{input_file_path}" could not be loaded.'
        logger.error(msg)
        return json.dumps({'error': msg})

    if not has_report:
        msg = f'Report column "{report_col}" not found in input data.'
        logger.error(msg)
        return json.dumps({'error': msg})

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    if has_clientname:
        # Strip whitespace from clientnames
        df = df.with_columns(pl.col(clientname_col).str.strip_chars())

        datakey = process_datakey(df, input_cols_dict, datakey, input_folder)
        save_datakey(datakey, output_folder)
    else:
        logger.info('Clientname not provided, skipping datakey creation.\n')

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

    handler = DeidentifyHandler()
    tracker.update('Data transformation')

    if has_clientname:
        df = replace_synonyms(df, datakey, input_cols_dict)

    df = handler.deidentify_text(input_cols_dict, df, datakey)

    # if has_patient_name:
    #     # Create a new column `patientID` with datakeys
    #     name_to_pseudonym = {entry['Clientnaam']: entry['Code'] for entry in datakey}
    #     df = df.with_columns(
    #         pl.col(patient_name_col)
    #         # Obtain randomized string corresponding to name
    #         .replace_strict(name_to_pseudonym, default=None)
    #         .alias('patientID'),
    #     )

    #     # Replace [PATIENT] tags in `processed_report` with datakeys
    #     df = df.with_columns(
    #         pl.col('processed_report').str.replace_all(r'\[PATIENT\]', pl.format('[{}]', pl.col('patientID'))),
    #     )

    # # Prepare output data
    # df = _prepare_output_data(df, input_cols_dict, output_cols_dict)

    # Show pseudonymized reports in debug mode and when NOT running as a frozen executable
    # if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
    #     handler.debug_deidentify_text()

    # # Update progress - filtering nulls
    # tracker.update('Filtering nulls')

    # # ---------------------------- STEP 4: FILTERING NULLS ---------------------------- #

    # df = _filter_null_rows(df, input_cols_dict, output_folder)

    # # Only show example to terminal when NOT running as a frozen executable
    # if not getattr(sys, 'frozen', False):
    #     sys.stdout.write(f'\n{df}\n')

    # # Update progress - writing output
    # tracker.update('Writing output')

    # ----------------------------- STEP 5: WRITE OUTPUT ------------------------------ #

    performance_metrics(start_time, df.height)
    tracker.update('Finalizing')

    output_file = create_output_path(output_folder, input_file)
    save_data_file(df, output_file)

    # Extract first 10 rows as JSON for return value
    return json.dumps({'data': df.head(10).to_dicts()})
