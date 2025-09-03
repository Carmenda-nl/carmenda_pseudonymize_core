# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Data processing module for pseudonymizing data.

This module provides functionality for:
    - Loading data from files
    - Creating pseudonym keys for patient names
    - Transforming and pseudonymizing patient data
    - Filtering null values and handling data irregularities
    - Writing processed data to output files
"""

import json
import logging
import sys
import time
from functools import reduce

import polars as pl

from core.deidentify_handler import DeidentifyHandler
from core.pseudo_key import Pseudonymizer
from utils.file_handling import (
    create_output_file_path,
    get_environment_paths,
    get_file_extension,
    get_file_size,
    load_data_file,
    load_pseudonym_key,
    save_data_file,
    save_pseudonym_key,
)
from utils.logger import setup_logging
from utils.progress_tracker import progress_tracker, tracker

logger = setup_logging()
handler = DeidentifyHandler()


def _load_data_file(input_file_path: str) -> pl.DataFrame:
    """Load data file and log relevant information."""
    input_extension = get_file_extension(input_file_path)

    if input_extension == '.csv':
        file_size = get_file_size(input_file_path)
        logger.info('CSV input file of size: %s', file_size)

    # Load data using file utilities
    df = load_data_file(input_file_path)

    # Log a columns schema
    schema_str = 'root\n' + '\n'.join([f' |-- {name}: {dtype}' for name, dtype in df.schema.items()])
    logger.debug('%s \n', schema_str)

    # Count rows
    df_rowcount = df.height
    logger.info('Row count: %d', df_rowcount)
    return df


def _parse_column_mappings(input_cols: str, output_cols: str) -> tuple[dict[str, str], dict[str, str]]:
    """Parse column mapping strings into dictionaries."""
    input_cols = dict(item.strip().split('=') for item in input_cols.split(','))
    output_cols = dict(item.strip().split('=') for item in output_cols.split(','))

    return input_cols, output_cols


def _create_key(df: pl.DataFrame, input_cols: dict, pseudonym_key_dict: dict) -> dict[str, str]:
    """Create pseudonym key for patient names."""
    unique_names = (
        df.select(input_cols['patientName'])
        .filter(pl.col(input_cols['patientName']).is_not_null())
        .unique()
        .to_series()
    )

    pseudonymizer = Pseudonymizer()
    pseudonymizer.load_key(pseudonym_key_dict)
    return pseudonymizer.pseudonymize(unique_names)


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


def _filter_null_rows(df: pl.DataFrame, output_folder: str, input_fofi: str, output_extension: str) -> pl.DataFrame:
    """Filter out rows with null values and save them separately."""
    filter_condition = reduce(
        lambda acc, col_name: acc | pl.col(col_name).is_null(),
        df.columns,
        pl.lit(False),  # noqa: FBT003 (Linter false positive)
    )
    logger.info('Filtering rows with NULL in any of these columns: %s', ', '.join(df.columns))

    # Collecting rows with problems
    df_with_nulls = df.filter(filter_condition)

    # If null rows found, collect rows and build file
    if not df_with_nulls.is_empty():
        try:
            logger.warning('Number of rows with problems: %d\n', df_with_nulls.height)
            logger.debug('%s\n', df_with_nulls)

            logger.info('Attempting to write dataframe of rows with nulls to file.')
            output_file = create_output_file_path(output_folder, input_fofi, '_with_nulls')
            save_data_file(df_with_nulls, output_file, output_extension)

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


def _batch_process_reports(df: pl.DataFrame, input_cols_dict: dict, handler: DeidentifyHandler) -> pl.DataFrame:
    """Process reports in batches for better performance."""
    batch_size: int = 1000
    logger.debug('Processing %d rows in batches of %d', df.height, batch_size)

    processed_batches = []
    deidentify = handler.deidentify_text(input_cols_dict)

    for start_index in range(0, df.height, batch_size):
        end_index = min(start_index + batch_size, df.height)
        batch_df = df.slice(start_index, end_index - start_index)
        logger.debug('Processing batch %d-%d', start_index, end_index)

        # Convert batch to list of row dicts for fast iteration
        row_dicts = batch_df.to_dicts()
        processed_texts = [deidentify(row_dict) for row_dict in row_dicts]

        # Add processed column to batch
        batch_with_processed = batch_df.with_columns(pl.Series('processed_report', processed_texts))
        processed_batches.append(batch_with_processed)

    return pl.concat(processed_batches)


def _performance_metrics(start_time: float, df_rowcount: int) -> None:
    """Log performance metrics for the processing operation."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0

    logger.info('Time passed with a total row count of %d rows', df_rowcount)
    logger.info('Total time: %.2f seconds (%.6f seconds per row)', total_time, time_per_row)


def process_data(input_fofi: str, input_cols: str, output_cols: str, pseudonym_key: str, output_extension: str) -> str:
    """Process and pseudonymize patient data from input files and return in Json."""
    params = dict(locals().items())
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())

    input_folder, output_folder = get_environment_paths()

    logger.debug('\nParsed arguments:\n%s\n', params_str)

    # Start progress tracking
    progress = progress_tracker(tracker)
    progress['update'](progress['get_stage_name'](0))

    # Update progress - Loading data
    progress['update'](progress['get_stage_name'](1))

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    input_file_path = f'{input_folder}/{input_fofi}' if not input_fofi.startswith('/') else input_fofi
    df = _load_data_file(input_file_path)

    # Update progress - pre-processing
    progress['update'](progress['get_stage_name'](2))

    # Convert string mappings to dictionaries
    input_cols_dict, output_cols_dict = _parse_column_mappings(input_cols, output_cols)

    # Check if the `patientName` column is available
    patient_name_col = input_cols_dict.get('patientName', None)
    has_patient_name = patient_name_col in df.columns

    # Step 3: Create pseudonym key if needed
    progress['update'](progress['get_stage_name'](3))

    start_time = time.time()

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    # Load existing pseudonym key if provided
    pseudonym_key_dict = None
    logger.info('Searching for pseudonym key: %s', pseudonym_key)

    if pseudonym_key is not None:
        key_file_path = f'{input_folder}/{pseudonym_key}'
        pseudonym_key_dict = load_pseudonym_key(key_file_path)
        logger.info('Loaded existing pseudonym key: %s', pseudonym_key)

    if has_patient_name:
        # Strip whitespace from patient names
        df = df.with_columns(pl.col(patient_name_col).str.strip_chars())

        pseudonym_key = _create_key(df, input_cols_dict, pseudonym_key_dict)
        save_pseudonym_key(pseudonym_key, output_folder)

    # Update progress - data transformation
    progress['update'](progress['get_stage_name'](4))

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

    #  Create a new column `processed_report` with deduced names
    df = _batch_process_reports(df, input_cols_dict, handler)

    if has_patient_name:
        patient_name_col = input_cols_dict['patientName']

        # Create a new column `patientID` with pseudonym keys
        df = df.with_columns(
            pl.col(patient_name_col)
            # Obtain randomized string corresponding to name
            .replace_strict(pseudonym_key, default=None)
            .alias('patientID'),
        )

        # Replace [PATIENT] tags in `processed_report` with pseudonym keys
        df = df.with_columns(
            pl.col('processed_report')
            .str.replace_all('[PATIENT]', '[' + pl.col('patientID') + ']')
            .alias('processed_report'),
        )

    # Prepare output data
    df = _prepare_output_data(df, input_cols_dict, output_cols_dict)

    # Debug: Show all collected annotations if in debug mode
    if logger.level == logging.DEBUG:
        handler.debug_deidentify_text()

    # Update progress - filtering nulls
    progress['update'](progress['get_stage_name'](5))

    # ---------------------------- STEP 4: FILTERING NULLS ---------------------------- #

    df = _filter_null_rows(df, output_folder, input_fofi, output_extension)

    # Only show example to terminal when NOT running as a frozen executable
    if not getattr(sys, 'frozen', False):
        sys.stdout.write(f'\n{df}\n')

    # Update progress - writing output
    progress['update'](progress['get_stage_name'](6))

    # ----------------------------- STEP 5: WRITE OUTPUT ------------------------------ #

    # Extract first 10 rows as JSON for return value
    processed_preview = df.head(10).to_dicts()

    output_file = create_output_file_path(output_folder, input_fofi)
    save_data_file(df, output_file, output_extension)

    if output_extension == '.csv':
        logger.info('Selected output extension is .csv\n')
    elif output_extension != '.parquet':
        logger.warning('Selected output extension not supported, using parquet.\n')

    # Log performance and finalize
    _performance_metrics(start_time, df.height)

    # Update progress - finalizing
    progress['update'](progress['get_stage_name'](7))

    result = {'data': processed_preview}
    return json.dumps(result)
