# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Data processing module for pseudonymizing data.

This module provides functionality for:
    - Loading data from files
    - Creating data keys for patient names
    - Transforming and pseudonymizing patient data (in parallel batches)
    - Filtering null values and handling data irregularities
    - Writing processed data to output files
"""

from __future__ import annotations

import json
import logging
import sys
import time
from functools import reduce
from pathlib import Path

import polars as pl

from core.data_key import Pseudonymizer
from core.deidentify_handler import DeidentifyHandler
from utils.file_handling import (
    create_output_file_path,
    get_environment_paths,
    get_file_extension,
    get_file_size,
    load_data_file,
    load_data_key,
    save_data_file,
    save_data_key,
)
from utils.logger import setup_logging
from utils.progress_tracker import progress_tracker, tracker

DataKey = list[dict[str, str]]  # Type alias
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


def _processs_data_key(df: pl.DataFrame, input_cols: dict, data_key: str, input_folder: str) -> list[dict[str, str]]:
    """Create a new data key or update an existing one."""
    pseudonymizer = Pseudonymizer()

    df_unique_names = (
        df.select(input_cols['patientName'])
        .filter(pl.col(input_cols['patientName']).is_not_null())
        .unique()
        .to_series()
        .to_list()
    )

    if data_key:
        data_key_path = Path(input_folder) / data_key
        data_key_list = load_data_key(data_key_path)
        logger.info('Loaded existing data key: %s', data_key)

        # Check if existing data_key contains all patient names
        key_unique_names = pl.DataFrame(data_key_list).select('patient').unique().to_series().to_list()
        missing_names = [name for name in df_unique_names if name not in key_unique_names]
        data_key_list = pseudonymizer.get_existing_key(data_key_list, missing_names)
    else:
        data_key_list = [{'patient': name, 'synonym': '', 'pseudonym': ''} for name in df_unique_names]
        logger.info('No existing data key provided, creating a new one')

    return pseudonymizer.pseudonymize(data_key_list)


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


def _data_transformer(df: pl.DataFrame, input_cols: dict, data_key: DataKey) -> pl.DataFrame:
    """Process to de-identify text in the report column."""
    if data_key:
        handler.set_synonym_mapping(data_key)

    deidentify = handler.deidentify_text(input_cols, data_key)

    # Extract the report column as a list for processing
    report_col = input_cols['report']

    if report_col not in df.columns:
        result_df = df.with_columns(pl.lit('').alias('processed_report'))
    else:
        reports = df.select(report_col).to_series().to_list()
        other_cols = [col for col in df.columns if col != report_col]

        if other_cols:
            other_data = df.select(other_cols).to_dicts()
            batch_data = [{**row, report_col: report} for row, report in zip(other_data, reports)]
        else:
            batch_data = [{report_col: report} for report in reports]

        processed_texts = [deidentify(row_dict) for row_dict in batch_data]
        result_df = df.with_columns(pl.Series('processed_report', processed_texts))

    return result_df


def _performance_metrics(start_time: float, df_rowcount: int) -> None:
    """Log performance metrics for the processing operation."""
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0

    logger.info('Time passed with a total of %d rows', df_rowcount)
    logger.info('Total time: %.2f seconds (%.6f seconds per row)', total_time, time_per_row)


def process_data(input_fofi: str, input_cols: str, output_cols: str, data_key: str, output_extension: str) -> str:
    """Process and pseudonymize patient data from input files and return in Json."""
    params = dict(locals().items())
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())
    logger.debug('\nParsed arguments:\n%s\n', params_str)

    input_folder, output_folder = get_environment_paths()

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
    input_cols_dict = dict(item.strip().split('=') for item in input_cols.split(','))
    output_cols_dict = dict(item.strip().split('=') for item in output_cols.split(','))

    # Check if the `patientName` column is available
    patient_name_col = input_cols_dict.get('patientName')
    has_patient_name = patient_name_col in df.columns

    start_time = time.time()

    # Step 3: Create data key if needed
    progress['update'](progress['get_stage_name'](3))

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    if has_patient_name:
        # Strip whitespace from patient names
        df = df.with_columns(pl.col(patient_name_col).str.strip_chars())

        data_key = _processs_data_key(df, input_cols_dict, data_key, input_folder)
        save_data_key(data_key, output_folder)

    # Update progress - data transformation
    progress['update'](progress['get_stage_name'](4))

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

    df = _data_transformer(df, input_cols_dict, data_key)

    if has_patient_name:
        patient_name_col = input_cols_dict['patientName']

        # Create a new column `patientID` with data keys
        name_to_pseudonym = {entry['patient']: entry['pseudonym'] for entry in data_key}
        df = df.with_columns(
            pl.col(patient_name_col)
            # Obtain randomized string corresponding to name
            .replace_strict(name_to_pseudonym, default=None)
            .alias('patientID'),
        )

        # Replace [PATIENT] tags in `processed_report` with data keys
        df = df.with_columns(
            pl.col('processed_report').str.replace_all(r'\[PATIENT\]', pl.format('[{}]', pl.col('patientID'))),
        )

    # Prepare output data
    df = _prepare_output_data(df, input_cols_dict, output_cols_dict)

    # Show pseudonymized reports in debug mode
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
