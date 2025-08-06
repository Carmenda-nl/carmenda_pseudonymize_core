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

from services.logger import setup_logging
from services.progress_tracker import progress_tracker, tracker
from utils.file_handling import (
    get_environment_paths,
    get_file_extension,
    get_file_size,
    load_data_file,
    load_pseudonym_key,
    save_pseudonym_key,
    create_output_file_path,
    save_data_file,
)

from core.pseudonymizer import pseudonymize
from core.deduce_handler import DeduceHandler


import sys
import json
import time
import polars as pl

import multiprocessing
from functools import reduce


def process_data(input_fofi: str, input_cols: str, output_cols: str, pseudonym_key: str, output_extension: str) -> str:
    """Process and pseudonymize patient data from input files.

    Parameters
    ----------
    input_fofi : str
        Input file or folder identifier.
    input_cols : str
        Comma-separated string of input column mappings.
    output_cols : str
        Comma-separated string of output column mappings.
    pseudonym_key : str
        Path to pseudonym key file or None.
    output_extension : str
        Desired output file extension (.csv or .parquet).

    Returns
    -------
    str
        JSON string containing processed data preview.

    """
    params = dict(locals().items())
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())

    input_folder, output_folder = get_environment_paths()
    logger = setup_logging(output_folder)

    logger.debug('\nParsed arguments:\n%s\n', params_str)

    # start progress tracking
    progress = progress_tracker(tracker)
    progress['update'](progress['get_stage_name'](0))

    # update progress - loading data
    progress['update'](progress['get_stage_name'](1))

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    # get file path and extension
    input_file_path = f'{input_folder}/{input_fofi}' if not input_fofi.startswith('/') else input_fofi
    input_extension = get_file_extension(input_file_path)

    if input_extension == '.csv':
        file_size = get_file_size(input_file_path)
        logger.info('CSV input file of size: %s', file_size)

    # load data using file utilities
    df = load_data_file(input_file_path)

    # update progress - pre-processing
    progress['update'](progress['get_stage_name'](2))

    # convert string mappings to dictionaries
    input_cols = dict(item.strip().split('=') for item in input_cols.split(','))
    output_cols = dict(item.strip().split('=') for item in output_cols.split(','))

    # check if the `patientName` column is available
    patient_name_col = input_cols.get('patientName', None)
    has_patient_name = patient_name_col in df.columns

    # log a columns schema
    schema_str = 'root\n' + '\n'.join([f' |-- {name}: {dtype}' for name, dtype in df.schema.items()])
    logger.debug('%s \n', schema_str)

    # count rows
    df_rowcount = df.height
    logger.info('Row count: %d', df_rowcount)

    # update progress - pseudonymization
    progress['update'](progress['get_stage_name'](3))

    start_time = time.time()

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    # load existing pseudonym key if provided
    pseudonym_key_dict = None
    logger.info('Searching for pseudonym key: %s', pseudonym_key)

    if pseudonym_key is not None:
        key_file_path = f'{input_folder}/{pseudonym_key}'
        pseudonym_key_dict = load_pseudonym_key(key_file_path)

    # create an pseudonym_key when `patientName` exists
    if has_patient_name:
        unique_names = (
            df.select(input_cols['patientName'])
            .filter(pl.col(input_cols['patientName']).is_not_null())
            .unique()
            .to_series()
        )
        pseudonym_key = pseudonymize(unique_names, pseudonym_key, logger=logger)
        save_pseudonym_key(pseudonym_key, output_folder)

        # update progress - data transformation
        progress['update'](progress['get_stage_name'](4))

        # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

        handler = DeduceHandler()

        # create a new column `patientID` with pseudonym keys
        df = df.with_columns(
            pl.col(patient_name_col)
            # obtain randomized string corresponding to name
            .map_elements(lambda name: pseudonym_key.get(name, None), return_dtype=pl.Utf8)
            .alias('patientID'),
        )

        # create a new column `processed_report` with deduced names
        df = df.with_columns(
            pl.struct(df.columns)
            .map_elements(handler.deduce_with_id(input_cols), return_dtype=pl.Utf8)
            .alias('processed_report'),
        )

        # replace [PATIENT] tags in `processed_report` with pseudonym keys
        df = df.with_columns(
            pl.struct(['processed_report', 'patientID'])
            .map_elements(
                lambda row: handler.replace_tags(row['processed_report'], row['patientID']), return_dtype=pl.Utf8,
            )
            .alias('processed_report'),
        )

        # log the first 10 rows of the dataframe
        print(df.head(10))

    else:
        logger.info('No patientName column, extracting names from reports')

        # update progress - data transformation
        progress['update'](progress['get_stage_name'](4))

        # create a new column `processed_report` with deduced names
        df = df.with_columns(
            pl.struct(df.columns)
            .map_elements(handler.deduce_report_only(input_cols), return_dtype=pl.Utf8)
            .alias('processed_report'),
        )

    select_cols = [col for col in output_cols.values() if col in df.columns]
    df = df.select(select_cols)
    logger.info('Output columns: %s\n', df.columns)

    # rename headers to their original input name
    input_cols_keys = dict(input_cols)
    rename_headers = {}

    if 'patientID' in df.columns and 'patientName' in output_cols:
        rename_headers['patientID'] = input_cols_keys['patientName']
    if 'processed_report' in df.columns and 'report' in input_cols_keys:
        rename_headers['processed_report'] = input_cols_keys['report']

    if rename_headers:
        df = df.rename(rename_headers)

    # update progress - filtering nulls
    progress['update'](progress['get_stage_name'](5))

    # ---------------------------- STEP 4: FILTERING NULLS ---------------------------- #

    # handling of irregularities
    filter_condition = reduce(
        lambda acc, col_name: acc | pl.col(col_name).is_null(),
        df.columns,
        pl.lit(False),  # noqa: FBT003
    )
    logger.info('Filtering rows with NULL in any of these columns: %s', ', '.join(df.columns))

    # collecting rows with problems
    df_with_nulls = df.filter(filter_condition)

    # if null rows found, collect rows and build file
    if not df_with_nulls.is_empty():
        try:
            logger.warning('Number of rows with problems: %d\n', df_with_nulls.height)
            print(f'{df_with_nulls}\n')

            logger.info('Attempting to write dataframe of rows with nulls to file.')
            output_file = create_output_file_path(output_folder, input_fofi, '_with_nulls')
            save_data_file(df_with_nulls, output_file, output_extension)

        except (OSError, PermissionError, ValueError):
            logger.exception('Problematic rows detected. Continuing with valid rows.')

        # cleanup the problematic rows and keep the good ones
        if 'df_with_nulls' in locals():
            del df_with_nulls

        df = df.filter(~filter_condition)
        logger.info('Remaining rows after filtering rows with empty values: %d', df.height)
    else:
        logger.info('No problematic rows found!')

    # only print to terminal when not running as a frozen executable
    if not getattr(sys, 'frozen', False):
        # print example table to terminal.
        print(f'\n{df}\n')

    # update progress - writing output
    progress['update'](progress['get_stage_name'](6))

    # ----------------------------- STEP 5: WRITE OUTPUT ------------------------------ #

    # extract first 10 rows as JSON for return value
    processed_preview = df.head(10).to_dicts()

    output_file = create_output_file_path(output_folder, input_fofi)
    save_data_file(df, output_file, output_extension)

    if output_extension == '.csv':
        logger.info('Selected output extension is .csv\n')
    elif output_extension != '.parquet':
        logger.warning('Selected output extension not supported, using parquet.\n')

    # stop the timer and log the results
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0

    logger.info(
        'Time passed with %d CPU cores and a total row count of %d rows',
        multiprocessing.cpu_count(),
        df_rowcount,
    )
    logger.info('Total time: %.2f seconds (%.6f seconds per row)', total_time, time_per_row)

    # update progress - finalizing
    progress['update'](progress['get_stage_name'](7))

    result = {'data': processed_preview}
    return json.dumps(result)
