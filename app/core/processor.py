# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Data processing pipeline for pseudonymizing data.

This pipeline provides functionality for:
    - Loading data from files
    - Creating datakeys for clientnames
    - Transforming and pseudonymizing report data
    - Writing processed data to output files
"""

import json
import logging
import sys
import time
from pathlib import Path

import polars as pl

from core.datakey import process_datakey
from core.deidentify import DeidentifyHandler
from core.utils.file_handling import get_environment, load_data_file, save_datafile, save_datakey
from core.utils.logger import setup_logging
from core.utils.progress_tracker import performance_metrics

logger = setup_logging()


def process_data(input_file: str, input_cols: str, output_cols: str, datakey: str) -> str:
    """Process and pseudonymize data from input file and return the first 10 rows in Json."""
    params = dict(locals().items())
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())
    logger.debug('Parsed arguments:\n%s\n', params_str)

    input_folder, output_folder = get_environment()

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    input_file_path = f'{input_folder}/{input_file}' if not input_file.startswith('/') else input_file
    df = load_data_file(input_file_path, output_folder)

    if df is not None:
        input_cols_dict = dict(column.strip().split('=') for column in input_cols.split(','))
        output_cols_list = [column.strip() for column in output_cols.split(',')]

        clientname_col = input_cols_dict.get('clientname')
        has_clientname = clientname_col in df.columns
        report_col = input_cols_dict.get('report')
        has_report = report_col in df.columns

        start_time = time.time()
    else:
        message = f'Input file "{input_file_path}" could not be loaded.'
        logger.error(message)
        return json.dumps({'error': message})

    if not has_report:
        message = f'Report column "{report_col}" not found in input data.'
        logger.error(message)
        return json.dumps({'error': message})

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    if has_clientname:
        # Strip whitespace from clientnames
        df = df.with_columns(pl.col(clientname_col).str.strip_chars())

        processed_datakey = process_datakey(df, input_cols_dict, datakey, input_folder)
        datakey_filename = Path(datakey).name if datakey else 'datakey.csv'
        save_datakey(processed_datakey, input_file, output_folder, datakey_filename)
    else:
        logger.info('Clientname not provided, skipping datakey creation.\n')

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

    handler = DeidentifyHandler()

    if has_clientname:
        df = handler.replace_synonym(df, processed_datakey, input_cols_dict)
        df = handler.deidentify_text(df, processed_datakey, input_cols_dict)
        df = handler.add_clientcodes(df, processed_datakey, input_cols_dict)
    else:
        df = handler.deidentify_text(df, None, input_cols_dict)

    # Prepare output data
    df = df.select(pl.selectors.by_name(*output_cols_list, require_all=False))

    rename_headers = {}

    if 'clientcode' in df.columns and 'clientname' in input_cols_dict:
        rename_headers['clientcode'] = input_cols_dict['clientname']

    if 'processed_report' in df.columns and 'report' in input_cols_dict:
        rename_headers['processed_report'] = input_cols_dict['report']

    if rename_headers:
        df = df.rename(rename_headers)

    # Show pseudonymized reports in debug mode and when NOT running as a frozen executable
    if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
        handler.deidentify_text_debug()

    # ----------------------------- STEP 4: WRITE OUTPUT ------------------------------ #

    performance_metrics(start_time, df.height)
    save_datafile(df, input_file, output_folder)

    return json.dumps({'data': df.head(2).to_dicts()})
