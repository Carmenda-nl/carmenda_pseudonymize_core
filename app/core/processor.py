# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Data processing pipeline for pseudonymizing data.

This pipeline provides functionality for:
    - Loading data from files
    - Creating datakeys for clientnames
    - Transforming and pseudonymizing report data
    - Writing processed data to output files
"""

import logging
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl

from core.datakey import process_datakey
from core.deduce import DeidentifyHandler
from core.utils.file_handling import load_datafile, save_datafile, save_datakey
from core.utils.logger import setup_logging
from core.utils.progress_tracker import ProgressTracker, performance_metrics

logger = setup_logging()

MAX_FIRST_PREVIEW_ROWS = 3
MAX_LAST_PREVIEW_ROWS = 3
MINIMUM_ROWS = 6


def process_data(file: str, datakey: str, input_cols: str, tracker: ProgressTracker, output_dir: str) -> dict[str, Any]:
    """Process and pseudonymize data from input file and return the first 10 rows in Json."""
    start_time = time.time()
    tracker.set_progress('start')

    job_logger = logging.LoggerAdapter(logger, {'job_id': id(tracker)})
    job_logger.debug(
        'Parsed arguments:\n |-- input_file=%s\n |-- input_cols=%s\n |-- datakey=%s\n', file, input_cols, datakey
    )

    json_output: dict[str, Any] = {'datakey_path': None, 'log_path': None}

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    df = load_datafile(file, tracker=tracker)

    if df is not None:
        input_cols_dict = dict(column.strip().split('=') for column in input_cols.split(','))
        report_cols = [value.strip() for key, value in input_cols_dict.items() if key.startswith('report')]

        report_order = {
            report_key: index
            for index, report_key in enumerate((key for key in input_cols_dict if key.startswith('report')), start=1)
        }
        output_cols = [
            'clientcode'
            if key == 'clientname'
            else f'processed_report_{report_order[key]}'
            if key in report_order
            else value.strip()
            for key, value in input_cols_dict.items()
        ]

        clientname_col = input_cols_dict.get('clientname')
        has_clientname = clientname_col in df.columns
        missing_reports = [col for col in report_cols if col and col not in df.columns]
    else:
        message = f'Input file "{file}" could not be loaded.'
        logger.error(message)
        return {'error': message}

    if not report_cols or missing_reports:
        message = f'Report column not found in input data: {", ".join(missing_reports)}.'
        logger.error(message)
        return {'error': message}

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    if has_clientname and clientname_col is not None:
        # Strip whitespace from clientnames
        df = df.with_columns(pl.col(clientname_col).str.strip_chars())

        processed_datakey = process_datakey(df, input_cols_dict, datakey)
        datakey_filename = f'{Path(file).stem}_key.csv'
        json_output['datakey'] = save_datakey(processed_datakey, file, output_dir, datakey_filename)
    else:
        logger.info('Clientname not provided, skipping datakey creation.')

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

    handler = DeidentifyHandler(tracker=tracker)

    if has_clientname:
        df = handler.replace_synonym(df, processed_datakey, report_cols)
        df = handler.deidentify_text(df, input_cols_dict)
        df = handler.add_clientcodes(df, processed_datakey, input_cols_dict)
    else:
        df = handler.deidentify_text(df, input_cols_dict)

    # Prepare output data
    df = df.select(pl.selectors.by_name(*output_cols, require_all=False))

    rename_headers = {}

    if 'clientcode' in df.columns and 'clientname' in input_cols_dict:
        rename_headers['clientcode'] = input_cols_dict['clientname']

    rename_headers.update(
        {
            f'processed_report_{index}': input_cols_dict[report_key]
            for index, report_key in enumerate((key for key in input_cols_dict if key.startswith('report')), start=1)
            if f'processed_report_{index}' in df.columns
        },
    )

    df = df.rename(rename_headers)

    # Show pseudonymized reports in debug mode and when NOT running as a frozen executable
    if logger.level == logging.DEBUG and not getattr(sys, 'frozen', False):
        handler.deidentify_text_debug()

    # ----------------------------- STEP 4: WRITE OUTPUT ------------------------------ #

    json_output['output_file'] = save_datafile(df, file, output_dir)
    json_output['metrics'] = performance_metrics(start_time, df.height)
    tracker.set_progress('done')

    json_output['log'] = next(
        (Path(handler.baseFilename) for handler in logger.handlers if isinstance(handler, logging.FileHandler)),
        None,
    )

    json_output['preview'] = (
        df.head(MAX_FIRST_PREVIEW_ROWS).to_dicts()
        if df.height < MINIMUM_ROWS
        else df.head(MAX_FIRST_PREVIEW_ROWS).to_dicts() + df.tail(MAX_LAST_PREVIEW_ROWS).to_dicts()
    )

    return json_output
