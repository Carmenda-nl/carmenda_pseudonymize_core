# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Datakey utilities for generating random string mappings.

This module provides functionality to create pseudonyms (random strings) for
unique names while maintaining consistent mappings.
"""

from __future__ import annotations

import secrets
import string
from pathlib import Path

import polars as pl

from .utils.file_handling import load_datakey
from .utils.logger import setup_logging

logger = setup_logging()


def _create_new_entry(names: pl.Series) -> pl.DataFrame:
    """Create an entry for missing names with empty synonym and code columns."""
    return pl.DataFrame({'clientname': names, 'synonyms': [''] * len(names), 'code': [''] * len(names)})


def _add_clientcodes(df: pl.DataFrame) -> pl.DataFrame:
    """Generate missing pseudonym codes for unique names."""
    missing_codes = df.filter(pl.col('code') == '').height
    if missing_codes == 0:
        return df

    existing_codes = df.filter(pl.col('code') != '').get_column('code')

    # Create a large random-code pool to select from.
    pool_size = missing_codes * 15
    code_chars = string.ascii_uppercase + string.digits
    random_pool = pl.Series([''.join(secrets.choice(code_chars) for code in range(14)) for code in range(pool_size)])

    # Filter out existing codes and take unique ones
    available_codes = (
        pl.DataFrame({'temp_code': random_pool})
        .unique()
        .filter(~pl.col('temp_code').is_in(existing_codes.implode()))
        .head(missing_codes)
        .with_row_index('temp_index')
    )

    empty_rows = df.filter(pl.col('code') == '')
    filled_rows = df.filter(pl.col('code') != '')

    apply_codes = (
        empty_rows.with_row_index('temp_index')
        .join(available_codes, on='temp_index', how='left')
        .with_columns(pl.col('temp_code').alias('code'))
        .drop(['temp_index', 'temp_code'])
    )

    return pl.concat([filled_rows, apply_codes])


def _check_existing_key(datakey_df: pl.DataFrame, missing_names_df: pl.Series | None = None) -> pl.DataFrame:
    """Check existing datakey, add missing names and merge duplicates."""
    if missing_names_df is None or missing_names_df.is_empty():
        logger.debug('No missing names found to add to datakey')
    else:
        missing_df = _create_new_entry(missing_names_df)
        datakey_df = pl.concat([datakey_df, missing_df])

    # Merge duplicate client names and combine their synonyms
    return (
        datakey_df.group_by('clientname')
        .agg(
            [
                pl.col('synonyms').filter(pl.col('synonyms') != '').str.join(', ').alias('synonyms'),
                pl.col('code').filter(pl.col('code') != '').first().alias('code'),
            ],
        )
        .with_columns(
            [
                pl.col('synonyms').fill_null('').alias('synonyms'),
                pl.col('code').fill_null('').alias('code'),
            ],
        )
    )


def process_datakey(df: pl.DataFrame, input_cols: dict, datakey_file: str | None, input_folder: str) -> pl.DataFrame:
    """Create a new datakey or update an existing one."""
    unique_names_df = df[input_cols['clientname']].drop_nulls().unique()

    if datakey_file:
        datakey_path = Path(input_folder) / datakey_file

        if Path(datakey_path).is_file():
            datakey_df = load_datakey(str(datakey_path))

            if datakey_df is not None:  # Valid encoded datakey
                logger.info('Loaded existing datakey: %s', datakey_file)
                logger.debug('%s\n', datakey_df)

                # Collect unique missing names from patient column
                key_unique_names = datakey_df.get_column('clientname').drop_nulls().unique()
                missing_names_df = unique_names_df.filter(~unique_names_df.is_in(key_unique_names.implode()))

                datakey_df = _check_existing_key(datakey_df, missing_names_df)
                return _add_clientcodes(datakey_df).sort('clientname')

    logger.info('Datakey not provided. Creating a fresh new datakey')
    datakey_df = _create_new_entry(unique_names_df)

    return _add_clientcodes(datakey_df).sort('clientname')
