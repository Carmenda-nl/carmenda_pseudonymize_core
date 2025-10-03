# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
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

from utils.file_handling import load_datakey
from utils.logger import setup_logging

logger = setup_logging()


def _create_empty_datakey(names: pl.Series) -> pl.DataFrame:
    """Create a DataFrame with empty Synonym and Code columns for given names."""
    return pl.DataFrame({'Clientnaam': names, 'Synoniemen': [''] * len(names), 'Code': [''] * len(names)})


def _add_clientcodes(df: pl.DataFrame) -> pl.DataFrame:
    """Generate missing pseudonym codes for unique names."""
    missing_codes = df.filter(pl.col('Code') == '').height
    if missing_codes == 0:
        return df

    existing_codes = df.filter(pl.col('Code') != '').get_column('Code')

    # Create a large random-code pool to select from.
    pool_size = missing_codes * 15
    code_chars = string.ascii_uppercase + string.digits
    random_pool = pl.Series([''.join(secrets.choice(code_chars) for code in range(14)) for code in range(pool_size)])

    # Filter out existing codes and take unique ones.
    available_codes = (
        pl.DataFrame({'temp_code': random_pool}).unique()
        .filter(~pl.col('temp_code').is_in(existing_codes.implode()))
        .head(missing_codes).with_row_index('temp_index')
    )

    empty_rows = df.filter(pl.col('Code') == '')
    filled_rows = df.filter(pl.col('Code') != '')

    apply_codes = (
        empty_rows.with_row_index('temp_index')
        .join(available_codes, on='temp_index', how='left')
        .with_columns(pl.col('temp_code').alias('Code'))
        .drop(['temp_index', 'temp_code'])
    )

    return pl.concat([filled_rows, apply_codes])


def _get_existing_key(datakey_df: pl.DataFrame, df_missing_names: pl.Series | None = None) -> pl.DataFrame:
    """Load existing datakey, add missing names and merge duplicates."""
    datakey_df = datakey_df.with_columns(
        [
            # Ensure all clients have a synonym field and code field.
            pl.col('Synoniemen').fill_null('').alias('Synoniemen'),
            pl.col('Code').fill_null('').alias('Code'),
        ],
    )

    if df_missing_names is not None:
        missing_df = _create_empty_datakey(df_missing_names)
        datakey_df = pl.concat([datakey_df, missing_df])

    # Merge duplicate client names and combine their synonyms.
    return (
        datakey_df.group_by('Clientnaam')
        .agg(
            [
                pl.col('Synoniemen').filter(pl.col('Synoniemen') != '').str.join(', ').alias('Synoniemen'),
                pl.col('Code').filter(pl.col('Code') != '').first().alias('Code'),
            ],
        )
        .with_columns(
            [
                pl.col('Synoniemen').fill_null('').alias('Synoniemen'),
                pl.col('Code').fill_null('').alias('Code'),
            ],
        )
    )


def process_datakey(df: pl.DataFrame,input_cols: dict, datakey_file: str | None, input_folder: str) -> pl.DataFrame:
    """Create a new datakey or update an existing one."""
    df_unique_names = df[input_cols['clientname']].drop_nulls().unique()

    if datakey_file:
        datakey_path = Path(input_folder) / datakey_file

        if Path(datakey_path).is_file():
            datakey_df = load_datakey(datakey_path)

            if datakey_df is not None:  # Valid encoded datakey
                logger.info('Loaded existing datakey: %s', datakey_file)
                logger.debug('%s\n', datakey_df)

                # Collect unique missing names from patient column
                key_unique_names = datakey_df.get_column('Clientnaam').drop_nulls().unique()
                df_missing_names = df_unique_names.filter(~df_unique_names.is_in(key_unique_names.implode()))

                datakey_df = _get_existing_key(datakey_df, df_missing_names)
                return _add_clientcodes(datakey_df).sort('Clientnaam')

    logger.info('Creating a fresh new datakey')
    datakey_df = _create_empty_datakey(df_unique_names)

    return _add_clientcodes(datakey_df).sort('Clientnaam')
