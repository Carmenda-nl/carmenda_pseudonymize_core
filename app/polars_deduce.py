# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""
Script Name: polars_deduce.py
Authors: Django Heimgartner, Joep Tummers, Pim van Oirschot
Date: 2025-04-24
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
    - Setup pyspark
    - Define functions
    - Load data and when using dummy data, amplify to simulate real-world scenario
    - Apply transformations
        - Identify unique names based on patientName column and map them to randomized patientIDs.
        - Apply Deduce algorithm to report text column, making use of patientName to increase likelihood of at least
          de-identifying the main subject.
        - Replace the generated [PATIENT] tags with the new patientID
    - Write output to disk
"""

import os
import sys
import shutil
import random
import string
import json
import time
import argparse

import polars as pl
import multiprocessing
from functools import reduce

import deduce
from deduce.person import Person

# initialize deduce
if getattr(sys, 'frozen', False):
    lookup_data = os.path.join('data', 'deduce', 'data', 'lookup')

    if not os.path.exists(lookup_data):
        lookup_cached = os.path.join(sys._MEIPASS, 'deduce', 'data', 'lookup')
        shutil.copytree(lookup_cached, lookup_data, dirs_exist_ok=True)

    deduce_instance = deduce.Deduce(lookup_data_path=lookup_data, cache_path=lookup_data)
else:
    deduce_instance = deduce.Deduce()


def deduce_with_id(input_cols):
    """
    This is a factory function that returns an inner function configured to process
    rows of data using the specified column mappings. The inner function extracts
    patient name information and uses it to enhance the de-identification process.

    Parameters:
        input_cols (dict): Dictionary mapping required column names:
            - 'patientName': Column containing patient's full name
            - 'report': Column containing text to de-identify

    Returns:
        report_deid (string): The de-identified text, or None if processing fails
    """
    def inner_func(row):
        try:
            patient_name = str.split(row[input_cols['patientName']], ' ', maxsplit=1)
            patient_initials = ''.join([name[0] for name in patient_name])
            """
            Difficult to predict person metadata exactly. I.e. in case of multiple spaces
            does this indicate multitude of first names or a composed last name ?

            Best will be if input data contains columns for first names, surname, initials
                patient = Person(first_names=[patient_name[0:-1]]
                surname=patient_name[-1], initials=patient_initials)
            """
            deduce_patient = Person(first_names=[patient_name[0]], surname=patient_name[1], initials=patient_initials)

            report_deid = deduce_instance.deidentify(row[input_cols['report']], metadata={'patient': deduce_patient})
            report_deid = getattr(report_deid, 'deidentified_text')

            return report_deid

        except Exception:
            # no error log as null rows will be collected later
            return None

    return inner_func


def deduce_report_only(input_cols):
    """
    Purpose:
        Apply the Deduce algorithm

    Parameters:
        report (string): Text entry to apply on

    Returns:
        report_deid (string): Deidentified version of report input
    """
    def inner_func(row):
        try:
            report_deid = deduce_instance.deidentify(row[input_cols['report']])
            report_deid = getattr(report_deid, 'deidentified_text')

            return report_deid

        except Exception:
            # no error log as null rows will be collected later
            return None

    return inner_func


def replace_tags(text, new_value, tag='[PATIENT]'):
    """
    Purpose:
        Deduce labels occurences of patient name in text with a [PATIENT] tag.
        This function replaces that with the randomized ID.

    Parameters:
        text (string): the text possibly containing tag to replace
        new_value (string): text to replace tag with
        tag (string, default = "[PATIENT]): Formatted string indicating tag to replace

    Returns:
        text from text but with tag replaced by new_value
    """
    try:
        return str.replace(text, '[PATIENT]', f'[{new_value}]')
    except Exception:
        # no error log as null rows will be collected later
        return None


def pseudonymize(unique_names, pseudonym_key=None, droplist=[], logger=None):
    """
    Generates pseudonyms (random strings) for a list of unique names.

    Parameters:
        unique_names (list): List of names to pseudonymize
        pseudonym_key (dict, optional): Existing name-pseudonym mapping. Default: None
        droplist (list, optional): Names to skip. Default: []
        logger (logging.Logger, optional): Logger for information messages. Default: None

    Returns:
        dict: Mapping of original names to pseudonyms. Also contains None:None mapping.

    Raises:
        AssertionError: If number of unique names doesn't match number of mappings.
    """
    unique_names = [name for name in unique_names if name not in droplist]

    if pseudonym_key is None:
        logger.info('Building new key because argument was None')
        pseudonym_key = {}
    else:
        logger.info('Building from existing key')

    for name in unique_names:
        if name not in pseudonym_key:
            found_new = False
            iterate = 0

            while (found_new is False and iterate < 15):
                iterate += 1
                pseudonym_candidate = ''.join(
                    random.choices(string.ascii_uppercase + string.ascii_uppercase+string.digits, k=14)
                )
                if pseudonym_candidate not in pseudonym_key.values():
                    found_new is True
                    pseudonym_key[name] = pseudonym_candidate

    error_message = 'Unique_names (input) and pseudonym_key (output) do not have the same length'
    assert len(unique_names) == len(pseudonym_key.items()), error_message

    return pseudonym_key


def progress_tracker(tracker):
    """
    Creates a progress tracking mechanism for multi-stage processing.

    Parameters:
        tracker: Global tracker object to update overall progress

    Returns:
        Dictionary containing progress tracking functions and data
    """
    progress_stages = [
        'Prepare data', 'Loading data', 'Pre-processing',
        'Pseudonymization', 'Data transformation', 'Filtering nulls',
        'Writing output', 'Finalizing'
    ]

    total_stages = len(progress_stages)

    progress_data = {
        'current_stage': 0,
        'progress_percentage': 0,
        'total_stages': total_stages,
        'stages': progress_stages
    }

    def update_progress(stage_name=None):
        if stage_name is None and progress_data['current_stage'] < total_stages:
            stage_name = progress_stages[progress_data['current_stage']]

        progress_data['current_stage'] += 1
        progress_data['progress_percentage'] = int((progress_data['current_stage'] / total_stages) * 100)

        # update the global progress tracker
        tracker.update_progress(progress_data['progress_percentage'], stage_name)

        return progress_data['progress_percentage']

    return {
        'update': update_progress,
        'data': progress_data,
        'get_stage_name': lambda idx: progress_stages[idx] if 0 <= idx < len(progress_stages) else None
    }


def process_data(input_fofi, input_cols, output_cols, pseudonym_key, output_extension):
    # log the parameters
    params = {key: value for key, value in locals().items()}
    params_str = '\n'.join(f' |-- {key}={value}' for key, value in params.items())
    print(f'Parsed arguments:\n{params_str}\n')

    # start progress tracking
    progress = progress_tracker(tracker)
    progress['update'](progress['get_stage_name'](0))

    # check if environment is on: django, pyinstaller or script
    if sys.argv[0] == 'manage.py':
        input_folder = 'data/input/'
        output_folder = 'data/output/'
        logger = setup_logging('data/output/')
    else:
        if getattr(sys, 'frozen', False):
            input_folder = os.path.join(sys._MEIPASS, 'data', 'input')
            output_folder = os.path.join(sys._MEIPASS, 'data', 'output')
            logger = setup_logging(os.path.join(sys._MEIPASS, 'data', 'output'))

        else:
            input_folder = '/data/input/'
            output_folder = '/data/output/'
            logger = setup_logging('/data/output/')

    input_extension = os.path.splitext(os.path.join(input_folder, input_fofi))[-1]

    # update progress - loading data
    progress['update'](progress['get_stage_name'](1))

    # ----------------------------- STEP 1: LOADING DATA ------------------------------ #

    if input_extension == '.csv':
        input_fofi_size = os.stat(os.path.join(input_folder, input_fofi)).st_size
        logger.info(f'CSV input file of size: {input_fofi_size}')
        df = pl.read_csv(os.path.join(input_folder, input_fofi))
    else:
        df = pl.read_parquet(os.path.join(input_folder, input_fofi))

    # update progress - pre-processing
    progress['update'](progress['get_stage_name'](2))

    # convert string mappings to dictionaries
    input_cols = dict(item.strip().split('=') for item in input_cols.split(','))
    output_cols = dict(item.strip().split('=') for item in output_cols.split(','))

    # check if the `patientName` column is available
    patient_name_col = input_cols.get('patientName', None)
    has_patient_name = patient_name_col in df.columns

    # if you want to amplify (dublicate) data, usually for performance testing purposes
    n_concat = 1
    if n_concat > 1 and has_patient_name:
        df = pl.concat([df] * n_concat)

    # print a columns schema
    print('root\n' + '\n'.join([f' |-- {name}: {dtype}' for name, dtype in df.schema.items()]) + '\n')

    # count rows
    df_rowcount = df.height
    logger.info(f'Row count: {df_rowcount}')

    # update progress - pseudonymization
    progress['update'](progress['get_stage_name'](3))

    start_time = time.time()

    # ------------------------------ STEP 2: CREATE KEY ------------------------------- #

    # turn names into a dictionary of randomized ID's
    logger.info(f'Searching for pseudonym key: {pseudonym_key}')
    if pseudonym_key is not None:
        pseudonym_key = json.load(open(os.path.join(input_folder, pseudonym_key)))

    # create an pseudonym_key when `patientName` exists
    if has_patient_name:
        unique_names = (
            df.select(input_cols['patientName'])
            .filter(pl.col(input_cols['patientName']).is_not_null()).unique().to_series()
        )
        pseudonym_key = pseudonymize(unique_names, pseudonym_key, logger=logger)

        # write to json file
        with open(os.path.join(output_folder, 'pseudonym_key.json'), 'w') as outfile:
            json.dump(pseudonym_key, outfile)

        outfile.close()

        # update progress - data transformation
        progress['update'](progress['get_stage_name'](4))

    # -------------------------- STEP 3: DATA TRANSFORMATION -------------------------- #

        # create a new column `patientID` with pseudonym keys
        df = df.with_columns(
            pl.col(patient_name_col)
            # obtain randomized string corresponding to name
            .map_elements(lambda name: pseudonym_key.get(name, None), return_dtype=pl.Utf8)
            .alias('patientID')
        )

        # create a new column `processed_report` with deduced names
        df = df.with_columns(
            pl.struct(df.columns)
            .map_elements(deduce_with_id(input_cols), return_dtype=pl.Utf8)
            .alias('processed_report')
        )

        # replace [PATIENT] tags in `processed_report` with pseudonym keys
        df = df.with_columns(
            pl.struct(['processed_report', 'patientID'])
            .map_elements(lambda row: replace_tags(row['processed_report'], row['patientID']), return_dtype=pl.Utf8)
            .alias('processed_report')
        )
    else:
        logger.info('No patientName column, extracting names from reports')

        # update progress - data transformation
        progress['update'](progress['get_stage_name'](4))

        # create a new column `processed_report` with deduced names
        df = df.with_columns(
            pl.struct(df.columns)
            .map_elements(deduce_report_only(input_cols), return_dtype=pl.Utf8)
            .alias('processed_report')
        )

    select_cols = [col for col in output_cols.values() if col in df.columns]
    df = df.select(select_cols)
    logger.info(f'Output columns: {df.columns}\n')

    # rename headers to their original input name
    input_cols_keys = {}

    for key, value in input_cols.items():
        input_cols_keys[key] = value

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
    filter_condition = reduce(lambda acc, col_name: acc | pl.col(col_name).is_null(), df.columns, pl.lit(False))
    logger.info(f"Filtering rows with NULL in any of these columns: {', '.join(df.columns)}")

    # collecting rows with problems
    df_with_nulls = df.filter(filter_condition)

    # if null rows found, collect rows and build file
    if not df_with_nulls.is_empty():
        try:
            logger.warning(f'Number of rows with problems: {df_with_nulls.height}\n')
            print(f'{df_with_nulls}\n')

            logger.info('Attempting to write dataframe of rows with nulls to file.')
            output_file = os.path.join(output_folder, f'{os.path.splitext(os.path.basename(input_fofi))[0]}_with_nulls')

            if output_extension == '.csv':
                df_with_nulls.write_csv(f'{output_file}.csv')
            else:
                df_with_nulls.write_parquet(f'{output_file}.parquet')

        except Exception as e:
            logger.error(f'Problematic rows detected. Continuing with valid rows.\n{str(e)}')

        # cleanup the problematic rows and keep the good ones
        if 'df_with_nulls' in locals():
            del df_with_nulls

        df = df.filter(~filter_condition)
        logger.info(f'Remaining rows after filtering rows with empty values: {df.height}')
    else:
        logger.info('No problematic rows found!')

    # do not run this when used in Electron, as it gives a `charmap` error
    if not getattr(sys, 'frozen', False):
        # print example table to terminal.
        print(f'\n{df}\n')

    # update progress - writing output
    progress['update'](progress['get_stage_name'](6))

    # ----------------------------- STEP 5: WRITE OUTPUT ------------------------------ #

    # extract first 10 rows as JSON for return value
    processed_preview = df.head(10).to_dicts()

    output_file = os.path.join(output_folder, os.path.splitext(os.path.basename(input_fofi))[0])

    if output_extension == '.csv':
        df.write_csv(f'{output_file}.csv')
        logger.info('Selected output extension is .csv\n')
    else:
        if output_extension != '.parquet':
            logger.warning('Selected output extension not supported, using parquet.\n')
        df.write_parquet(f'{output_file}.parquet')

    # stop the timer and log the results
    end_time = time.time()
    total_time = end_time - start_time
    time_per_row = total_time / df_rowcount if df_rowcount > 0 else 0

    logger.info(f'Time passed with {multiprocessing.cpu_count()} CPU cores and a total row count of {df_rowcount} rows')
    logger.info(f'Total time: {total_time:.2f} seconds ({time_per_row:.6f} seconds per row)')

    # update progress - finalizing
    progress['update'](progress['get_stage_name'](7))

    result = {'data': processed_preview}
    return json.dumps(result)


def parse_cli_arguments():
    """
    Parse command-line arguments for CLI usage.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Input parameters for the python program.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '--input_fofi',
        nargs='?',
        default='dummy_input.csv',
        help="""
             Name of the input file. Currently .csv and .parquet files are supported.
             """
    )
    parser.add_argument(
        '--input_cols',
        nargs='?',
        default='patientName=Cliëntnaam, report=rapport',
        help="""
             Input column names as a single string with comma separated key=value format e.g.
             Whitespaces are removed, so column names currently can't contain them.
             Keys patientName and report with values existing in the data are essential.
             """
    )
    parser.add_argument(
        '--output_cols',
        nargs='?',
        default='patientID=patientID, processed_report=processed_report',
        help="""
             Output column names with same structure as input_cols. Take care when adding the reports column
             from input_cols, as this likely results in doubling of large data.
             """
    )
    parser.add_argument(
        '--pseudonym_key',
        nargs='?',
        default=None,
        help="""
             Existing pseudonymization key to expand. Supply as JSON file with format {\'name\': \'pseudonym\'}.
             """
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
             """
    )
    # parse and process arguments
    args = parser.parse_args()

    return args


def main():
    """
    This section sets up logging, parses command-line arguments,
    and calls the main function with the parsed arguments.

    Command-line arguments:
      --input_fofi: Specifies the input folder or file
      --input_cols: Defines the mapping of input column names
      --output_cols: Defines the mapping of output column names
      --output_extension: Specifies the output format extension

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
    from logger import setup_logging
    from progress_tracker import tracker
    main()
else:
    from services.logger import setup_logging
    from services.progress_tracker import tracker
