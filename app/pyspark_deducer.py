# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""
Script Name: pyspark_deducer.py
Authors: Lars Spekschoor (lars.spekschoor@radboudumc.nl), Joep Tummers, Pim van Oirschot, Django Heimgartner
Date: 2025-04-14
Description:
    This script deidentifies Dutch report texts (unstructured data) using Deduce
    (Menger et al. 2018, DOI: https://doi.org/10.1016/j.tele.2017.08.002., also on GitHub).
    The column/field in source data marked as patient names is used to generate unique codes for each value.
    This makes it possible to use the same code across entries.
    Pyspark allows for performance on large input data sizes. Dependencies are handled with containerization.
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

import deduce
from deduce.person import Person

from pyspark.sql.functions import udf, col, explode, array_repeat, struct, concat_ws
from functools import reduce

import os
import random
import string
import json
import time
import warnings
import argparse
import glob


def pseudonymize(unique_names, pseudonym_key=None, droplist=[], logger=None):
    """
    Purpose:
        Turn a list of unique names into a dictionary, mapping each to a randomized string.
        Uniqueness of input isresponsibility of caller, but if the generated output is not
        of the same length (can also occur if not enough unique strings were generated) it raises an assertionError.
    Parameters:
        unique_names (list): list of unique strings.
    Returns:
        Dictionary: keys are the original names, values are the randomized strings.
    """
    unique_names = [name for name in unique_names if name not in droplist]

    if pseudonym_key is None:
        logger.info("Building new key because argument was None")
        pseudonym_key = {}
    else:
        logger.info("Building from existing key")

    for name in unique_names:
        if name not in pseudonym_key:
            found_new = False
            iterate = 0

            while (found_new is False and iterate < 15):
                iterate += 1
                pseudonym_candidate = "".join(
                    random.choices(string.ascii_uppercase + string.ascii_uppercase+string.digits, k=14)
                )
                if pseudonym_candidate not in pseudonym_key.values():
                    found_new is True
                    pseudonym_key[name] = pseudonym_candidate

    error_message = "Pseudonymization safeguard: unique_names (input) and pseudonym_key (output) don't have same length"
    assert len(unique_names) == len(pseudonym_key.items()), error_message
    pseudonym_key[None] = None

    return pseudonym_key


def map_dictionary_func(pseudonym_key_bc):
    """
    Purpose:
        Obtain randomized string corresponding to name
    Parameters:
        name (string): person name
        pseudonym_key_bc (dict, global environment): dictionary of person names (keys, string) and
        randomized IDs (values, string)
    Returns:
        Value from pseudonym_key_bc corresponding to input name
    """
    def inner_func(name):
        return pseudonym_key_bc.value.get(name, None)

    return inner_func


def deduce_with_id_func(input_cols_bc):
    """
    Purpose:
        Apply the Deduce algorithm
    Parameters:
        report (string): Text entry to apply on.
        patient (string): Patient name to enhance the algorithm performance
    Returns:
        report_deid (string): Deidentified version of report input
    """
    deduce_instance = deduce.Deduce()

    def inner_func(row):
        try:
            input_cols = input_cols_bc.value

            patient_name = str.split(row[input_cols["patientName"]], " ", maxsplit=1)
            patient_initials = "".join([x[0] for x in patient_name])

            """
            Difficult to predict person metadata exactly. I.e. in case of multiple spaces does this indicate
            multitude of first names or a composed last name?

            Best will be if input data contains columns for first names, surname, initials
            patient = Person(first_names=[patient_name[0:-1]], surname=patient_name[-1], initials=patient_initials)
            """
            deduce_patient = Person(first_names=[patient_name[0]], surname=patient_name[1], initials=patient_initials)

            report_deid = deduce_instance.deidentify(row[input_cols["report"]], metadata={"patient": deduce_patient})
            report_deid = getattr(report_deid, "deidentified_text")

            return report_deid

        except Exception as e:
            logger.error(f"{e} row failed to process")
            return None

    return inner_func


def deduce_report_only_func(input_cols_bc):
    """
    Purpose:
        Apply the Deduce algorithm
    Parameters:
        report (string): Text entry to apply on.
    Returns:
        report_deid (string): Deidentified version of report input
    """
    deduce_instance = deduce.Deduce()

    def inner_func(row):
        try:
            input_cols = input_cols_bc.value

            report_deid = deduce_instance.deidentify(row[input_cols["report"]])
            report_deid = getattr(report_deid, "deidentified_text")

            return report_deid

        except Exception as e:
            logger.error(f"{e} row failed to process")
            return None

    return inner_func


@udf
def replace_tags_udf(text, new_value, tag="[PATIENT]"):
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
        return str.replace(text, "[PATIENT]", f"[{new_value}]")
    except Exception as e:
        logger.error(f"Failed to process in replace_patient_tag, returning None \n {e}")
        return None


def progress_tracker(logger, tracker):
    """
    Creates a progress tracking mechanism for multi-stage processing.

    Parameters:
        logger: Logger object to log progress messages
        tracker: Global tracker object to update overall progress

    Returns:
        Dictionary containing progress tracking functions and data
    """
    progress_stages = [
        "Initializing", "Loading data", "Pre-processing",
        "Pseudonymization", "Data transformation", "Filtering nulls",
        "Writing output", "Finalizing"
    ]

    total_stages = len(progress_stages)

    progress_data = {
        "current_stage": 0,
        "progress_percentage": 0,
        "total_stages": total_stages,
        "stages": progress_stages
    }

    def update_progress(stage_name=None):
        if stage_name is None and progress_data["current_stage"] < total_stages:
            stage_name = progress_stages[progress_data["current_stage"]]

        progress_data["current_stage"] += 1
        progress_data["progress_percentage"] = int((progress_data["current_stage"] / total_stages) * 100)

        # update the global progress tracker
        tracker.update_progress(
            progress_data["progress_percentage"],
            stage_name
        )

        return progress_data["progress_percentage"]

    return {
        "update": update_progress,
        "data": progress_data,
        "get_stage_name": lambda idx: progress_stages[idx] if 0 <= idx < len(progress_stages) else None
    }


def process_data(
    input_fofi, input_cols, output_cols, pseudonym_key,
    max_n_processes, output_extension, partition_n, coalesce_n, logger=None
):
    # if no logger is provided, set up default logging
    if logger is None:
        logger, logger_py4j = setup_logging()

    # start progress tracking
    progress = progress_tracker(logger, tracker)
    progress["update"](progress["get_stage_name"](0))

    # on larger systems leave a CPU, also there's no benefit going beyond number of cores available
    n_processes = min(max_n_processes, max(1, (os.cpu_count() - 1)))
    spark = get_spark(n_processes)

    input_folder = os.getenv("INPUT_FOLDER") or "/data/input/"
    output_folder = os.getenv("OUTPUT_FOLDER") or "/data/output/"

    input_extension = os.path.splitext(input_folder + input_fofi)[-1]

    # update progress - loading data
    progress["update"](progress["get_stage_name"](1))

    if input_extension == ".csv":
        input_fofi_size = os.stat(input_folder + input_fofi).st_size
        logger.info(f"CSV input file of size: {input_fofi_size}")
        psdf = spark.read.options(header=True, delimiter=",", multiline=True).csv(input_folder + input_fofi)
    else:
        psdf = spark.read.parquet(input_folder + input_fofi)

    # update progress - pre-processing
    progress["update"](progress["get_stage_name"](2))

    # convert string mappings to dictionaries
    input_cols = dict(item.strip().split("=") for item in input_cols.split(","))
    output_cols = dict(item.strip().split("=") for item in output_cols.split(","))

    # check if the `patientName` column is available
    patient_name = input_cols.get("patientName", None) in psdf.columns

    # if you want to amplify (dublicate) data, usually for performance testing purposes
    n_concat = 1

    if patient_name:
        psdf = psdf.withColumn(input_cols["patientName"], explode(array_repeat(input_cols["patientName"], n_concat)))

    psdf.printSchema()

    # count rows
    psdf_rowcount = psdf.count()
    logger.info(f"Row count: {psdf_rowcount}")

    """
    An informed decision can be made with the data size and system properties
      - more partitions than cores to improve load balancing/skew especially when entries need to be shuffeled
      - partitions should fit in memory on nodes (recommended is around 256MB)
    """
    if partition_n is None and input_extension == ".csv":
        # big files -> more partitions, much rows -> more partitions
        partition_n = max(
            input_fofi_size * n_concat // (1024**2 * 128) + 1,
            psdf_rowcount // 1000 + 1
        )

    if partition_n is not None:
        logger.info("Repartitioning")
        psdf = psdf.repartition(partition_n)

    logger.info(f"partitions: {psdf.rdd.getNumPartitions()}")

    # update progress - pseudonymization
    progress["update"](progress["get_stage_name"](3))

    input_cols_bc = spark.sparkContext.broadcast(input_cols)
    start_time = time.time()

    # turn names into a dictionary of randomized IDs
    logger.info(f"Searching for pseudonym key: {pseudonym_key}")
    if pseudonym_key is not None:
        pseudonym_key = json.load(open(input_folder + pseudonym_key))

    # create an pseudonym_key when `patientName` exists
    if patient_name:
        pseudonym_key = pseudonymize(
            psdf.select(input_cols["patientName"]).distinct().toPandas()[input_cols["patientName"]],
            pseudonym_key,
            logger=logger
        )

        # write to json file
        with open((output_folder + "pseudonym_key.json"), "w") as outfile:
            json.dump(pseudonym_key, outfile)

        outfile.close()

        # update progress - data transformation
        progress["update"](progress["get_stage_name"](4))

        pseudonym_key_bc = spark.sparkContext.broadcast(pseudonym_key)
        map_dictionary_udf = udf(map_dictionary_func(pseudonym_key_bc))

        deduce_with_id_udf = udf(deduce_with_id_func(input_cols_bc))

        """
        define transformations, trigger execution by writing output to disk
          1. create a new column `patientID`
          2. create a new column `processed_report`
          3. replace [PATIENT] tags
        """
        psdf = psdf \
            .withColumn("patientID", map_dictionary_udf(input_cols["patientName"]))\
            .withColumn("processed_report", deduce_with_id_udf(struct(*psdf.columns)))\
            .withColumn("processed_report", replace_tags_udf("processed_report", "patientID"))
    else:
        logger.info("No patientName column, extracting names from reports")

        # update progress - data transformation
        progress["update"](progress["get_stage_name"](4))

        deduce_report_only_udf = udf(deduce_report_only_func(input_cols_bc))

        """
        define transformations, trigger execution by writing output to disk
          1. Apply deidentification to the report
          2. Replace [PATIENT] tags with a constant value
        """
        psdf = psdf \
            .withColumn("processed_report", deduce_report_only_udf(struct(*psdf.columns)))

    select_cols = [col for col in output_cols.values() if col in psdf.columns]
    psdf = psdf.select(select_cols)
    logger.info(f"Output columns: {psdf.columns}")

    # update progress - filtering nulls
    progress["update"](progress["get_stage_name"](5))

    # handling of irregularities
    filter_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in psdf.columns])
    logger.info(filter_condition)

    try:
        psdf_with_nulls = psdf.filter(filter_condition)
        logger.info(f"Row count with problems: {psdf_with_nulls.count()}")
        logger.info("Attempting to write dataframe of rows with nulls to file.")
        output_file = output_folder + os.path.splitext(os.path.basename(input_fofi))[0] + "_with_nulls"

        psdf_with_nulls.write.mode("overwrite").csv(output_file)
        psdf_with_nulls.unpersist()

    except Exception as e:
        logger.error(
            f"Some rows are so problematic that pyspark couldn't write them to file.\n\
            In order not to lose progress on good rows, program will continue for those.\n\
            Caught Exception:\n {e}"
        )

        try:
            psdf_with_nulls.unpersist()
        except Exception:
            pass

    # keep the rows without nulls
    psdf = psdf.filter(~filter_condition)
    psdf.show(10)
    logger.info(
        f"Remaining rows after filtering rows with empty values (that happened during processing): {psdf.count()}"
    )
    if coalesce_n is not None:
        psdf = psdf.coalesce(coalesce_n)

    # extract first 10 rows as JSON for return value
    processed_preview = psdf.limit(10).toPandas().to_dict(orient="records")

    # update progress - writing output
    progress["update"](progress["get_stage_name"](6))

    output_file = output_folder + os.path.splitext(os.path.basename(input_fofi))[0] + "_processed"

    if output_extension == ".csv":
        psdf.write.mode("overwrite").option("header", "true").csv(output_file)

        csv_files = glob.glob(os.path.join(output_file, 'part-*.csv'))

        if csv_files:
            file_to_rename = csv_files[0]
            new_name = os.path.join(output_file, input_fofi)
            os.rename(file_to_rename, new_name)

    elif output_extension == ".txt":
        psdf = psdf.select(concat_ws(", ", *psdf.columns).alias("value"))
        psdf.write.mode("overwrite").option("header", "true").text(output_file)
    else:
        if output_extension != ".parquet":
            warnings.warn("Selected output extension not supported, using parquet.")

        psdf.write.mode("overwrite").option("header", "true").parquet(output_file)

    end_time = time.time()
    logger.info(
        f"Time passed with {n_processes} processes and row count (start, end) = ({psdf_rowcount}, \
            {psdf.count()}): {end_time - start_time}s ({(end_time - start_time) / psdf_rowcount}s / row)"
    )
    psdf.printSchema()

    # update progress - finalizing
    progress["update"](progress["get_stage_name"](7))

    # close shop
    spark.stop()
    result = {"data": processed_preview}

    return json.dumps(result)


def parse_cli_arguments():
    """
    Parse command-line arguments for CLI usage.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Input parameters for the python program.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--input_fofi",
        nargs="?",
        default="dummy_input.csv",
        help="""
             Name of the input folder or file. Currently csv file and parquet file or folder are supported.
             """
    )
    parser.add_argument(
        "--input_cols",
        nargs="?",
        default="patientName=Cliëntnaam, report=rapport",
        help="""
             Input column names as a single string with comma separated key=value format e.g.
             Whitespaces are removed, so column names currently can't contain them.
             Keys patientName and report with values existing in the data are essential.
             Optional columns reflect functionality that may not have been implemented (yet),
             """
    )
    parser.add_argument(
        "--output_cols",
        nargs="?",
        default="patientID=patientID, processed_report=processed_report",
        help="""
             Output column names with same structure as input_cols. Take care when adding the reports column
             from input_cols, as this likely results in doubling of large data.
             """
    )
    parser.add_argument(
        "--pseudonym_key",
        nargs="?",
        default=None,
        help="""
             Existing pseudonymization key to expand. Supply as JSON file with format {\'name\': \'pseudonym\'}.
             """
    )
    parser.add_argument(
        "--max_n_processes",
        nargs="?",
        default=2,  # used to be os.cpu_count() but that doesn't work nice in containers.
        help="""
             Maximum number of processes. Default behavior is to detect the number of cores on the
             system and subtract 1. Going above the number of available cores has no benefit.
             """
    )
    parser.add_argument(
        "--output_extension",
        nargs="?",
        default=".parquet",
        help="""
             Select output format, currently only parquet (default), csv and txt are supported.
             CSV is supported to provide a human readable format, but is not recommended for (large) datasets
             with potential irregular fields (e.g. containing data that can be misinterpreted such as
             early string closure symbols followed by the separator symbol).
             """
    )
    parser.add_argument(
        "--partition_n",
        nargs="?",
        default=None,
        help="""
             Manually set number of partitions during processing phase. The default makes an estimation based
             on file size and row count for .csv input or used default settings for .parquet.
             """
    )
    parser.add_argument(
        "--coalesce_n",
        nargs="?",
        default=None,
        help="""
             Should you want to control the number of partitions of the output this option can be used.
             Default is None because coalesceing and output writing to single file is slow.
             Because coalesce is different from reshuffle, this argument cannot be larger than partition_n
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
        --input_fofi`: Specifies the input folder or file
        --input_cols`: Defines the mapping of input column names
        --output_cols`: Defines the mapping of output column names
        --max_n_processes`: Defines the maximum number of parallel processes
        --output_extension`: Specifies the output format extension

        --pseudonym_key`: Path to an existing pseudonymization key in JSON format (optional).
        --partition_n`: Manually set the number of partitions during processing (optional).
        --coalesce_n`: Manually set the number of output partitions after processing (optional).
    """
    args = parse_cli_arguments()

    parsed_args_str = "\n".join(f" |-- {key}={value}" for key, value in vars(args).items())
    logger.info(f"Parsed arguments:\n{parsed_args_str}\n")

    process_data(
        input_fofi=args.input_fofi,
        input_cols=args.input_cols,
        output_cols=args.output_cols,
        pseudonym_key=args.pseudonym_key,
        max_n_processes=int(args.max_n_processes),
        output_extension=args.output_extension,
        partition_n=int(args.partition_n) if args.partition_n else None,
        coalesce_n=int(args.coalesce_n) if args.coalesce_n else None,
        logger=logger
    )


if __name__ == "__main__":
    from spark_session import get_spark
    from logger import setup_logging
    from progress_tracker import tracker

    logger, logger_py4j = setup_logging()
    main()
else:
    from services.spark_session import get_spark
    from services.logger import setup_logging
    from services.progress_tracker import tracker

    logger, logger_py4j = setup_logging()
