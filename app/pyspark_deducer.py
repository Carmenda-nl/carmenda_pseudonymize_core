"""
Script Name: pyspark_deducer.py
Authors: Lars Spekschoor (lars.spekschoor@radboudumc.nl), Joep Tummers, Pim van Oirschot, Django Heimgartner
Date: 2025-03-25
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
        - There is some error handling and logging, but it's so far only been used for debugging.
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
                    random.choices(string.ascii_uppercase + string.ascii_lowercase+string.digits, k=14)
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
            report_deid = deduce_instance.deidentify(row[input_cols["report"]], metadata={'patient': deduce_patient})
            report_deid = getattr(report_deid, "deidentified_text")
            return report_deid
        except Exception as e:
            logger.error(f"{e} row failed to process in deduce_with_id")
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
        return str.replace(text, "[PATIENT]", new_value)
    except Exception as e:
        logger.error(f"{e} Failed to process in replace_patient_tag, returning None")
        return None


def main(input_fofi, input_cols, output_cols, pseudonym_key, max_n_processes, output_extension, partition_n, coalesce_n):

    # On larger systems leave a CPU, also there's no benefit going beyond number of cores available
    n_processes = min(max_n_processes, max(1, (os.cpu_count() - 1)))
    spark = get_spark(n_processes)

    input_ext = os.path.splitext("/data/input/" + input_fofi)[-1]
    if input_ext == ".csv":
        input_fofi_size = os.stat("/data/input/" + input_fofi).st_size
        logger.info("CSV input file of size: " + str(input_fofi_size))
        psdf = spark.read.options(header=True, delimiter=",", multiline=True).csv(
            "/data/input/" + input_fofi)
    else:
        psdf = spark.read.parquet("/data/input/" + input_fofi)

    # If you want to amplify data, usually for performance testing purposes
    n_concat = 1
    psdf = psdf.withColumn(input_cols["patientName"], explode(
        array_repeat(input_cols["patientName"], n_concat)))
    psdf.printSchema()
    # For debugging
    # psdf = psdf.limit(100)
    psdf_rowcount = psdf.count()
    logger.info("Row count: " + str(psdf_rowcount))

    # An informed decision can be made with the data size and system properties
    # - more partitions than cores to improve load balancing/skew especially when entries need to be shuffeled (e.g. due to group_by)
    # - partitions should fit in memory on nodes (recommended is around 256MB)
    if partition_n == None and input_ext == ".csv":
        partition_n = max(
            input_fofi_size * n_concat // (1024**2 * 128) + 1,
            psdf_rowcount // 1000 + 1
        )
    if partition_n != None:
        logger.info("Repartitioning")
        psdf = psdf.repartition(partition_n)
    logger.info("partitions: " + str(psdf.rdd.getNumPartitions()))

    # Turn names into dictionary of randomized IDs
    logger.info(f"Searching for pseudonym key: {pseudonym_key}")
    if pseudonym_key != None:
        pseudonym_key = json.load(open("/data/input/" + pseudonym_key))
    pseudonym_key = pseudonymize(psdf.select(input_cols["patientName"]).distinct().toPandas()[input_cols["patientName"]],
                                 pseudonym_key)
    with open(("/data/output/" + "pseudonym_key.json"), "w") as outfile:
        json.dump(pseudonym_key, outfile)
    outfile.close()
    pseudonym_key_bc = spark.sparkContext.broadcast(pseudonym_key)
    input_cols_bc = spark.sparkContext.broadcast(input_cols)

    map_dictionary_udf = udf(map_dictionary_func(pseudonym_key_bc))
    deduce_with_id_udf = udf(deduce_with_id_func(input_cols_bc))

    start_t = time.time()
    # Define transformations, trigger execution by writing output to disk
    psdf = psdf.withColumn("patientID", map_dictionary_udf(input_cols["patientName"]))\
        .withColumn("processed_report", deduce_with_id_udf(struct(*psdf.columns)))\
        .withColumn("processed_report", replace_tags_udf("processed_report", "patientID"))

    select_cols = [col for col in output_cols.values() if col in psdf.columns]
    psdf = psdf.select(select_cols)
    logger.info(f"Output columns: {psdf.columns}")

    # Handling of irregularities
    filter_condition = reduce(
        lambda x, y: x | y, [col(c).isNull() for c in psdf.columns])
    print(filter_condition)
    # To avoid some redundancy
    # psdf.cache()
    try:
        psdf_with_nulls = psdf.filter(filter_condition)
        logger.info(f"Row count with problems: {psdf_with_nulls.count()}")
        logger.info(
            f"Attempting to write dataframe of rows with nulls to file.")
        output_file = "/data/output/" + \
            os.path.splitext(os.path.basename(input_fofi))[0] + "_with_nulls"
        psdf_with_nulls.write.mode("overwrite").csv(output_file)
        psdf_with_nulls.unpersist()
    except Exception as e:
        logger.error(f"Some rows are so problematic that pyspark couldn't write them to file.\n\
                    In order not to lose progress on good rows, program will continue for those.\n\
                    Caught Exception:\n\
                    {e}")
        try:
            psdf_with_nulls.unpersist()
        except:
            pass
    # Keep the rows without nulls
    psdf = psdf.filter(~filter_condition)
    psdf.show(10)
    logger.info(
        f"Remaining rows after filtering rows with empty values (which are usually indicative of exceptions that happened during processing): {psdf.count()}")

    if coalesce_n != None:
        psdf = psdf.coalesce(coalesce_n)

    if output_extension == ".csv":
        output_file = "/data/output/" + \
            os.path.splitext(os.path.basename(input_fofi))[0] + "_processed"
        psdf.write.mode("overwrite").csv(output_file)
    elif output_extension == ".txt":
        output_file = "/data/output/" + \
            os.path.splitext(os.path.basename(input_fofi))[0] + "_processed"
        psdf = psdf.select(concat_ws(', ', *psdf.columns).alias("value"))
        psdf.write.mode("overwrite").text(output_file)
    else:
        if output_extension != ".parquet":
            warnings.warn(
                "Selected output extension not supported, using parquet.")
        output_file = "/data/output/" + \
            os.path.splitext(os.path.basename(input_fofi))[0] + "_processed"
        psdf.write.mode("overwrite").parquet(output_file)
    # coalesceing is single-threaded, only do when necessary
    # psdf.coalesce(1).write.mode("overwrite").csv("/tmp/" + filename + "_processed")
    end_t = time.time()  # timeit.timeit()
    logger.info(
        f"time passed with n_processes = {n_processes} and row count (start, end) = ({psdf_rowcount}, {psdf.count()}): {end_t - start_t}s ({(end_t - start_t) / psdf_rowcount}s / row)")
    psdf.printSchema()
    # logger.info(psdf.take(10))

    # Close shop
    spark.stop()


if __name__ == "__main__":
    from spark_session import get_spark
    from logger import setup_logging

    logger, logger_py4j = setup_logging()

    parser = argparse.ArgumentParser(description="Input parameters for the python programme.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input_fofi",
                        nargs="?",
                        default="dummy_input.csv",
                        help="Name of the input folder or file. Currently csv file and parquet file or folder are supported.")
    parser.add_argument("--input_cols",
                        nargs="?",
                        help="Input column names as a single string with comma separated key=value format e.g. \"patientName=foo, report=bar, ...=foobar\". Whitespaces are removed, so column names currently can't contain them. Keys patientName and report with values existing in the data are essential. Optional columns reflect functionality that may not have been implemented (yet)",
                        default="patientName=Cliëntnaam, time=Tijdstip, caretakerName=Zorgverlener, report=rapport"),
    parser.add_argument("--output_cols",
                        nargs="?",
                        help="Output column names with same structure as input_cols. Take care when adding the reports column from input_cols, as this likely results in doubling of large data.",
                        default="patientID=patientID, processed_report=processed_report"),
    parser.add_argument("--pseudonym_key",
                        nargs="?",
                        default=None,
                        help="Existing pseudonymization key to expand. Supply as JSON file with format {\"name\": \"pseudonym\"}.")
    parser.add_argument("--max_n_processes",
                        nargs="?",
                        # Used to be os.cpu_count() but that doesn't work nice in Docker containers.
                        default=2,
                        help="Maximum number of processes. Default behavior is to detect the number of cores on the system and subtract 1. Going above the number of available cores has no benefit.")
    parser.add_argument("--output_extension",
                        nargs="?",
                        default=".parquet",
                        help="Select output format, currently only parquet (default), csv and txt are supported. CSV is supported to provide a human readable format, but is not recommended for (large) datasets with potential irregular fields (e.g. containing data that can be misinterpreted such as early string closure symbols followed by the separator symbol).")
    parser.add_argument("--partition_n",
                        nargs="?",
                        default=None,
                        help="Manually set number of partitions during processing phase. The default makes an estimation based on file size and row count for .csv input or used default settings for .parquet.")
    parser.add_argument("--coalesce_n",
                        nargs="?",
                        default=None,
                        help="Should you want to control the number of partitions of the output this option can be used. Default is None because coalesceing and output writing to single file is slow. Because coalesce is different from reshuffle, this argument cannot be larger than partition_n")

    def parse_dict_cols(mapping_str):
        return dict(item.strip().split("=") for item in mapping_str.split(","))

    def parse_list_cols(mapping_str):
        return [item.strip() for item in mapping_str.split(",")]

    args = parser.parse_args()
    args.input_cols = parse_dict_cols(args.input_cols)
    args.output_cols = parse_dict_cols(args.output_cols)
    if args.partition_n != None:
        args.partition_n = int(args.partition_n)
    if args.coalesce_n != None:
        args.coalesce_n = int(args.coalesce_n)
    args.max_n_processes = int(args.max_n_processes)
    logger.info(args)

    main(args.input_fofi, args.input_cols, args.output_cols, args.pseudonym_key,
         args.max_n_processes, args.output_extension, args.partition_n, args.coalesce_n)
