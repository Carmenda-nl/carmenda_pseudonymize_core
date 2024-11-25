"""
Script Name: pyspark_deducer.py
Authors: Lars Spekschoor (lars.spekschoor@radboudumc.nl), Joep Tummers, Pim van Oirschot
Date: 2024-08-12
Version: 0.1.0
Description:
    This script deidentifies Dutch raport texts (unstructured data) using Deduce (Menger et al. 2018, DOI: https://doi.org/10.1016/j.tele.2017.08.002., also on GitHub). 
    It makes use of pyspark to speed up performance. This adds some dependencies and the goal is to handle this with a containerization image. For a python-only version, inquire with authors.
    Performance is unlikely to ever achieve 100%, but there are several things that can be done to maximize it:
    - Update name lookup lists with names occurring in data set. E.g. a health care institution can add a patient list.
    - The current script already assumes a patient name column is present. This is parsed into first name, last name and initials. For special cases, more extensive parsing can be developed.
    - We might add noise to obfuscate false negatives.
Disclaimer:
    This script applying Deduce is a best attempt to pseudonymize data and while we are dedicated to improve performance we cannot guarantee an absence of false negatives. 
    We consider it useful to limit the chance that a researcher would recognize a name in a dataset (researcher knowing an individual combined with that individual not properly being deidentified).
    Datasets should still be handled with care because individuals are likely (re-)identifiable through both false negatives and from context of the report texts.
Script logic:
    - Load relevant module elements
    - Setup pyspark
    - Define functions
    - Load data and when using dummy data, amplify to simulate real-world scenario
    - Apply transformations
        - Identify unique names based on patientName column and map them to randomized patientIDs.
        - Apply Deduce algorithm to report text column, making use of patientName to increase likelihood of at least de-identifying the main subject.
        - Replace the generated [PATIENT] tags with the new patientID
    - Write output to disk (in development, currently writing to temp folder in Linux)
Execution:
    During development, pyspark only worked together with Deduce on Linux.
    - Load correctly built environment (conda or venv), e.g. "source deduce_venv/bin/activate"
    - move to directory containing this script (and as of writing, containing the data as well)
    - Launch script "python3 pyspark_deducer.py"
TODO:
    - Enabling data file selection through command line argument
    - Address terminal warnings and context messages appearing on terminal
"""


##Imports
import argparse

#pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, udf, col, explode, array_repeat, coalesce, struct

#standard modules
import os
import json
import pandas as pd
import numpy as np
import random
import string
import time
import warnings

#deduce
import deduce
from deduce.person import Person

import logging


def main(input_file, custom_cols, pseudonym_key, max_n_processes, output_extention, partition_n, coalesce_n):
    ##Function definitions
    def pseudonymize(unique_names, pseudonym_key = None):
        """
        Purpose:
            Turn a list of unique names into a dictionary, mapping each to a randomized string. Uniqueness of input isresponsibility of caller, but if the generated output is not of the same length (can also occur if not enough unique strings were generated) it raises an assertionError.
        Parameters:
            unique_names (list): list of unique strings.
        Returns:
            Dictionary: keys are the original names, values are the randomized strings.
        """
        if pseudonym_key == None:
            logger_main.info("Building new pseudonym_key because argument was None")
            pseudonym_key = {}
        else:
            logger_main.info("Building from existing pseudonym_key")
        for name in unique_names:
            if name not in pseudonym_key:
                found_new = False
                i = 0
                while (found_new == False and i < 15):
                    i += 1
                    pseudonym_candidate = "".join(random.choices(string.ascii_uppercase+string.ascii_lowercase+string.digits, k=14))
                    if pseudonym_candidate not in pseudonym_key.values():
                        found_new == True
                        pseudonym_key[name] = pseudonym_candidate
        assert len(unique_names) == len(pseudonym_key.items()), "Pseudonymization function safeguard: unique_names (input) and pseudonym_key (output) don't have same length"
        return pseudonym_key


    @udf
    def deduce_with_id(row):
        """
        Purpose:
            Apply the Deduce algorithm
        Parameters:
            report (string): Text entry to apply on.
            patient (string): Patient name to enhance the algorithm performance
        Returns:
            report_deid (string): Deidentified version of report input
        """
        try:
            patient_name = str.split(row[custom_cols["patientName"]], " ", maxsplit = 1)
            patient_initials = "".join([x[0] for x in patient_name])
            
            #Difficult to predict person metadata exactly. I.e. in case of multiple spaces does this indicate multitude of first names or a composed last name?
            #Best will be if input data contains columns for first names, surname, initials
            #patient = Person(first_names = [patient_name[0:-1]], surname = patient_name[-1], initials = patient_initials)
            deduce_patient = Person(first_names = [patient_name[0]], surname = patient_name[1], initials = patient_initials)
            report_deid = deducer.deidentify(row[custom_cols["report"]], metadata={'patient': deduce_patient})
            report_deid = getattr(report_deid, "deidentified_text")
            return report_deid
        except Exception as e:
            print(f"Following row failed to process in function deduce_with_id, returning None:\n{row}")
            return None


    @udf
    def map_dictionary(name):
        """
        Purpose:
            Obtain randomized string corresponding to name
        Parameters:
            name (string): person name
            pseudonym_key_bc (dict, global environment): dictionary of person names (keys, string) and randomized IDs (values, string)
        Returns:
            Value from pseudonym_key_bc corresponding to input name
        """
        return pseudonym_key_bc.value.get(name)
    
    @udf
    def replace_patient_tag(text, new_value, tag = "[PATIENT]"):
        """
        Purpose:
            Deduce labels occurences of patient name in text with a [PATIENT] tag. This function replaces that with the randomized ID.
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
            print(f"Following text failed to process in fucntion replace_patient_tag, returning None:\n{text}")
            return None

    n_processes = min(max_n_processes, max(1, (os.cpu_count() -1))) #On larger systems leave a CPU, also there's no benefit going beyond number of cores available

    spark = SparkSession.builder.master("local[" + str(n_processes) + "]").appName("DeduceApp").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    ##maxPartitionBytes doesn't seem to work for csv input and for parquet the default seems best.
    #spark.conf.set("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024)

    input_ext = os.path.splitext("../data/input/" + input_file)[-1]
    if input_ext == ".csv":
        input_file_size = os.stat("../data/input/" + input_file).st_size
        logger_main.info("CSV input file of size: " + str(input_file_size))
        psdf = spark.read.options(header = True, delimiter = ",", multiline = True).csv("../data/input/" + input_file)
    else:
        psdf = spark.read.parquet("../data/input/" + input_file)


    #If you want to amplify data, usually for performance testing purposes
    n_concat = 1
    psdf = psdf.withColumn(custom_cols["patientName"], explode(array_repeat(custom_cols["patientName"], n_concat)))
    psdf.printSchema()
    psdf_rowcount = psdf.count()
    logger_main.info("Row count: " + str(psdf_rowcount))

    ##An informed decision can be made with the data size and system properties
    ##- more partitions than cores to improve load balancing/skew especially when entries need to be shuffeled (e.g. due to group_by)
    ##- partitions should fit in memory on nodes (recommended is around 256MB)
    if partition_n == None and input_ext == ".csv":
        partition_n = input_file_size * n_concat // (1024**2 * 128) + 1
        #partition_n = psdf_rowcount // 3000 + 1
    psdf = psdf.repartition(partition_n)
    logger_main.info("partitions: " + str(psdf.rdd.getNumPartitions()))
    
    ##Turn names into dictionary of randomized IDs
    logger_main.info(f"Searching for pseudonym key: {pseudonym_key}")
    if pseudonym_key != None:
        pseudonym_key = json.load(open("../data/input/" + pseudonym_key))
    pseudonym_key = pseudonymize(psdf.select(custom_cols["patientName"]).distinct().toPandas()[custom_cols["patientName"]],
                                 pseudonym_key)
    pseudonym_key_bc = spark.sparkContext.broadcast(pseudonym_key)
    with open(("../data/output/" + "pseudonym_key.json"), "w") as outfile:
        json.dump(pseudonym_key, outfile)
    outfile.close()


    #broadcasting this deduce class instance does not work (serialization issue)
    deducer = deduce.Deduce()

    start_t = time.time()
    ##Define transformations, trigger execution by writing output to disk
    psdf = psdf.withColumn("patientID", map_dictionary(custom_cols["patientName"]))\
    .withColumn(custom_cols["report"], deduce_with_id(struct(*psdf.columns)))\
    .withColumn(custom_cols["report"], replace_patient_tag(custom_cols["report"], "patientID"))

    issue_rows = psdf.filter(psdf[custom_cols["report"]].isNull())
    logger_main.info(f"Row count with issues: {issue_rows.count()}")
    issue_rows = issue_rows.collect()
    for row in issue_rows:
        logger_issues.error(row)
    psdf = psdf.filter(psdf[custom_cols["report"]].isNotNull())
    logger_main.info(f"Remaining rows: {psdf.count()}")

    existing_cols = psdf.columns
    select_cols = ["patientID", custom_cols["time"], custom_cols["caretakerName"], custom_cols["report"]]
    select_cols = [col for col in select_cols if col in existing_cols]
    psdf = psdf.select(select_cols)

    if coalesce_n != None:
        psdf = psdf.coalesce(coalesce_n)
        
    if output_extention == ".csv":
        output_file = "../data/output/" + os.path.splitext(os.path.basename(input_file))[0] + "_processed"
        psdf.write.mode("overwrite").csv(output_file)
    else:
        if output_extention != ".parquet":
            warnings.warn("Selected output extention not supported, using parquet.")
        output_file = "../data/output/" + os.path.splitext(os.path.basename(input_file))[0] + "_processed"
        psdf.write.mode("overwrite").parquet(output_file)
    #coalesceing is single-threaded, only do when necessary
    #psdf.coalesce(1).write.mode("overwrite").csv("/tmp/" + filename + "_processed")
    psdf_row_count = psdf.count()
    end_t = time.time() #timeit.timeit()
    logger_main.info(f"time passed with n_processes = {n_processes} and row count = {psdf_rowcount}: {end_t - start_t}s ({(end_t - start_t) / psdf_rowcount}s / row)")
    psdf.printSchema()
    #logger_main.info(psdf.take(10))


    #Close shop
    spark.stop()

if __name__ == "__main__":
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    
    logger_main = logging.getLogger(__name__)
    logger_main.setLevel(logging.INFO)
    handler_main = logging.FileHandler("../data/output/logger_main.log")
    handler_main.setFormatter(formatter)
    logger_main.addHandler(handler_main)

    logger_issues = logging.getLogger("issues")
    logger_issues.setLevel(logging.WARNING)
    handler_issues = logging.FileHandler("../data/output/logger_issues.log")
    handler_issues.setFormatter(formatter)
    logger_issues.addHandler(handler_issues)

    #This can help suppress extremely verbose logs (somehow the default is set to DEBUG)
    logger_py4j = logging.getLogger("py4j")
    logger_py4j.setLevel("ERROR") #Default seems to be DEBUG, good alternative might be WARNING
    logger_py4j.addHandler(logging.FileHandler("../data/output/logger_py4j.log"))

    parser = argparse.ArgumentParser(description = "Input parameters for the python programme.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input_file", 
                        nargs="?", 
                        default="dummy_input.csv", 
                        help="Name of the input file")
    parser.add_argument("--custom_cols", 
                        nargs="?", 
                        help="Column names as a single string of format \"patientName=foo, time=bar, caretakerName=foobar, report=barfoo\". Whitespaces are removed, so column names currently can't contain them.", 
                        default="patientName=Cliëntnaam, time=Tijdstip, caretakerName=Zorgverlener, report=rapport")
    parser.add_argument("--pseudonym_key",
                        nargs="?",
                        default=None,
                        help = "Existing pseudonymization key to expand. Supply as JSON file with format {\"name\": \"pseudonym\"}.")
    parser.add_argument("--max_n_processes",
                        nargs="?",
                        default=os.cpu_count(),
                        help = "Maximum number of processes. Default behavior is to detect the number of cores on the system and subtract 1. Going above the number of available cores has no benefit.")
    parser.add_argument("--output_extention",
                        nargs="?",
                        default = ".parquet",
                        help = "Select output format, currently only parquet (default) and csv are supported.")
    parser.add_argument("--partition_n",
                        nargs = "?",
                        default = None,
                        help = "Manually set number of partitions during processing phase. The default makes an estimation based on file size for .csv input or used default settings for .parquet.")
    parser.add_argument("--coalesce_n",
                        nargs="?",
                        default = None,
                        help = "Should you want to control the number of partitions of the output this option can be used. Default is None because coalesceing and output writing to single file is slow. Because coalesce is different from reshuffle, this argument cannot be larger than partition_n")

    def parse_custom_cols(mapping_str):
        return dict(item.strip().split("=") for item in mapping_str.split(","))
    
    args = parser.parse_args()
    args.custom_cols = parse_custom_cols(args.custom_cols)
    if args.partition_n != None:
        args.partition_n = int(args.partition_n)
    if args.coalesce_n != None:
        args.coalesce_n = int(args.coalesce_n) 
    logger_main.info(args)

    main(args.input_file, args.custom_cols, args.pseudonym_key, args.max_n_processes, args.output_extention, args.partition_n, args.coalesce_n)