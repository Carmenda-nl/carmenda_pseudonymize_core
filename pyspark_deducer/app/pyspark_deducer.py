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
from pyspark.sql.functions import pandas_udf, udf, col, explode, array_repeat

#standard modules
import os
import json
import pandas as pd
import numpy as np
import random
import string
import time

#deduce
import deduce
from deduce.person import Person

import logging


def main(input_file, custom_cols):
    ##Function definitions
    def pseudonymize(unique_names):
        """
        Purpose:
            Turn a list of unique names into a dictionary, mapping each to a randomized string. Uniqueness of input isresponsibility of caller, but if the generated output is not of the same length (can also occur if not enough unique strings were generated) it raises an assertionError.
        Parameters:
            unique_names (list): list of unique strings.
        Returns:
            Dictionary: keys are the original names, values are the randomized strings.
        """
        name_map = {}
        for name in unique_names:
            found_new = False
            i = 0
            while (found_new == False and i < 10):
                i += 1
                randomization = "".join(random.choices(string.ascii_uppercase+string.digits, k=12))
                if randomization not in name_map:
                    found_new == True
                    name_map[name] = randomization
        assert len(unique_names) == len(name_map.items()), "Pseudonymization function safeguard: unique_names (input) and name_map (output) don't have same length"
        return name_map


    @udf
    def deduce_with_id(report, patient):
        """
        Purpose:
            Apply the Deduce algorithm
        Parameters:
            report (string): Text entry to apply on.
            patient (string): Patient name to enhance the algorithm performance
        Returns:
            report_deid (string): Deidentified version of report input
        """
        patient_name = str.split(patient, " ", maxsplit = 1)
        patient_initials = "".join([x[0] for x in patient_name])
        
        #Difficult to predict person metadata exactly. I.e. in case of multiple spaces does this indicate multitude of first names or a composed last name?
        #Best will be if input data contains columns for first names, surname, initials
        #patient = Person(first_names = [patient_name[0:-1]], surname = patient_name[-1], initials = patient_initials)
        patient = Person(first_names = [patient_name[0]], surname = patient_name[1], initials = patient_initials)
        report_deid = deducer.deidentify(report, metadata={'patient': patient})
        report_deid = getattr(report_deid, "deidentified_text")
        return report_deid


    @udf
    def map_dictionary(name):
        """
        Purpose:
            Obtain randomized string corresponding to name
        Parameters:
            name (string): person name
            name_map_bc (dict, global environment): dictionary of person names (keys, string) and randomized IDs (values, string)
        Returns:
            Value from name_map_bc corresponding to input name
        """
        return name_map_bc.value.get(name)
    
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
        return str.replace(text, "[PATIENT]", new_value)

    
    #This can help suppress extremely verbose logs (somehow the default is set to DEBUG)
    logger = logging.getLogger("py4j")
    logger.setLevel("ERROR") #Default seems to be DEBUG, good alternative might be WARNING

    n_threads = 3#max(2, (os.cpu_count() -1)) #On larger systems leave a CPU, also there's no benefit going beyond number of cores available

    spark = SparkSession.builder.master("local[" + str(n_threads) + "]").appName("DeduceApp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    psdf = spark.read.options(header = True, delimiter = ",", multiline = True).csv("../data/input/" + input_file)\
    
    #If you want to amplify data, usually for performance testing purposes
    n_concat = 1
    psdf = psdf.withColumn(custom_cols["patientName"], explode(array_repeat(custom_cols["patientName"], n_concat)))
    psdf.printSchema()
    psdf_rowcount = psdf.count()
    print("Row count: " + str(psdf_rowcount))

    #broadcasting this class instance does not work (serialization issue)
    print("Deduce will now be initialized, which unfortunately has to be redone (for now) when running inside container.")
    deducer = deduce.Deduce()

    #An informed decision can be made with the data size and system properties
    #- more partitions than cores to improve load balancing/skew especially when entries need to be shuffeled (e.g. due to group_by)
    #- partitions should fit in memory on nodes (recommended is around 256MB)
    ##Amplify for testing purposes
    n_partitions = psdf_rowcount // 3000 + 1
    psdf = psdf.repartition(n_partitions)

    print("count: " + str(psdf_rowcount))
    print("partitions: " + str(psdf.rdd.getNumPartitions()))

    ##Turn names into dictionary of randomized IDs
    name_map = pseudonymize(psdf.select(custom_cols["patientName"]).distinct().toPandas()[custom_cols["patientName"]])
    name_map_bc = spark.sparkContext.broadcast(name_map)
    with open(("../data/output/" + "name_map.json"), "w") as outfile:
        json.dump(name_map, outfile)
    outfile.close()

    start_t = time.time()
    ##Define transformations, trigger execution by writing output to disk
    psdf = psdf.withColumn("patientID", map_dictionary(custom_cols["patientName"]))\
    .withColumn(custom_cols["report"], deduce_with_id(custom_cols["report"], custom_cols["patientName"]))\
    .withColumn(custom_cols["report"], replace_patient_tag(custom_cols["report"], "patientID"))\
    .select("patientID", custom_cols["time"], custom_cols["caretakerName"], custom_cols["report"])

    output_file = "../data/output/" + os.path.splitext(os.path.basename(input_file))[0] + "_processed.csv"
    psdf.write.mode("overwrite").csv(output_file)
    #coalesceing is single-threaded, only do when necessary
    #psdf.coalesce(1).write.mode("overwrite").csv("/tmp/" + filename + "_processed")
    psdf_row_count = psdf.count()
    end_t = time.time() #timeit.timeit()
    print(f"time passed with n_threads = {n_threads} and row count = {psdf_rowcount}: {end_t - start_t}s ({(end_t - start_t) / psdf_rowcount}s / row)")
    psdf.printSchema()
    print(psdf.take(10))


    #Close shop
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Input parameters for the python programme.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input_file", 
                        nargs="?", 
                        default="input.csv", 
                        help="Name of the input file")
    parser.add_argument("--custom_cols", 
                        nargs="?", 
                        help="Column names as a single string of format \"patientName=foo, time=bar, caretakerName=foobar, report=barfoo\". Whitespaces are removed, so column currently can't contain them.", 
                        default="patientName=patientName, time=time, caretakerName=caretakerName, report=report")

    def parse_custom_cols(mapping_str):
        return dict(item.strip().split("=") for item in mapping_str.split(","))
    
    args = parser.parse_args()
    args.custom_cols = parse_custom_cols(args.custom_cols)
    print(args.custom_cols)

    main(args.input_file, args.custom_cols)