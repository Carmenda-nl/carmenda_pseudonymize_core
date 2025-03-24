# Imports
import argparse
import os
import json
import pandas as pd
import numpy as np
import random
import string
import time
import warnings
import logging
import multiprocessing as mp
from functools import partial
from tqdm import tqdm

# deduce
import deduce
from deduce.person import Person


def pseudonymize(unique_names, pseudonym_key=None, droplist=[]):
    """
    Purpose:
        Turn a list of unique names into a dictionary, mapping each to a randomized string. Uniqueness of input is
        responsibility of caller, but if the generated output is not of the same length (can also occur if not enough unique strings were generated) it raises an assertionError.
    Parameters:
        unique_names (list): list of unique strings.
    Returns:
        Dictionary: keys are the original names, values are the randomized strings.
    """
    unique_names = [name for name in unique_names if name not in droplist]
    if pseudonym_key == None:
        logger_main.info(
            "Building new pseudonym_key because argument was None")
        pseudonym_key = {}
    else:
        logger_main.info("Building from existing pseudonym_key")
    for name in unique_names:
        if name not in pseudonym_key:
            found_new = False
            i = 0
            while (found_new == False and i < 15):
                i += 1
                pseudonym_candidate = "".join(random.choices(string.ascii_uppercase +
                                              string.ascii_lowercase+string.digits, k=14))
                if pseudonym_candidate not in pseudonym_key.values():
                    found_new == True
                    pseudonym_key[name] = pseudonym_candidate
    assert len(unique_names) == len(pseudonym_key.items(
    )), "Pseudonymization function safeguard: unique_names (input) and pseudonym_key (output) don't have same length"
    pseudonym_key[None] = None
    return pseudonym_key


def deduce_with_id(row, input_cols, pseudonym_key):
    """
    Purpose:
        Apply the Deduce algorithm and replace patient tag with ID
    Parameters:
        row: DataFrame row
        input_cols: Dictionary of column names
        pseudonym_key: Dictionary mapping names to IDs
    Returns:
        Tuple with processed report and patient ID
    """
    try:
        # Get patient information
        patient_name = str.split(
            row[input_cols["patientName"]], " ", maxsplit=1)
        if len(patient_name) < 2:
            # Add empty surname if only one name provided
            patient_name.append("")
        patient_initials = "".join([x[0] for x in patient_name if x])

        # Setup deduce processor
        deduce_instance = deduce.Deduce()
        deduce_patient = Person(
            first_names=[patient_name[0]], surname=patient_name[1], initials=patient_initials)

        # De-identify the text
        report_deid = deduce_instance.deidentify(
            row[input_cols["report"]], metadata={'patient': deduce_patient})
        report_deid = getattr(report_deid, "deidentified_text")

        # Get patient ID
        patient_id = pseudonym_key.get(row[input_cols["patientName"]])

        # Replace [PATIENT] tag with patient ID
        report_final = str.replace(
            report_deid, "[PATIENT]", patient_id) if patient_id else report_deid

        return report_final, patient_id
    except Exception as e:
        logger_main.error(f"Error processing row: {e}")
        return None, None


def process_chunk(chunk, input_cols, output_cols, pseudonym_key):
    """
    Process a chunk of data in parallel
    """
    results = []
    for _, row in chunk.iterrows():
        processed_report, patient_id = deduce_with_id(
            row, input_cols, pseudonym_key)
        if processed_report is not None:
            # Create result row with selected columns
            result_row = {}
            if "patientID" in output_cols.values():
                result_row["patientID"] = patient_id
            if "processed_report" in output_cols.values():
                result_row["processed_report"] = processed_report
            # Add any other columns that should be passed through
            for out_key, out_col in output_cols.items():
                if out_col not in ["patientID", "processed_report"] and out_col in chunk.columns:
                    result_row[out_col] = row[out_col]
            results.append(result_row)
    return pd.DataFrame(results)


def main(input_fofi, input_cols, output_cols, pseudonym_key, max_n_processes, output_extension):
    # Start timer for the entire processing
    total_start_time = time.time()
    
    # On larger systems leave a CPU
    n_processes = min(max_n_processes, max(1, (os.cpu_count() - 1)))
    logger_main.info(f"Using {n_processes} processes")

    # Load data
    input_ext = os.path.splitext("data/input/" + input_fofi)[-1]
    if input_ext == ".csv":
        input_fofi_size = os.stat("./data/input/" + input_fofi).st_size
        logger_main.info("CSV input file of size: " + str(input_fofi_size))
        df = pd.read_csv("data/input/" + input_fofi)
    else:
        # For parquet files
        df = pd.read_parquet("data/input/" + input_fofi)

    # For debugging
    # df = df.head(100)
    df_rowcount = len(df)
    logger_main.info("Row count: " + str(df_rowcount))

    # Load or create pseudonym dictionary
    if pseudonym_key:
        with open("data/input/" + pseudonym_key, 'r') as f:
            pseudonym_key_dict = json.load(f)
    else:
        pseudonym_key_dict = None

    # Generate pseudonyms for unique patient names
    unique_names = df[input_cols["patientName"]].unique().tolist()
    pseudonym_key_dict = pseudonymize(unique_names, pseudonym_key_dict)

    # Save pseudonym key
    with open("data/output/pseudonym_key.json", "w") as outfile:
        json.dump(pseudonym_key_dict, outfile)

    # Timer for parallel processing part
    processing_start_time = time.time()

    # Split dataframe into chunks for parallel processing
    # 10 chunks per process for better load balancing
    chunk_size = max(1, df_rowcount // (n_processes * 10))
    chunks = [df[i:i + chunk_size] for i in range(0, df_rowcount, chunk_size)]
    logger_main.info(
        f"Processing {len(chunks)} chunks with chunk size {chunk_size}")

    # Process chunks in parallel
    process_func = partial(process_chunk, input_cols=input_cols,
                           output_cols=output_cols, pseudonym_key=pseudonym_key_dict)

    # Progress reporting with tqdm
    results = []
    with mp.Pool(processes=n_processes) as pool:
        for result in tqdm(pool.imap_unordered(process_func, chunks), total=len(chunks)):
            results.append(result)

    # Combine results
    result_df = pd.concat(results, ignore_index=True)

    # Count processed rows
    processed_rows = len(result_df)
    logger_main.info(
        f"Processed {processed_rows} rows out of {df_rowcount} input rows")

    # Processing time calculation
    processing_end_time = time.time()
    processing_time = processing_end_time - processing_start_time
    logger_main.info(
        f"Processing time with n_processes = {n_processes} and row count (start, end) = ({df_rowcount}, {processed_rows}): {processing_time:.2f}s ({processing_time / df_rowcount:.6f}s / row)")

    # Handle output
    output_base = "data/output/" + \
        os.path.splitext(os.path.basename(input_fofi))[0] + "_processed"

    if output_extension == ".csv":
        result_df.to_csv(output_base + ".csv", index=False)
    elif output_extension == ".txt":
        # Convert each row to a single line of text
        with open(output_base + ".txt", 'w') as f:
            for _, row in result_df.iterrows():
                f.write(', '.join(str(v) for v in row.values) + '\n')
    else:
        # Default to parquet
        if output_extension != ".parquet":
            warnings.warn(
                "Selected output extension not supported, using parquet.")
        result_df.to_parquet(output_base + ".parquet", index=False)

    # Total time calculation
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    
    logger_main.info(f"Total script execution time: {total_time:.2f} seconds")
    logger_main.info(f"Time breakdown: Processing: {processing_time:.2f}s ({processing_time/total_time*100:.1f}%), Other operations: {total_time-processing_time:.2f}s ({(total_time-processing_time)/total_time*100:.1f}%)")
    
    print(f"\nTotal script execution time: {total_time:.2f} seconds")
    print(f"Processing time: {processing_time:.2f} seconds ({processing_time/total_time*100:.1f}%)")
    print(f"Other operations time: {total_time-processing_time:.2f} seconds ({(total_time-processing_time)/total_time*100:.1f}%)")

    # Display sample of processed data
    print("\nSample of processed data:")
    print(result_df.head(10))


if __name__ == "__main__":
    # Start timer for the entire script
    script_start_time = time.time()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logger_main = logging.getLogger(__name__)
    logger_main.setLevel(logging.INFO)

    # Create output directory if it doesn't exist
    os.makedirs("data/output", exist_ok=True)

    handler_main = logging.FileHandler("data/output/logger_main.log")
    handler_main.setFormatter(formatter)
    logger_main.addHandler(handler_main)

    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger_main.addHandler(console_handler)
    
    logger_main.info("Script started")

    parser = argparse.ArgumentParser(description="Input parameters for the python programme.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input_fofi",
                        nargs="?",
                        default="dummy_input.csv",
                        help="Name of the input folder or file. Currently csv file and parquet file or folder are supported.")
    parser.add_argument("--input_cols",
                        nargs="?",
                        help="Input column names as a single string with comma separated key=value format e.g. \"patientName=foo, report=bar, ...=foobar\". Whitespaces are removed, so column names currently can't contain them. Keys patientName and report with values existing in the data are essential.",
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
                        default=2,
                        help="Maximum number of processes. Default behavior is to detect the number of cores on the system and subtract 1.")
    parser.add_argument("--output_extension",
                        nargs="?",
                        default=".parquet",
                        help="Select output format, currently only parquet (default), csv and txt are supported.")

    def parse_dict_cols(mapping_str):
        return dict(item.strip().split("=") for item in mapping_str.split(","))

    args = parser.parse_args()
    args.input_cols = parse_dict_cols(args.input_cols)
    args.output_cols = parse_dict_cols(args.output_cols)
    args.max_n_processes = int(args.max_n_processes)
    logger_main.info(args)

    main(args.input_fofi, args.input_cols, args.output_cols, args.pseudonym_key,
         args.max_n_processes, args.output_extension)
    
    # Calculate and display total script execution time
    script_end_time = time.time()
    script_total_time = script_end_time - script_start_time
    logger_main.info(f"Total script execution time (including initialization): {script_total_time:.2f} seconds")
    print(f"\nTotal script execution time (including initialization): {script_total_time:.2f} seconds")