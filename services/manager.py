import os
import json
import random
import string
import time
import logging
from pathlib import Path
from django.conf import settings
from django.db import transaction
from pyspark.sql.functions import udf, col, explode, array_repeat
from pyspark.sql.types import StringType

# Import je modellen
from api.models import DeidentificationJob
from services.spark_session import get_spark_session

# Importeer deduce (zorg ervoor dat deze geïnstalleerd is)
import deduce
from deduce.person import Person
import subprocess


def process_deidentification(job_id):
    """
    Process a deidentification job

    Args:
        job_id: UUID of the job to process
    """
    try:
        # get the current job from the database
        job = DeidentificationJob.objects.get(pk=job_id)

        # update job status to processing
        job.status = 'processing'
        job.save()

        # create input and output directories
        job_dir = os.path.join(settings.MEDIA_ROOT, str(job.job_id))
        input_dir = os.path.join(job_dir, 'input')
        output_dir = os.path.join(job_dir, 'output')

        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        # Create a symbolic link or copy the input file to the input directory
        input_filename = os.path.basename(job.input_file.name)
        input_filepath = os.path.join(input_dir, input_filename)

        # Prepare the command arguments
        # input_cols = f"patientName={job.patient_column}, time={job.time_column}, caretakerName={job.caretaker_column}, report={job.report_column}"
        output_cols = "patientID=patientID, processed_report=processed_report"

        # Determine file extension for output
        input_ext = os.path.splitext(input_filename)[1].lower()
        output_ext = input_ext if input_ext in ['.csv', '.parquet'] else '.parquet'

        # Build the command to run pyspark_deducer.py
        cmd = [
            'python',
            os.path.join(settings.BASE_DIR, ".venv", "Scripts", "python"),
            os.path.join(settings.BASE_DIR, 'services', 'pyspark_deducer.py'),
            # f'--input_fofi={input_filename}',
            # '--input_cols', input_cols,
            # '--output_cols', output_cols,
            # '--output_extension', output_ext
        ]

        # Configure environment variables for the subprocess
        env = os.environ.copy()
        # env['PYTHONPATH'] = settings.BASE_DIR
        # # Set environment variables to redirect input/output directories
        # env['DATA_INPUT_DIR'] = input_dir
        # env['DATA_OUTPUT_DIR'] = output_dir

        print(f'******************* {job} *******************')
        print(f'  Job status {job.status}')
        print(f'  Job dir: {job_dir}')
        print(f'  INPUT = {input_dir}')
        print(f'  OUTPUT = {output_dir}')
        print('*********************************************')
        print(f'  Input filename = {input_filename}')
        print(f'  Input filepath = {input_filepath}')
        print('*********************************************')
        # print(f'  Input Columns = {input_cols}')
        print(f'  Output Columns = {output_cols}')
        print(' ')
        print(f'  INPUT extension = {input_ext}')
        print(f'  OUTPUT extension = {output_ext}')
        print(' ')
        print(f'COMMAND {cmd}')

        print('*********************************************')
        print('*********************************************')
        print('*********************************************')
        # print(f'ENV = {env}')
        # print(f"{env['PYTHONPATH']}")
        # print(f"{env['DATA_INPUT_DIR']}")
        # print(f"{env['DATA_OUTPUT_DIR']}")

        # Execute the command
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=job_dir
        )

        stdout, stderr = process.communicate()

        # Check if the process was successful
        if process.returncode != 0:
            raise Exception(f"Process failed with code {process.returncode}: {stderr.decode('utf-8')}")




#         # Update status naar processing
#         job.status = 'processing'
#         job.save()

#         # Haal bestandspad op en controleer of het bestaat
#         input_file_path = job.get_input_file_path()
#         if not os.path.exists(input_file_path):
#             raise FileNotFoundError(f"Input file not found: {input_file_path}")

#         # Maak een kopie van het bestand in de data/input directory
#         input_filename = os.path.basename(input_file_path)
#         data_input_path = os.path.join(settings.DATA_INPUT_DIR, input_filename)

#         # Zorg dat de directory bestaat
#         os.makedirs(os.path.dirname(data_input_path), exist_ok=True)

#         # Kopieer het bestand
#         with open(input_file_path, 'rb') as src_file:
#             with open(data_input_path, 'wb') as dst_file:
#                 dst_file.write(src_file.read())

#         # Maak custom_cols dictionary voor het script
#         # custom_cols = {
#         #     "patientName": job.patient_name_column,
#         #     "time": job.time_column,
#         #     "caretakerName": job.caretaker_name_column,
#         #     "report": job.report_column
#         # }

#         custom_cols = {
#             "patientName": "Cliëntnaam",
#             "time": "Tijdstip",
#             "caretakerName": "Zorgverlener",
#             "report": "rapport"
#         }

#         # Voer de-identificatie uit
#         output_path = run_deidentification(input_filename, custom_cols)


#         print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
#         print(job)
#         print(input_file_path)
#         print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')

#         # Update job met output bestand
#         output_filename = f"{os.path.splitext(input_filename)[0]}_processed.csv"
#         output_filepath = os.path.join(
#             settings.DATA_OUTPUT_DIR, output_filename)

#         # Als het outputbestand bestaat, koppel het aan de job
#         if os.path.exists(output_filepath):
#             # In een echte implementatie zou je hier het bestand naar MEDIA_ROOT kopiëren
#             # en job.output_file instellen
#             with transaction.atomic():
#                 job.status = 'completed'
#                 job.save()
#         else:
#             raise FileNotFoundError(
#                 f"Output file was not generated: {output_filepath}")

    except Exception as e:
        logging.error(f"Error processing job {job_id}: {str(e)}")
        # Update job met foutmelding
        try:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except Exception as inner_e:
            logging.error(f"Failed to update job status: {str(inner_e)}")


# def pseudonymize(unique_names):
#     """
#     Purpose:
#         Turn a list of unique names into a dictionary, mapping each to a randomized string.
#     Parameters:
#         unique_names (list): list of unique strings.
#     Returns:
#         Dictionary: keys are the original names, values are the randomized strings.
#     """
#     name_map = {}
#     for name in unique_names:
#         found_new = False
#         i = 0
#         while (not found_new and i < 10):
#             i += 1
#             randomization = "".join(random.choices(
#                 string.ascii_uppercase+string.digits, k=12))
#             if randomization not in name_map:
#                 found_new = True
#                 name_map[name] = randomization
#     assert len(unique_names) == len(
#         name_map), "Pseudonymization function safeguard: unique_names and name_map don't have same length"
#     return name_map


# def run_deidentification(input_filename, custom_cols):
#     """
#     Run the deidentification process using PySpark and Deduce.

#     Args:
#         input_filename: Filename in the DATA_INPUT_DIR to process
#         custom_cols: Dictionary mapping the required column names

#     Returns:
#         Path to the output file
#     """
#     # Krijg een Spark sessie
#     print('SPARK')
#     spark = get_spark_session()
#     n_threads = 3

#     print(spark)

#     try:
#         # Initialiseer deduce

#         # broadcasting this class instance does not work (serialization issue)
#         print("Deduce will now be initialized, which unfortunately has to be redone (for now) when running inside container.")
#         deducer = deduce.Deduce()

#         # Definieer UDFs voor PySpark
#         @udf(StringType())
#         def deduce_with_id(report, patient):
#             """
#             Purpose:
#                 Apply the Deduce algorithm
#             Parameters:
#                 report (string): Text entry to apply on.
#                 patient (string): Patient name to enhance the algorithm performance
#             Returns:
#                 report_deid (string): Deidentified version of report input
#             """
#             try:
#                 patient_name = str.split(patient, " ", maxsplit=1)
#                 patient_initials = "".join([x[0] for x in patient_name])

#                 # Difficult to predict person metadata exactly. I.e. in case of multiple spaces does this indicate multitude of first names or a composed last name?
#                 # Best will be if input data contains columns for first names, surname, initials
#                 # patient = Person(first_names = [patient_name[0:-1]], surname = patient_name[-1], initials = patient_initials)
#                 patient_obj = Person(first_names=[patient_name[0]], surname=patient_name[1], initials=patient_initials)
#                 report_deid = deducer.deidentify(report, metadata={'patient': patient_obj})
#                 report_deid = getattr(report_deid, "deidentified_text")
#                 return report_deid
#             except Exception as e:
#                 logging.error(f"Error in deduce_with_id: {str(e)}")
#                 return report  # Return original as fallback

#         @udf(StringType())
#         def map_dictionary(name):
#             """
#             Purpose:
#                 Obtain randomized string corresponding to name
#             Parameters:
#                 name (string): person name
#                 name_map_bc (dict, global environment): dictionary of person names (keys, string) and randomized IDs (values, string)
#             Returns:
#                 Value from name_map_bc corresponding to input name
#             """
#             return name_map_bc.value.get(name)

#         @udf(StringType())
#         def replace_patient_tag(text, new_value, tag="[PATIENT]"):
#             """
#             Purpose:
#                 Deduce labels occurences of patient name in text with a [PATIENT] tag. This function replaces that with the randomized ID.
#             Parameters:
#                 text (string): the text possibly containing tag to replace
#                 new_value (string): text to replace tag with
#                 tag (string, default = "[PATIENT]): Formatted string indicating tag to replace
#             Returns:
#                 text from text but with tag replaced by new_value
#             """
#             return str.replace(text, "[PATIENT]", new_value)










#         # Lees het invoerbestand
#         input_path = os.path.join(settings.DATA_INPUT_DIR, input_filename)
#         psdf = spark.read.options(
#             header=True, delimiter=",", multiline=True).csv(input_path)












#         # If you want to amplify data, usually for performance testing purposes
#         n_concat = 1
#         psdf = psdf.withColumn(custom_cols["patientName"], explode(array_repeat(custom_cols["patientName"], n_concat)))
#         psdf.printSchema()
#         psdf_rowcount = psdf.count()
#         print("Row count: " + str(psdf_rowcount))

#         # An informed decision can be made with the data size and system properties
#         # - more partitions than cores to improve load balancing/skew especially when entries need to be shuffeled (e.g. due to group_by)
#         # - partitions should fit in memory on nodes (recommended is around 256MB)
#         # Amplify for testing purposes
#         n_partitions = psdf_rowcount // 3000 + 1
#         psdf = psdf.repartition(n_partitions)

#         print("count: " + str(psdf_rowcount))
#         print("partitions: " + str(psdf.rdd.getNumPartitions()))

#         name_map = pseudonymize(psdf.select(custom_cols["patientName"]).distinct().toPandas()[custom_cols["patientName"]])
#         name_map_bc = spark.sparkContext.broadcast(name_map)
#         with open(("data/output/" + "name_map.json"), "w") as outfile:
#             json.dump(name_map, outfile)
#         outfile.close()

#         start_t = time.time()
#         # Define transformations, trigger execution by writing output to disk
#         psdf = psdf.withColumn("patientID", map_dictionary(custom_cols["patientName"]))\
#             .withColumn(custom_cols["report"], deduce_with_id(custom_cols["report"], custom_cols["patientName"]))\
#             .withColumn(custom_cols["report"], replace_patient_tag(custom_cols["report"], "patientID"))\
#             .select("patientID", custom_cols["time"], custom_cols["caretakerName"], custom_cols["report"])

#         pandas_df = psdf.toPandas()
#         output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "output")
#         os.makedirs(output_dir, exist_ok=True)

#         # Schrijf output naar bestand
#         output_filename = f"{os.path.splitext(input_filename)[0]}_processed.csv"
#         output_path = os.path.join(settings.DATA_OUTPUT_DIR, output_filename)


#         # Bouw het outputpad op
#         # base_filename = os.path.splitext(os.path.basename(input_file))[0]
#         # output_file = os.path.join(output_dir, base_filename + "_processed.csv")

#         # Schrijf output naar bestand
#         # output_filename = f"{os.path.splitext(input_filename)[0]}_processed.csv"
#         output_path = os.path.join(settings.DATA_OUTPUT_DIR, output_filename)



#         # pandas_df.to_csv(output_file, index=False)
#         pandas_df.to_csv(output_path, index=False)


#         psdf_row_count = psdf.count()
#         end_t = time.time()  # timeit.timeit()
#         print(
#             f"time passed with n_threads = {n_threads} and row count = {psdf_rowcount}: {end_t - start_t}s ({(end_t - start_t) / psdf_rowcount}s / row)")
#         psdf.printSchema()
#         print(psdf.take(10))

#         return output_path








#     finally:
#         # Stop de Spark sessie
#         print('!!!CLOSING SPARK!!!')
#         spark.stop()
