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
from deduce_service.spark_session import get_spark_session

# Importeer deduce (zorg ervoor dat deze geïnstalleerd is)
import deduce
from deduce.person import Person


def process_deidentification(job_id):
    """
    Voert de de-identificatie uit voor een specifieke job.
    Deze functie wordt in een aparte thread uitgevoerd.
    """
    # Haal de job op uit de database
    try:
        job = DeidentificationJob.objects.get(id=job_id)

        # Update status naar processing
        job.status = 'processing'
        job.save()

        # Haal bestandspad op en controleer of het bestaat
        input_file_path = job.get_input_file_path()
        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Input file not found: {input_file_path}")

        # Maak een kopie van het bestand in de data/input directory
        input_filename = os.path.basename(input_file_path)
        data_input_path = os.path.join(settings.DATA_INPUT_DIR, input_filename)

        # Zorg dat de directory bestaat
        os.makedirs(os.path.dirname(data_input_path), exist_ok=True)

        # Kopieer het bestand
        with open(input_file_path, 'rb') as src_file:
            with open(data_input_path, 'wb') as dst_file:
                dst_file.write(src_file.read())

        # Maak custom_cols dictionary voor het script
        custom_cols = {
            "patientName": job.patient_name_column,
            "time": job.time_column,
            "caretakerName": job.caretaker_name_column,
            "report": job.report_column
        }

        # Voer de-identificatie uit
        output_path = run_deidentification(input_filename, custom_cols)


        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        print(job)
        print(input_file_path)
        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')

        # Update job met output bestand
        output_filename = f"{os.path.splitext(input_filename)[0]}_processed.csv"
        output_filepath = os.path.join(
            settings.DATA_OUTPUT_DIR, output_filename)

        # Als het outputbestand bestaat, koppel het aan de job
        if os.path.exists(output_filepath):
            # In een echte implementatie zou je hier het bestand naar MEDIA_ROOT kopiëren
            # en job.output_file instellen
            with transaction.atomic():
                job.status = 'completed'
                job.save()
        else:
            raise FileNotFoundError(
                f"Output file was not generated: {output_filepath}")

    except Exception as e:
        logging.error(f"Error processing job {job_id}: {str(e)}")
        # Update job met foutmelding
        try:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except Exception as inner_e:
            logging.error(f"Failed to update job status: {str(inner_e)}")


def pseudonymize(unique_names):
    """
    Purpose:
        Turn a list of unique names into a dictionary, mapping each to a randomized string.
    Parameters:
        unique_names (list): list of unique strings.
    Returns:
        Dictionary: keys are the original names, values are the randomized strings.
    """
    name_map = {}
    for name in unique_names:
        found_new = False
        i = 0
        while (not found_new and i < 10):
            i += 1
            randomization = "".join(random.choices(
                string.ascii_uppercase+string.digits, k=12))
            if randomization not in name_map:
                found_new = True
                name_map[name] = randomization
    assert len(unique_names) == len(
        name_map), "Pseudonymization function safeguard: unique_names and name_map don't have same length"
    return name_map


def run_deidentification(input_filename, custom_cols):
    """
    Run the deidentification process using PySpark and Deduce.

    Args:
        input_filename: Filename in the DATA_INPUT_DIR to process
        custom_cols: Dictionary mapping the required column names

    Returns:
        Path to the output file
    """
    # Krijg een Spark sessie
    print('SPARK')
    spark = get_spark_session()

    print(spark)

    try:
        # Initialiseer deduce
        deducer = deduce.Deduce()

        # Definieer UDFs voor PySpark
        @udf(StringType())
        def deduce_with_id(report, patient):
            """Apply the Deduce algorithm"""
            try:
                patient_name = str.split(patient, " ", maxsplit=1)
                patient_initials = "".join([x[0] for x in patient_name])

                # Maak Person object voor Deduce
                if len(patient_name) > 1:
                    patient_obj = Person(first_names=[
                                         patient_name[0]], surname=patient_name[1], initials=patient_initials)
                else:
                    patient_obj = Person(
                        first_names=[], surname=patient_name[0], initials=patient_initials)

                report_deid = deducer.deidentify(
                    report, metadata={'patient': patient_obj})
                report_deid = getattr(report_deid, "deidentified_text")
                return report_deid
            except Exception as e:
                logging.error(f"Error in deduce_with_id: {str(e)}")
                return report  # Return original as fallback

        @udf(StringType())
        def map_dictionary(name):
            """Lookup name in broadcast dictionary"""
            return name_map_bc.value.get(name)

        @udf(StringType())
        def replace_patient_tag(text, new_value, tag="[PATIENT]"):
            """Replace [PATIENT] tags with pseudonymized IDs"""
            return str.replace(text, tag, new_value)

        # Lees het invoerbestand
        input_path = os.path.join(settings.DATA_INPUT_DIR, input_filename)
        psdf = spark.read.options(
            header=True, delimiter=",", multiline=True).csv(input_path)

        # Controleer of de kolommen bestaan
        for col_name in custom_cols.values():
            if col_name not in psdf.columns:
                raise ValueError(
                    f"Column '{col_name}' not found in the input file")

        # Consistente partitionering op basis van gegevensomvang
        psdf_rowcount = psdf.count()
        n_partitions = max(1, psdf_rowcount // 3000 + 1)
        psdf = psdf.repartition(n_partitions)

        # Maak name_map en broadcast
        name_map = pseudonymize(psdf.select(custom_cols["patientName"]).distinct(
        ).toPandas()[custom_cols["patientName"]].tolist())
        name_map_bc = spark.sparkContext.broadcast(name_map)

        # Sla name_map op (voor eventuele latere tracering)
        with open(os.path.join(settings.DATA_OUTPUT_DIR, "name_map.json"), "w") as outfile:
            json.dump(name_map, outfile)

        # Transformations
        start_t = time.time()
        psdf = psdf.withColumn("patientID", map_dictionary(col(custom_cols["patientName"]))) \
            .withColumn(custom_cols["report"], deduce_with_id(col(custom_cols["report"]), col(custom_cols["patientName"]))) \
            .withColumn(custom_cols["report"], replace_patient_tag(col(custom_cols["report"]), col("patientID"))) \
            .select("patientID", col(custom_cols["time"]), col(custom_cols["caretakerName"]), col(custom_cols["report"]))

        # Schrijf output naar bestand
        output_filename = f"{os.path.splitext(input_filename)[0]}_processed.csv"
        output_path = os.path.join(settings.DATA_OUTPUT_DIR, output_filename)

        psdf.write.mode("overwrite").csv(output_path)

        end_t = time.time()
        logging.info(
            f"Processing completed in {end_t - start_t:.2f}s ({(end_t - start_t) / psdf_rowcount:.4f}s per row)")

        return output_path

    finally:
        # Stop de Spark sessie
        spark.stop()
