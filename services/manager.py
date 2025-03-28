from api.models import DeidentificationJob
from services.pyspark_deducer import process_data
from django.conf import settings
from services.logger import setup_logging

from pathlib import Path
import threading
import os
import queue


logger, logger_py4j = setup_logging()


def thread_target(
        input_fofi, input_cols, output_cols, pseudonym_key, max_n_processes,
        output_extension, partition_n, coalesce_n, job, output_queue=None):
    """
    Execute a deidentification job in a separate thread.

    Processes data, manages job status, and handles potential errors
    during the deidentification workflow.

    Handles threading, job status updates, and result communication
    for asynchronous data processing.
    """
    try:
        # call the main deidentification process
        processed_rows_json = process_data(
            input_fofi=input_fofi,
            input_cols=input_cols,
            output_cols=output_cols,
            pseudonym_key=pseudonym_key,
            max_n_processes=max_n_processes,
            output_extension=output_extension,
            partition_n=partition_n,
            coalesce_n=coalesce_n
        )

        # if an output queue is provided, put the JSON data in the queue
        if output_queue is not None:
            output_queue.put(processed_rows_json)

        # if no errors, update job to 'completed'
        job.status = 'completed'
        job.save()

    except Exception as e:
        # update job status
        logger.error(f'Error in thread for job {job.id}: {str(e)}')

        # if an output queue is provided, put the error in the queue
        if output_queue is not None:
            output_queue.put(None)

        try:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except Exception as inner_e:
            logger.error(f'Failed to update job status for job {job.id}: {str(inner_e)}')
            pass


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

        # prepare the command arguments
        input_cols = 'patientName=Cliëntnaam, time=Tijdstip, caretakerName=Zorgverlener, report=rapport'
        output_cols = 'patientID=patientID, processed_report=processed_report'

        input_fofi = Path(job.input_file.name).name
        input_extension = os.path.splitext(input_fofi)[1].lower()
        output_extension = input_extension if input_extension in ['.csv', '.parquet'] else '.parquet'

        # create a queue to receive the processed rows JSON
        output_queue = queue.Queue()

        # create a thread to run the main deidentification process
        thread = threading.Thread(
            target=thread_target,
            args=(
                input_fofi,
                input_cols,
                output_cols,
                None,  # pseudonym_key
                2,  # max_n_processes
                output_extension,
                None,  # partition_n
                None,  # coalesce_n
                job,
                output_queue
            )
        )
        thread.daemon = True
        thread.start()

        # wait until thread is finished
        thread.join()

        # retrieve 10 first rows in JSON
        try:
            processed_rows_json = output_queue.get(block=False)

            # Optionally, save the processed rows to the job model
            if processed_rows_json:
                job.processed_preview = processed_rows_json
                job.save()
        except queue.Empty:
            logger.warning(f'No processed rows JSON retrieved for job {job_id}')

        output_filepath = os.path.join(
            settings.DATA_OUTPUT_DIR, os.path.splitext(os.path.basename(input_fofi))[0] + '_processed', input_fofi)

        # check if output is ready and connect to job
        if os.path.exists(output_filepath):
            job.output_file = output_filepath
            job.key_file = os.path.join(settings.DATA_OUTPUT_DIR, 'pseudonym_key.json')
            job.log_file = os.path.join(settings.DATA_OUTPUT_DIR, 'main.log')
            job.status = 'completed'
            job.save()
        else:
            raise FileNotFoundError('Output files are not properly generated')

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")

        try:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except Exception as inner_e:
            logger.error(f'Failed to update job status: {str(inner_e)}')
