from services.polars_deduce import process_data
from services.logger import setup_logging
from api.models import DeidentificationJob
from django.conf import settings

from pathlib import Path
import threading
import os
import queue
import json
import zipfile


logger = setup_logging()


def thread_target(input_fofi, input_cols, output_cols, pseudonym_key, output_extension, job, output_queue=None):

    """
    Execute a deidentification job in a separate thread.

    This function serves as the target for a thread that processes data asynchronously.
    It orchestrates the deidentification workflow, manages job status updates, and
    handles communication of results through a queue mechanism when provided.

    Returns:
        None. Results are communicated through side effects:
        - The job status is updated in the database
        - If output_queue is provided, processed data is added to the queue
        - Error information is logged and stored in the job record if exceptions occur
    """
    try:
        # call the main deidentification process
        processed_rows_json = process_data(
            input_fofi=input_fofi,
            input_cols=input_cols,
            output_cols=output_cols,
            pseudonym_key=pseudonym_key,
            output_extension=output_extension,
        )

        # if an output queue is provided, put the JSON data in the queue
        if output_queue is not None:
            output_queue.put(processed_rows_json)

        # if no errors, update job to 'completed'
        job.status = 'completed'
        job.save()

    except Exception as e:
        # update job status
        logger.error(f'Error in thread for job {job.pk}: {str(e)}')

        # if an output queue is provided, put the error in the queue
        if output_queue is not None:
            output_queue.put(None)
        try:
            job.status = 'failed'
            job.error_message = f'Job error: {str(e)}'
            job.save()
        except Exception as inner_e:
            logger.error(f'Failed to update job status for job {job.id}: {str(inner_e)}')
            pass


def transform_output_cols(input_cols):
    """
    This function takes a comma-separated string of input column mappings
    and replaces specific key mappings with their corresponding output mappings:

    Args:
        input_cols (str): A comma-separated string of key-value pairs in the
                         format 'key1=value1, key2=value2, ...'

    Returns:
        str: A comma-separated string with transformed mappings, maintaining
             the same format as the input but with the replaced values.

    Example:
        transform_output_cols('patientName=name, report=text, other=value')
        'patientName=patientID, report=processed_report, other=value'
    """
    parts = [part.strip() for part in input_cols.split(',')]

    for col_name, part in enumerate(parts):
        if part.startswith('patientName='):
            parts[col_name] = 'patientName=patientID'
        elif part.startswith('report='):
            parts[col_name] = 'report=processed_report'

    return ', '.join(parts)


def create_zip_file(job_id, file_paths, output_filename):
    """
    Create a zip file containing the output files

    Args:
        job_id: UUID of the job
        file_paths: List of file paths to include in the zip
        output_filename: Name of the output file

    Returns:
        Tuple of (path to the created zip file, list of filenames included in the zip)
    """
    base_name = os.path.splitext(output_filename)[0]
    zip_filename = f'{base_name}_deidentified.zip'

    # define zip file path with output file name
    zip_path = os.path.join(settings.MEDIA_ROOT, 'output', zip_filename)

    included_files = []

    # create the zip file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in file_paths:
            if os.path.exists(file_path):
                basename = os.path.basename(file_path)

                # add file to zip with the determined name
                zipf.write(file_path, basename)

                # add to our list of included files
                included_files.append(basename)
            else:
                logger.warning(f'File not found for zipping: {file_path}')

    return zip_path, zip_filename, included_files


def process_deidentification(job_id):
    """
    This function orchestrates the entire workflow for a job, including:
    - Loading the job from the database
    - Setting up input/output column mappings
    - Running the deidentification process in a separate thread
    - Collecting and saving processed data previews
    - Managing output files (data, keys, logs)
    - Creating a zip archive of all output files
    - Handling job status updates and error conditions

    Args:
        job_id (UUID): Unique identifier of the job to process.

    Returns:
        None. Results are persisted through side effects:
        - Job status and metadata updated in the database
        - Output files created in the filesystem
        - Errors logged to the application logger

    Raises:
        No exceptions are propagated as they are caught and handled internally.
        Errors are logged and reflected in the job's status and error_message fields.

    Note:
        This function operates asynchronously by spawning a separate thread for
        the actual deidentification work, but it blocks until that thread completes.
    """
    try:
        # get the current job from the database
        job = DeidentificationJob.objects.get(pk=job_id)

        # update job status to processing
        job.status = 'processing'
        job.save()

        # output has the same amount of cols as input
        # but some cols need to be replaced with the psuedomized versions
        input_cols = job.input_cols
        output_cols = transform_output_cols(input_cols)

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
                output_extension,
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

            # optionally, save the first 10 processed rows to the job model
            if processed_rows_json:
                processed_data = json.loads(processed_rows_json)
                job.processed_preview = processed_data
                job.save()
        except queue.Empty:
            logger.warning(f'No processed rows JSON retrieved for job {job_id}')

        # set the output directory and filename
        DATA_OUTPUT_DIR = os.path.join(settings.MEDIA_ROOT, 'output')

        # define paths for all output files
        output_path = os.path.join(DATA_OUTPUT_DIR, input_fofi)
        key_path = os.path.join(DATA_OUTPUT_DIR, 'pseudonym_key.json')
        log_path = os.path.join(DATA_OUTPUT_DIR, 'deidentification.log')

        files_to_zip = []

        # output file
        if os.path.exists(output_path):
            relative_path = os.path.relpath(output_path, settings.MEDIA_ROOT)
            job.output_file.name = relative_path
            files_to_zip.append(output_path)
            output_filename = os.path.basename(output_path)

        # key file
        if os.path.exists(key_path):
            relative_path = os.path.relpath(key_path, settings.MEDIA_ROOT)
            job.key_file.name = relative_path
            files_to_zip.append(key_path)

        # log file
        if os.path.exists(log_path):
            relative_path = os.path.relpath(log_path, settings.MEDIA_ROOT)
            job.log_file.name = relative_path
            files_to_zip.append(log_path)

        # create zip file with all available output files
        if files_to_zip:
            try:
                zip_path, zip_filename, included_files = create_zip_file(job_id, files_to_zip, output_filename)
                relative_zip_path = os.path.relpath(zip_path, settings.MEDIA_ROOT)

                # store the zip file path in the job model
                job.zip_file.name = relative_zip_path

                # store the list of included files in the job model
                job.zip_preview = {
                    'zip_file': zip_filename,
                    'files': included_files
                }
            except Exception as e:
                logger.error(f'Failed to create zip file: {str(e)}')
        else:
            logger.warning(f'No output files found to zip for job {job_id}')

        # update job status
        job.status = 'completed'
        job.save()
    except Exception as e:
        logger.error(f'Error processing job {job_id}: {str(e)}')
        try:
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except Exception as inner_e:
            logger.error(f'Failed to update job status: {str(inner_e)}')
