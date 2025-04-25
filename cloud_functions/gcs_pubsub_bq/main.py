import base64
import json
from google.cloud import bigquery
import logging
import google.cloud.logging
import os
# REMOVED: from flask import Flask, request - No longer needed
from google.cloud import storage

# --- Configuration ---
# Load configuration from environment variables
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID')
TABLE_ID = os.environ.get('TABLE_ID')
BIGQUERY_LOCATION = os.environ.get('BIGQUERY_LOCATION', "EU")
ARCHIVE_BUCKET_NAME = os.environ.get('ARCHIVE_BUCKET_NAME')
# STAGING_BUCKET_NAME seems unused in the logic, but keep if needed elsewhere
STAGING_BUCKET_NAME = os.environ.get('STAGING_BUCKET_NAME')
LOG_NAME = "bigquery-loader-function" # Cloud Logging log name

# --- Logging Setup ---
def setup_logging():
    """Sets up Cloud Logging and returns a logger. Handles errors."""
    try:
        client = google.cloud.logging.Client(project=PROJECT_ID)
        # Configures the root logger to send logs to Cloud Logging.
        # logger = client.logger(LOG_NAME) # Deprecated way, use setup_logging
        handler = google.cloud.logging.handlers.CloudLoggingHandler(client, name=LOG_NAME)
        google.cloud.logging.handlers.setup_logging(handler, log_level=logging.INFO)
        print(f"Successfully initialized Cloud Logging with handler for {LOG_NAME}.")
        # Return the standard Python logger, now configured for Cloud Logging
        return logging.getLogger(__name__)
    except Exception as e:
        print(f"Error initializing Cloud Logging: {e}. Falling back to standard logging.")
        logging.basicConfig(level=logging.INFO) # Fallback basic config
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to set up Cloud Logging: {e}")
        return logger # Return the standard logger

# Initialize logging globally
logger = setup_logging()


# --- BigQuery Helper Functions ---
def check_dataset_exists(client, project_id, dataset_id):
    """Checks if a BigQuery dataset exists."""
    dataset_ref = client.dataset(dataset_id, project=project_id)
    try:
        client.get_dataset(dataset_ref)
        log_message = f"Dataset '{dataset_id}' in project '{project_id}' already exists."
        logger.info(log_message)
        # print(log_message) # Optional: Keep for local testing if desired
        return True
    except Exception as e:
        if "Not found" in str(e):
            log_message = f"Dataset '{dataset_id}' in project '{project_id}' does not exist."
            logger.info(log_message)
            # print(log_message)
            return False
        else:
            log_message = f"Error checking dataset '{dataset_id}': {e}"
            logger.error(log_message)
            raise e

def create_bigquery_dataset(client, project_id, dataset_id, location="EU"):
    """Creates a BigQuery dataset."""
    try:
        log_message = f"Creating dataset '{dataset_id}' in project '{project_id}' at location '{location}'."
        logger.info(log_message)
        # print(log_message)
        dataset_ref = client.dataset(dataset_id, project=project_id) # Include project_id
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset, exists_ok=True) # Use exists_ok=True
        log_message = f"Dataset '{dataset.dataset_id}' ensured in project '{project_id}' with location '{dataset.location}'."
        logger.info(log_message)
        # print(log_message)
        return True
    except Exception as e:
        error_message = f"An error occurred while creating the dataset '{dataset_id}': {e}"
        logger.error(error_message)
        raise e


# --- Main Cloud Function Logic ---
# Entry point for the Cloud Function (defined during deployment)
def load_json_from_gcs_to_bigquery(event, context):
    """
    Cloud Function triggered by Pub/Sub to load JSON data from GCS to BigQuery.
    Handles newline-delimited JSON files (.txt or .jsonl) and moves the file after successful load.

    Args:
        event (dict): The dictionary with data specific to this type of event.
                      For Pub/Sub, this contains:
                      {'data': base64encoded message, 'attributes': {}}
        context (google.cloud.functions.Context): Metadata about the event.
                      Provides event_id, timestamp, event_type, resource etc.
    """
    global logger # Ensure we're using the globally configured logger

    try:
        # 1. Decode the Pub/Sub message data directly from the event dictionary
        if 'data' in event:
            file_info_encoded = event['data']
            file_info = base64.b64decode(file_info_encoded).decode('utf-8')
            data = json.loads(file_info) # Assuming the message *is* JSON defining the file
            bucket_name = data.get('bucket')
            file_name = data.get('name')

            # Validate required fields from the message
            if not bucket_name or not file_name:
                raise ValueError("Decoded Pub/Sub message data is missing 'bucket' or 'name' keys.")
        else:
            # This was the original error condition
            raise ValueError("Pub/Sub event dictionary is missing the 'data' field.")

        # Log context information
        logger.info(f"Processing event ID: {context.event_id} for file: {file_name} in bucket: {bucket_name}")

        # --- Initialize Clients ---
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            storage_client = storage.Client(project=PROJECT_ID)
            logger.info(f"Initialized BigQuery and Storage clients for project {PROJECT_ID}.")
        except Exception as e:
            logger.error(f"Failed to initialize Google Cloud clients: {e}")
            raise  # Cannot proceed without clients

        # --- Ensure BigQuery Dataset Exists ---
        try:
            if not check_dataset_exists(bq_client, PROJECT_ID, DATASET_ID):
                create_bigquery_dataset(bq_client, PROJECT_ID, DATASET_ID, location=BIGQUERY_LOCATION)
        except Exception as e:
            logger.error(f"Failed during BigQuery dataset check/create: {e}")
            raise # Stop execution if dataset setup fails

        # --- Configure and Run BigQuery Load Job ---
        gcs_uri = f"gs://{bucket_name}/{file_name}"
        table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
        logger.info(f"Loading data from {gcs_uri} into {table_ref.path}")

        # Define the schema (ensure this matches your JSON structure)
        schema = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("order_status", "STRING"),
            bigquery.SchemaField("order_purchase_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("order_approved_at", "TIMESTAMP"),
            bigquery.SchemaField("order_delivered_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("order_estimated_delivery_date", "DATE"),
            bigquery.SchemaField("customer_zip_code_prefix", "STRING"),
            bigquery.SchemaField("customer_city", "STRING"),
            bigquery.SchemaField("customer_state", "STRING"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("seller_id", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("shipping_charges", "FLOAT"),
            bigquery.SchemaField("payment_sequential", "INTEGER"),
            bigquery.SchemaField("payment_type", "STRING"),
            bigquery.SchemaField("payment_installments", "INTEGER"),
            bigquery.SchemaField("payment_value", "FLOAT"),
            bigquery.SchemaField("product_category_name", "STRING"),
            bigquery.SchemaField("product_weight_g", "FLOAT"),
            bigquery.SchemaField("product_length_cm", "FLOAT"),
            bigquery.SchemaField("product_height_cm", "FLOAT"),
            bigquery.SchemaField("product_width_cm", "FLOAT"),
        ]

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, # Explicit constant
            autodetect=False, # Explicitly False as schema is provided
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # Consider adding error tolerance if needed:
            # max_bad_records=10,
        )

        # Start the load job
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
        )
        logger.info(f"Started BigQuery load job: {load_job.job_id}")

        load_job.result() # Wait for the job to complete

        # --- Log Load Job Results ---
        if load_job.errors:
            logger.error(f"BigQuery load job {load_job.job_id} failed for {gcs_uri}. Errors: {load_job.errors}")
            # Depending on requirements, you might want to raise an exception here
            # to prevent the file from being archived if the load fails.
            raise Exception(f"BigQuery load job failed: {load_job.errors}")
        else:
            rows_loaded = load_job.output_rows
            log_message = (f"BigQuery load job {load_job.job_id} completed. "
                           f"Loaded {rows_loaded} rows into {table_ref.path} from {gcs_uri}")
            logger.info(log_message)


        # --- Archive the Processed File ---
        if ARCHIVE_BUCKET_NAME: # Proceed only if archive bucket is configured
            source_bucket = storage_client.bucket(bucket_name)
            archive_bucket = storage_client.bucket(ARCHIVE_BUCKET_NAME)
            source_blob = source_bucket.blob(file_name)

            if not source_blob.exists():
                 logger.warning(f"Source file {gcs_uri} not found for archiving. It might have been processed already or deleted.")
                 # Decide if this is an error or just a warning
                 return # Exit gracefully if file is already gone

            destination_blob_name = file_name # Keep the same name in the archive bucket
            logger.info(f"Attempting to move {gcs_uri} to gs://{ARCHIVE_BUCKET_NAME}/{destination_blob_name}")

            try:
                # Option 1: Rewrite (preferred for large files, handles copy across locations/classes)
                new_blob = source_bucket.copy_blob(source_blob, archive_bucket, destination_blob_name)
                source_blob.delete() # Delete original after successful copy
                logger.info(f"Successfully copied {file_name} to archive bucket and deleted original.")

                # Option 2: Rename (more efficient if possible: same location, same class)
                try:
                    renamed_blob = source_bucket.rename_blob(source_blob, new_name=f"gs://{ARCHIVE_BUCKET_NAME}/{destination_blob_name}")
                    logger.info(f"Successfully renamed/moved {file_name} to gs://{ARCHIVE_BUCKET_NAME}/{destination_blob_name}")
                except Exception as rename_err:
                    logger.warning(f"Rename failed ({rename_err}), falling back to copy/delete.")
                #     # Fallback logic (copy/delete as above) would go here if needed

            except Exception as e:
                error_message = f"Error moving file {file_name} to archive bucket {ARCHIVE_BUCKET_NAME}: {e}"
                logger.error(error_message)
                # Decide if failure to archive should fail the function.
                # Re-raising the error will cause the function execution to fail,
                # potentially leading to retries of the Pub/Sub message if configured.
                raise e
        else:
            logger.info("ARCHIVE_BUCKET_NAME not set, skipping file archival.")

        # --- Successful Execution ---
        logger.info(f"Function execution completed successfully for event ID: {context.event_id}.")
        # Background functions don't return HTTP responses. Returning None is fine.
        return None

    except ValueError as ve:
        # Handle specific data/configuration errors gracefully
        error_message = f"Configuration or data error (Event ID: {context.event_id if context else 'N/A'}): {ve}"
        logger.error(error_message)
        # Do not raise here to prevent retries for bad data/config.
        return None # Acknowledge the Pub/Sub message
    except Exception as e:
        # Catch all other unexpected errors
        error_message = f"Unhandled error processing event (Event ID: {context.event_id if context else 'N/A'}): {e}"
        logger.error(error_message, exc_info=True) # Log traceback for unexpected errors
        # Re-raise the exception to signal failure to the Cloud Functions runtime.
        # This might trigger retries based on your function's settings.
        raise e