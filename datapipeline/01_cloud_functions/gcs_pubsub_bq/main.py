import base64
import json
from google.cloud import bigquery
import logging
import google.cloud.logging
import os
from google.cloud import storage

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID')
TABLE_ID = os.environ.get('TABLE_ID')
STAGING_DATASET_ID = os.environ.get('STAGING_DATASET_ID')
STAGING_TABLE_ID = os.environ.get('STAGING_TABLE_ID')
BIGQUERY_LOCATION = os.environ.get('BIGQUERY_LOCATION', "EU")
ARCHIVE_BUCKET_NAME = os.environ.get('ARCHIVE_BUCKET_NAME')
STAGING_BUCKET_NAME = os.environ.get('STAGING_BUCKET_NAME')
LOG_NAME = "bigquery-loader-function"

# --- Logging Setup ---
def setup_logging():
    """Sets up Cloud Logging and returns a logger. Handles errors."""
    try:
        client = google.cloud.logging.Client(project=PROJECT_ID)
        handler = google.cloud.logging.handlers.CloudLoggingHandler(client, name=LOG_NAME)
        google.cloud.logging.handlers.setup_logging(handler, log_level=logging.INFO)
        print(f"Successfully initialized Cloud Logging with handler for {LOG_NAME}.")
        return logging.getLogger(__name__)
    except Exception as e:
        print(f"Error initializing Cloud Logging: {e}. Falling back to standard logging.")
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to set up Cloud Logging: {e}")
        return logger

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
        return True
    except Exception as e:
        if "Not found" in str(e):
            log_message = f"Dataset '{dataset_id}' in project '{project_id}' does not exist."
            logger.info(log_message)
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
        dataset_ref = client.dataset(dataset_id, project=project_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset, exists_ok=True)
        log_message = f"Dataset '{dataset.dataset_id}' ensured in project '{project_id}' with location '{dataset.location}'."
        logger.info(log_message)
        return True
    except Exception as e:
        error_message = f"An error occurred while creating the dataset '{dataset_id}': {e}"
        logger.error(error_message)
        raise e

def archive_file(file_name):
    """Archives the processed file to another bucket."""
    try:
        logger.info("Archiving file: %s", file_name)
        source_blob = storage_client.bucket(STAGING_BUCKET_NAME).blob(file_name)
        destination_blob = storage_client.bucket(ARCHIVE_BUCKET_NAME).blob(file_name)
        destination_blob.rewrite(source_blob)
        source_blob.delete()
        logger.info("Archived file: %s", file_name)
    except Exception as e:
        logger.error("Error archiving file %s: %s", file_name, str(e), exc_info=True)
        raise

def execute_bigquery_query(client, query):
    """Executes a BigQuery query."""
    try:
        job = client.query(query)
        job.result()  # Wait for the query to complete
        if job.errors:
            raise Exception(f"BigQuery query failed: {job.errors}")
        logger.info(f"BigQuery query completed successfully. Job ID: {job.job_id}")
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        raise


# --- Main Cloud Function Logic ---
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
    global logger

    try:
        # 1. Decode the Pub/Sub message data directly from the event dictionary
        if 'data' in event:
            file_info = event['data']
            logger.info(f"Pub/Sub message data: {file_info}")  # Add this line for debugging
            if isinstance(file_info, dict):
                # Handle the case where event['data'] is already a dictionary
                bucket_name = file_info.get('bucket')
                file_name = file_info.get('name')
            elif isinstance(file_info, (bytes, str)):
                # Handle the case where event['data'] is base64-encoded string
                file_info_decoded = base64.b64decode(file_info).decode('utf-8')
                data = json.loads(file_info_decoded)
                bucket_name = data.get('bucket')
                file_name = data.get('name')
            else:
                raise ValueError(f"Unexpected type for event['data']: {type(file_info)}")

            if not bucket_name or not file_name:
                raise ValueError("Pub/Sub message data is missing 'bucket' or 'name' keys.")
        else:
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
            if not check_dataset_exists(bq_client, PROJECT_ID, STAGING_DATASET_ID):
                create_bigquery_dataset(bq_client, PROJECT_ID, STAGING_DATASET_ID, location=BIGQUERY_LOCATION)
            if not check_dataset_exists(bq_client, PROJECT_ID, DATASET_ID):
                create_bigquery_dataset(bq_client, PROJECT_ID, DATASET_ID, location=BIGQUERY_LOCATION)
        except Exception as e:
            logger.error(f"Failed during BigQuery dataset check/create: {e}")
            raise  # Stop execution if dataset setup fails

        # --- Configure and Run BigQuery Load Job to Staging Table ---
        gcs_uri = f"gs://{bucket_name}/{file_name}"
        staging_table_ref = bq_client.dataset(STAGING_DATASET_ID).table(STAGING_TABLE_ID)
        logger.info(f"Loading data from {gcs_uri} into staging table: {staging_table_ref.path}")

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

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Truncate staging table
        )

        try:
            load_job = bq_client.load_table_from_uri(
                gcs_uri,
                staging_table_ref,
                job_config=job_config,
            )
            logger.info(f"Started BigQuery load job to staging table: {load_job.job_id}")
            load_job.result()
            if load_job.errors:
                logger.error(f"BigQuery load job to staging table {load_job.job_id} failed for {gcs_uri}. Errors: {load_job.errors}")
                raise Exception(f"BigQuery load job to staging table failed: {load_job.errors}")
            else:
                rows_loaded = load_job.output_rows
                logger.info(f"Loaded {rows_loaded} rows into staging table: {staging_table_ref.path} from {gcs_uri}")
        except Exception as e:
            logger.error(f"Error loading data to staging table: {e}")
            raise

        # --- Merge Data from Staging to Final Table ---
        final_table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
        # Use client.dataset(...).table(...).table_id to get the table ID string
        staging_table_ref_for_query = f"`{bq_client.dataset(STAGING_DATASET_ID).table(STAGING_TABLE_ID).table_id}`"
        final_table_ref_for_query = f"`{bq_client.dataset(DATASET_ID).table(TABLE_ID).table_id}`"


        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS target
        USING `{PROJECT_ID}.{STAGING_DATASET_ID}.{STAGING_TABLE_ID}` AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN
          UPDATE SET
            target.customer_id = source.customer_id,
            target.order_status = source.order_status,
            target.order_purchase_timestamp = source.order_purchase_timestamp,
            target.order_approved_at = source.order_approved_at,
            target.order_delivered_timestamp = source.order_delivered_timestamp,
            target.order_estimated_delivery_date = source.order_estimated_delivery_date,
            target.customer_zip_code_prefix = source.customer_zip_code_prefix,
            target.customer_city = source.customer_city,
            target.customer_state = source.customer_state,
            target.product_id = source.product_id,
            target.seller_id = source.seller_id,
            target.price = source.price,
            target.shipping_charges = source.shipping_charges,
            target.payment_sequential = source.payment_sequential,
            target.payment_type = source.payment_type,
            target.payment_installments = source.payment_installments,
            target.payment_value = source.payment_value,
            target.product_category_name = source.product_category_name,
            target.product_weight_g = source.product_weight_g,
            target.product_length_cm = source.product_length_cm,
            target.product_height_cm = source.product_height_cm,
            target.product_width_cm = source.product_width_cm
        WHEN NOT MATCHED THEN
          INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
                  order_delivered_timestamp, order_estimated_delivery_date, customer_zip_code_prefix,
                  customer_city, customer_state, product_id, seller_id, price, shipping_charges,
                  payment_sequential, payment_type, payment_installments, payment_value,
                  product_category_name, product_weight_g, product_length_cm, product_height_cm,
                  product_width_cm)
          VALUES (source.order_id, source.customer_id, source.order_status, source.order_purchase_timestamp,
                  source.order_approved_at, source.order_delivered_timestamp, source.order_estimated_delivery_date,
                  source.customer_zip_code_prefix, source.customer_city, source.customer_state, source.product_id,
                  source.seller_id, source.price, source.shipping_charges, source.payment_sequential,
                  source.payment_type, source.payment_installments, source.payment_value,
                  source.product_category_name, source.product_weight_g, source.product_length_cm,
                  source.product_height_cm, source.product_width_cm);
        """
        logger.info(f"Executing merge query from {bq_client.dataset(STAGING_DATASET_ID).table(STAGING_TABLE_ID).table_id} to {bq_client.dataset(DATASET_ID).table(TABLE_ID).table_id}")
        try:
            execute_bigquery_query(bq_client, merge_query)
            logger.info("Successfully executed merge query.")
        except Exception as e:
            logger.error(f"Error executing merge query: {e}")
            raise
        
        archive_file(file_name)
        logger.info(f"Function execution completed successfully for event ID: {context.event_id}.")
        return None

    except ValueError as ve:
        error_message = f"Configuration or data error (Event ID: {context.event_id if context else 'N/A'}): {ve}"
        logger.error(error_message)
        return None
    except Exception as e:
        error_message = f"Unhandled error processing event (Event ID: {context.event_id if context else 'N/A'}): {e}"
        logger.error(error_message, exc_info=True)
        raise e
