import json
import logging
import os

from google.cloud import bigquery
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load configuration from environment variables
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "EU")
ARCHIVE_BUCKET_NAME = os.environ.get("ARCHIVE_BUCKET_NAME")
STAGING_BUCKET_NAME = os.environ.get("STAGING_BUCKET_NAME")

# Initialize BigQuery and Storage clients
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

def archive_processed_data(bucket_name, file_name):
    """Moves the processed data to the archive bucket."""
    if not ARCHIVE_BUCKET_NAME or not STAGING_BUCKET_NAME or bucket_name != STAGING_BUCKET_NAME:
        logging.warning("Archive bucket or staging bucket not configured correctly or wrong bucket. Skipping archiving.")
        return

    logging.info(f"Archiving file: gs://{bucket_name}/{file_name} to gs://{ARCHIVE_BUCKET_NAME}/{file_name}")

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)
    destination_bucket = storage_client.bucket(ARCHIVE_BUCKET_NAME)

    try:
        # Copy the blob to the archive bucket
        destination_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, file_name
        )
        logging.info(f"File copied to: gs://{destination_bucket.name}/{destination_blob.name}")

        # Delete the blob from the staging bucket
        source_blob.delete()
        logging.info(f"File deleted from: gs://{source_bucket.name}/{source_blob.name}")

    except Exception as e:
        logging.error(f"Error during archiving of gs://{bucket_name}/{file_name}: {e}")

def load_json_from_gcs_to_bigquery(event, context):
    """Triggered by a Cloud Storage event; loads JSON data to BigQuery and calls archiving."""
    bucket_name = event['bucket']
    file_name = event['name']

    logging.info(f"Processing file: gs://{bucket_name}/{file_name}")

    if not file_name.endswith(".json"):
        logging.info("File is not a JSON file. Skipping.")
        return

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        json_content = blob.download_as_text(encoding="utf-8")
        data = json.loads(json_content)

        if not isinstance(data, list):
            data = [data]  # Ensure we have a list of rows

        table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)

        job_config = bigquery.LoadJobConfig(
            schema=[
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
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        )

        load_job = bq_client.load_table_from_json(
            [data],  # Pass the list of rows
            table_ref,
            job_config=job_config,
        )

        load_job.result()  # Waits for the job to complete

        logging.info(f"Loaded {load_job.output_rows} rows into {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

        # Call the archiving function
        archive_processed_data(bucket_name, file_name)

    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from gs://{bucket_name}/{file_name}: {json_content}")
    except Exception as e:
        logging.error(f"Error processing gs://{bucket_name}/{file_name}: {e}")