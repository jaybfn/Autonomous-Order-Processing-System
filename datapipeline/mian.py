import json
import logging
import os

from google.cloud import bigquery
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load configuration from environment variables (set during deployment)
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "EU")

# Initialize BigQuery and Storage clients
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

def load_json_from_gcs_to_bigquery(event, context):
    """Triggered by a Cloud Storage event; loads JSON data to BigQuery."""
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

    except Exception as e:
        logging.error(f"Error processing gs://{bucket_name}/{file_name}: {e}")