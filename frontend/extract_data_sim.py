from __future__ import annotations

import pendulum
import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pandas as pd

# Source CSV Bucket and Path
CSV_BUCKET_NAME = "europe-west3-extract-data-s-69ecc830-bucket"
CSV_BLOB_PATH = "data/df_pubsub.csv"

# Destination JSON Bucket
JSON_BUCKET_NAME = "staging-ecomm-data-j"

@dag(
    schedule="*/5 * * * *",  # Cron expression for running every 5 minutes
    start_date=pendulum.datetime(2025, 4, 29, tz="UTC"),
    catchup=False,
    tags=["csv_to_gcs"],
)
def process_csv_and_upload_timed():
    @task
    def extract_data_from_csv(bucket_name, blob_path):
        """Reads data from a CSV file in GCS into a Pandas DataFrame."""
        gcs_path = f"gs://{bucket_name}/{blob_path}"
        try:
            df = pd.read_csv(gcs_path)
            return df
        except Exception as e:
            print(f"Error reading CSV from GCS: {e}")
            return None

    @task
    def transform_to_json(df: pd.DataFrame):
        """Extracts the first row of the DataFrame into a list of JSON objects and saves the rest back to GCS."""
        extracted_data = {}
        remaining_df = df.copy()
        if df is not None and not df.empty:
            first_row = df.iloc[0]
            
            # Assuming your CSV columns are in the same order as before
            extracted_data["order_id"] = str(first_row[0]) if pd.notna(first_row[0]) else None
            extracted_data["customer_id"] = str(first_row[1]) if pd.notna(first_row[1]) else None
            extracted_data["order_status"] = str(first_row[2]) if pd.notna(first_row[2]) else None
            extracted_data["order_purchase_timestamp"] = str(first_row[3]) if pd.notna(first_row[3]) else None
            extracted_data["order_approved_at"] = str(first_row[4]) if pd.notna(first_row[4]) else None
            extracted_data["order_delivered_timestamp"] = str(first_row[5]) if pd.notna(first_row[5]) else None
            extracted_data["order_estimated_delivery_date"] = str(first_row[6]) if pd.notna(first_row[6]) else None
            extracted_data["customer_zip_code_prefix"] = str(int(first_row[7])) if pd.notna(first_row[7]) else None
            extracted_data["customer_city"] = str(first_row[8]) if pd.notna(first_row[8]) else None
            extracted_data["customer_state"] = str(first_row[9]) if pd.notna(first_row[9]) else None
            extracted_data["product_id"] = str(first_row[10]) if pd.notna(first_row[10]) else None
            extracted_data["seller_id"] = str(first_row[11]) if pd.notna(first_row[11]) else None
            extracted_data["price"] = float(first_row[12]) if pd.notna(first_row[12]) else None
            extracted_data["shipping_charges"] = float(first_row[13]) if pd.notna(first_row[13]) else None
            extracted_data["payment_sequential"] = int(first_row[14]) if pd.notna(first_row[14]) else None
            extracted_data["payment_type"] = str(first_row[15]) if pd.notna(first_row[15]) else None
            extracted_data["payment_installments"] = int(first_row[16]) if pd.notna(first_row[16]) else None
            extracted_data["payment_value"] = float(first_row[17]) if pd.notna(first_row[17]) else None
            extracted_data["product_category_name"] = str(first_row[18]) if pd.notna(first_row[18]) else None
            extracted_data["product_weight_g"] = float(first_row[19]) if pd.notna(first_row[19]) else None
            extracted_data["product_length_cm"] = float(first_row[20]) if pd.notna(first_row[20]) else None
            extracted_data["product_height_cm"] = float(first_row[21]) if pd.notna(first_row[21]) else None
            extracted_data["product_width_cm"] = float(first_row[22]) if pd.notna(first_row[22]) else None


            # Print the data types of the values in json_obj
            # print("Data types before json.dumps():")
            # for key, value in json_obj.items():
            #     print(f"{key}: {type(value)}")  # Print key and type

            # Remove the first row
            remaining_df = df.iloc[1:]
            remaining_df.reset_index(drop=True, inplace=True)

            # Save the remaining DataFrame back to GCS in the 'data' folder, overwriting the file
            gcs_client = storage.Client()
            bucket = gcs_client.bucket(CSV_BUCKET_NAME)
            blob = bucket.blob("data/df_pubsub.csv")
            csv_buffer = remaining_df.to_csv(index=False, encoding='utf-8')
            blob.upload_from_string(csv_buffer, content_type='text/csv')
            print(f"Remaining DataFrame saved to gs://{CSV_BUCKET_NAME}/data/df_pubsub.csv")
        return extracted_data

    def upload_json_to_gcs(extracted_data, bucket_name):
        """Uploads the extracted JSON data to a specified GCS bucket with a timestamped filename."""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"simulation_{current_datetime}.json"
        blob = bucket.blob(blob_name)
        json_string = json.dumps(extracted_data, indent=4)
        blob.upload_from_string(json_string, content_type="application/json")
        print(f"JSON data uploaded to gs://{bucket_name}/{blob_name}")

    read_csv_task = extract_data_from_csv(bucket_name=CSV_BUCKET_NAME, blob_path=CSV_BLOB_PATH)
    json_data = transform_to_json(df=read_csv_task)
    upload_task = PythonOperator(
        task_id="upload_json_to_gcs",
        python_callable=upload_json_to_gcs,
        op_kwargs={
            "extracted_data": json_data,
            "bucket_name": JSON_BUCKET_NAME,
        },
    )

    read_csv_task >> json_data >> upload_task

process_csv_and_upload_timed()