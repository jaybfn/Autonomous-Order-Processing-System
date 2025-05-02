from google.cloud import bigquery
import google.cloud.logging
import pandas as pd
import io
import yaml
import os

def setup_logging(project_id, log_name="bigquery-loader"):
    """Sets up Cloud Logging."""
    try:
        logging_client = google.cloud.logging.Client(project=project_id)  # ADC is used here
        logger = logging_client.logger(log_name)
        return logger
    except Exception as e:
        print(f"Error setting up Cloud Logging: {e}")
        logging.basicConfig(level=logging.ERROR, filename="local_error.log")
        logging.error(f"Failed to set up Cloud Logging: {e}")
        return None

def check_dataset_exists(client, project_id, dataset_id, logger):
    """Checks if a BigQuery dataset exists."""
    dataset_ref = client.dataset(dataset_id, project=project_id)
    try:
        client.get_dataset(dataset_ref)
        log_message = f"Dataset '{dataset_id}' in project '{project_id}' already exists."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
        return True
    except Exception as e:
        if "Not found" in str(e):
            log_message = f"Dataset '{dataset_id}' in project '{project_id}' does not exist."
            if logger:
                logger.log_struct({"message": log_message}, severity="INFO")
            print(log_message)
            return False
        else:
            log_message = f"Error checking dataset '{dataset_id}': {e}"
            if logger:
                logger.log_struct({"message": log_message, "error": str(e)}, severity="ERROR")
            print(log_message)
            return False

def create_bigquery_dataset(client, project_id, dataset_id, location="EU", logger=None):
    """Creates a BigQuery dataset."""
    try:
        log_message = f"Creating dataset '{dataset_id}' in project '{project_id}' at location '{location}'."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset)
        log_message = f"Dataset '{dataset_id}' created in project '{project_id}' with location '{dataset.location}'."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
        return True
    except Exception as e:
        error_message = f"An error occurred while creating the dataset '{dataset_id}': {e}"
        if logger:
            logger.log_struct({"message": error_message, "error": str(e)}, severity="ERROR")
        print(error_message)
        return False

def load_csv_to_bigquery(csv_data, client, project_id, dataset_id, table_id, logger=None):
    """Loads CSV data into a BigQuery table with Cloud Logging."""
    try:
        log_message = f"Attempting to load data into table '{table_id}' in dataset '{dataset_id}', project '{project_id}'."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
        table_ref = client.dataset(dataset_id).table(table_id)
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
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=False,
        )
        load_job = client.load_table_from_file(
            io.StringIO(csv_data),
            table_ref,
            job_config=job_config,
        )
        load_job.result()
        rows_loaded = load_job.output_rows
        log_message = f"Loaded {rows_loaded} rows to {project_id}.{dataset_id}.{table_id}"
        if logger:
            logger.log_struct({"message": log_message, "rows_loaded": rows_loaded}, severity="INFO")
        print(log_message)
    except Exception as e:
        error_message = f"An error occurred while loading data to '{table_id}': {e}"
        if logger:
            logger.log_struct({"message": error_message, "error": str(e)}, severity="ERROR")
        print(error_message)


if __name__ == "__main__":
    # Load configuration from YAML file
    try:
        with open('../config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        data_config = config.get('master_data_loading', {})
        logging_config = config.get('cloud_logging', {})

        csv_file_path = data_config.get('csv_file_path')
        project_id = data_config.get('project_id')
        dataset_id = data_config.get('dataset_id')
        table_id = data_config.get('table_id')
        bigquery_location = data_config.get('bigquery_location', "EU")
        log_name = logging_config.get('log_name', 'bigquery-loader')

        if not all([csv_file_path, project_id, dataset_id, table_id]):
            raise ValueError("Missing required configuration in config.yaml")

    except FileNotFoundError:
        print("Error: config.yaml not found. Please create this file with the necessary configuration.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        exit(1)
    except ValueError as e:
        print(f"Error in configuration: {e}")
        exit(1)

    # Initialize logging
    logger = setup_logging(project_id, log_name)
    if not logger:
        print("Logging is not set up.  The script will continue, but no logs will be sent to Cloud Logging.")
    # Initialize the BigQuery client
    try:
        client = bigquery.Client(project=project_id) #ADC
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        if logger:
            logger.log_struct({"message": "Failed to initialize BigQuery client", "error": str(e)}, severity="CRITICAL")
        exit(1)

    # Check if the dataset exists
    if not check_dataset_exists(client, project_id, dataset_id, logger):
        # If it doesn't exist, create it
        if create_bigquery_dataset(client, project_id, dataset_id, location=bigquery_location, logger=logger):
            print(f"Dataset {dataset_id} created")
        else:
            print(f"Failed to create dataset {dataset_id}")
            if logger:
                logger.log_struct({"message": f"Failed to create dataset {dataset_id}"}, severity="CRITICAL")
            exit(1)
    else:
        log_message = f"Skipping dataset creation as '{dataset_id}' already exists."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)

    # Read the CSV data using pandas
    try:
        df = pd.read_csv(csv_file_path)
        csv_data = df.to_csv(index=False)
    except FileNotFoundError:
        error_message = f"Error: CSV file not found at {csv_file_path}"
        if logger:
            logger.log_struct({"message": error_message}, severity="ERROR")
        print(error_message)
        exit()
    except Exception as e:
        error_message = f"Error reading CSV file: {e}"
        if logger:
            logger.log_struct({"message": error_message, "error": str(e)}, severity="ERROR")
        print(error_message)
        exit()

    # Load the CSV data to BigQuery
    load_csv_to_bigquery(csv_data, client, project_id, dataset_id, table_id, logger)

    print("Data loading process completed")
    if logger:
        print("Check the logs in Google Cloud Logging for more details.")
    else:
        print("Check local_error.log for any errors.")