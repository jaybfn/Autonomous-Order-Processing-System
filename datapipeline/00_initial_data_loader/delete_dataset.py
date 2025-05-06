
from utils import setup_logging, delete_bigquery_dataset
from google.cloud import bigquery
import google.cloud.logging
import pandas as pd
import yaml  # Make sure you import the yaml library

if __name__ == "__main__":
    # Load configuration from config.yaml

    try:
        with open("../config.yaml", "r") as file:
            config = yaml.safe_load(file)
            master_data_config = config.get("master_data_loading", {})
            cloud_logging_config = config.get("cloud_logging", {})

            project_id = master_data_config.get("project_id")
            dataset_id = master_data_config.get("dataset_id")
            log_name = cloud_logging_config.get("log_name", "delete_dataset_log")
            bigquery_location = master_data_config.get("bigquery_location", "EU")
            csv_file_path = master_data_config.get("csv_file_path")
            table_id = master_data_config.get("table_id")
            delete_dataset = master_data_config.get("dataset_id", False)
            logger = setup_logging(project_id, log_name)
            if not logger:
                print("Logging is not set up. The script will continue, but no logs will be sent to Cloud Logging.")
            try:
                client = bigquery.Client(project=project_id) #ADC
            except Exception as e:
                print(f"Error initializing BigQuery client: {e}")
                if logger:
                    logger.log_struct({"message": "Failed to initialize BigQuery client", "error": str(e)}, severity="CRITICAL")
                exit(1)
            if delete_dataset:

                delete_bigquery_dataset(client, project_id, dataset_id, logger)
                print(f"Dataset {dataset_id} deleted")
            else:
                print(f"Dataset {dataset_id} not deleted")
            if not all([project_id, dataset_id, log_name, csv_file_path, table_id]):
                raise ValueError("Missing required configuration in config.yaml")
    except FileNotFoundError:
        print("Error: config.yaml not found. Please create this file with the necessary configuration.")
        exit(1)