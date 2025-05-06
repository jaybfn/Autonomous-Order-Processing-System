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
        logger.basicConfig(level=logger.ERROR, filename="local_error.log")
        logger.error(f"Failed to set up Cloud Logging: {e}")
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
    

def delete_bigquery_dataset(client, project_id, dataset_id, logger=None):
    """Deletes a BigQuery dataset."""
    try:
        log_message = f"Deleting dataset '{dataset_id}' in project '{project_id}'."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
        dataset_ref = client.dataset(dataset_id)
        client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        log_message = f"Dataset '{dataset_id}' deleted in project '{project_id}'."
        if logger:
            logger.log_struct({"message": log_message}, severity="INFO")
        print(log_message)
    except Exception as e:
        error_message = f"An error occurred while deleting the dataset '{dataset_id}': {e}"
        if logger:
            logger.log_struct({"message": error_message, "error": str(e)}, severity="ERROR")
        print(error_message)