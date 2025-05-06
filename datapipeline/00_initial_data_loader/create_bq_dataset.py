from utils import setup_logging, check_dataset_exists, create_bigquery_dataset
from google.cloud import bigquery
import yaml


if __name__ == "__main__":
    try:
        # Load configuration
        with open("../../config.yaml", "r") as file:
            config = yaml.safe_load(file)

        # Extract nested configs
        master_data_config = config.get("staging_data_loading", {})
        cloud_logging_config = config.get("cloud_logging", {})

        project_id = master_data_config.get("project_id")
        dataset_id = master_data_config.get("dataset_id")
        log_name = cloud_logging_config.get("log_name", "create_dataset_log")
        bigquery_location = master_data_config.get("bigquery_location", "EU")
        table_id = master_data_config.get("table_id")
        create_dataset = master_data_config.get("create_dataset", False)

        # Validate required configs before proceeding
        if not all([project_id, dataset_id, log_name, table_id]):
            missing = [k for k, v in {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "log_name": log_name,
                "table_id": table_id
            }.items() if not v]
            raise ValueError(f"Missing required configuration(s) in config.yaml: {', '.join(missing)}")

        # Setup logging
        logger = setup_logging(project_id, log_name)
        if not logger:
            print("Logging is not set up. The script will continue, but no logs will be sent to Cloud Logging.")

        # Initialize BigQuery client
        try:
            client = bigquery.Client(project=project_id)
        except Exception as e:
            print(f"Error initializing BigQuery client: {e}")
            if logger:
                logger.log_struct({"message": "Failed to initialize BigQuery client", "error": str(e)}, severity="CRITICAL")
            exit(1)

        # Check and create dataset
        if create_dataset:
            dataset_exists = check_dataset_exists(client, project_id, dataset_id, logger)
            if not dataset_exists:
                create_bigquery_dataset(client, project_id, dataset_id, bigquery_location, logger)
                print(f"Dataset {dataset_id} created")
            else:
                print(f"Dataset {dataset_id} already exists")
        else:
            print(f"Dataset {dataset_id} not created")

    except FileNotFoundError:
        print("Error: config.yaml not found. Please create this file with the necessary configuration.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        exit(1)
    except ValueError as ve:
        print(str(ve))
        exit(1)
