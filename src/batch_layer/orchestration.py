"""
Databricks Job Orchestration: Batch Layer
Defines and manages all batch processing jobs for item properties
"""
import json
from datetime import datetime

# Job configuration for Databricks
BATCH_JOBS = {
    "load_item_properties_bronze": {
        "name": "Load Item Properties (Bronze)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/batch_layer/load_properties_bronze.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2
        },
        "schedule": {
            "quartz_cron_expression": "0 0 * * * ?",  # Daily at midnight
            "timezone_id": "UTC"
        },
        "timeout_seconds": 3600
    },
    "transform_item_properties_silver": {
        "name": "Transform Item Properties (Silver)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/batch_layer/transform_properties_silver.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2
        },
        "depends_on_past_runs": True,
        "timeout_seconds": 3600
    },
    "aggregate_item_properties_gold": {
        "name": "Aggregate Item Properties (Gold)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/batch_layer/aggregate_properties_gold.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2
        },
        "depends_on_past_runs": True,
        "timeout_seconds": 3600
    }
}

SPEED_JOBS = {
    "ingest_events_kinesis_bronze": {
        "name": "Ingest Events from Kinesis (Bronze)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/speed_layer/ingest_events_bronze.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.2xlarge",
            "num_workers": 3,
            "instance_profile_arn": "arn:aws:iam::<account-id>:instance-profile/databricks-profile"
        },
        "timeout_seconds": 0  # Run indefinitely (streaming)
    },
    "transform_events_silver": {
        "name": "Transform Events (Silver)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/speed_layer/transform_events_silver.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.2xlarge",
            "num_workers": 3
        },
        "timeout_seconds": 0  # Run indefinitely (streaming)
    },
    "aggregate_events_gold": {
        "name": "Aggregate Events for Analytics (Gold)",
        "job_type": "SPARK_PYTHON",
        "spark_python_task": {
            "python_file": "dbfs:/src/speed_layer/aggregate_events_gold.py"
        },
        "new_cluster": {
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "i3.2xlarge",
            "num_workers": 3
        },
        "timeout_seconds": 0  # Run indefinitely (streaming)
    }
}

if __name__ == "__main__":
    print("Batch Layer Jobs:")
    print(json.dumps(BATCH_JOBS, indent=2))
    print("\n\nSpeed Layer Jobs:")
    print(json.dumps(SPEED_JOBS, indent=2))
