import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# -------------------------------
# Default Airflow configurations
# -------------------------------
default_args = {
    "owner": "team alpha",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------
# DAG Definition
# --------------------------------
dag = DAG(
    dag_id="weather_batch_pipeline",
    default_args=default_args,
    description="Daily Weather Data ETL Pipeline with Spark + MinIO",
    schedule_interval=timedelta(days=1),     # Runs once per day
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["weather", "spark", "minio", "etl"]
)

# # --------------------------------
# # Task 1: Fetch batch weather data
# # --------------------------------
fetch_batch_data = BashOperator(
    task_id="fetch_batch_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_producer.py {{ ds }}",
    dag=dag,
)

# --------------------------------s
# Task 2: Consume batch data to MinIO raw zone
# --------------------------------
consume_batch_data = BashOperator(
    task_id="consume_batch_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_consumer.py {{ ds }}",
    dag=dag,
)
# # --------------------------------
# # Task 3: Process batch data with Spark (ETL + aggregation)
# # --------------------------------
process_batch_data = BashOperator(
    task_id="process_batch_data",
    bash_command="""
    docker exec bigdatafinalproject-spark-client-1 \
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
            /opt/spark/jobs/batch_data_processor.py {{ ds }}
    """,
    dag=dag,
)

# # --------------------------------
# # Task 4: Load to snowflake
# # (optional daily restart to ensure freshness)
# # --------------------------------
load_to_snowflake = BashOperator(
    task_id="load_historical_to_snowflake",
    bash_command = "python /opt/airflow/dags/scripts/load_to_snowflake.py {{ ds }}",
    dag=dag
)

# # --------------------------------
# # Task 5: Completion notification
# # --------------------------------
process_complete = BashOperator(
    task_id="process_complete",
    bash_command='echo "✅ Weather batch + streaming pipeline completed for {{ ds }}"',
    dag=dag,
)

# --------------------------------
# Task Dependencies (Execution Flow)
# -------------------------------

fetch_batch_data >> consume_batch_data >> process_batch_data >> load_to_snowflake >> process_complete
