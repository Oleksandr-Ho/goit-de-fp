from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

"""
Цей файл - DAG (Airflow), який послідовно запускає 3 Spark jobs:

1) landing_to_bronze_oholodetskyi
2) bronze_to_silver_oholodetskyi
3) silver_to_gold_oholodetskyi
"""

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),  
}

with DAG(
    dag_id="project_solution_oholodetskyi",  # назва DAG
    default_args=default_args,
    schedule_interval=None,  # без розкладу, запуск вручну
    catchup=False,
    tags=["oholodetskyi"]
) as dag:

    # Перший таск: landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        conn_id="spark-default",  # Airflow Connection ID
        application="dags/oholodetskyi/landing_to_bronze_oholodetskyi.py",  # Шлях відносно кореня репо
        name="landing_to_bronze_oholodetskyi",
        verbose=1  # рівень логів
    )

    # Другий таск: bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark-default",
        application="dags/oholodetskyi/bronze_to_silver_oholodetskyi.py",
        name="bronze_to_silver_oholodetskyi",
        verbose=1
    )

    # Третій таск: silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="spark-default",
        application="dags/oholodetskyi/silver_to_gold_oholodetskyi.py",
        name="silver_to_gold_oholodetskyi",
        verbose=1
    )

    # Зв'язуємо задачі у послідовність
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
