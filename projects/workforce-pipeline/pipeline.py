"""End-to-end workforce data pipeline example.

This module sketches how roster events flow from Kafka through Spark and into a
Postgres warehouse, all orchestrated by Airflow. It uses only free and
open-source components so the entire stack can run on the AWS Free Tier or
locally with Docker.
"""

import csv
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from pyspark.sql import SparkSession

BROKER = "localhost:9092"
TOPIC = "workforce_roster"


def publish_roster() -> None:
    """Stream the sample roster into a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    with open("workforce_daily_roster.csv", newline="") as f:
        for row in csv.DictReader(f):
            producer.send(TOPIC, row)
    producer.flush()


def spark_transform() -> None:
    """Consume roster events with Spark and load them to Postgres and Parquet."""
    spark = SparkSession.builder.appName("workforce_roster").getOrCreate()
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .load()
    )
    parsed = spark.read.json(
        df.selectExpr("CAST(value AS STRING)").rdd.map(lambda r: r.value)
    )
    (
        parsed.write.mode("overwrite")
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/workforce")
        .option("dbtable", "public.roster")
        .save()
    )
    parsed.write.mode("overwrite").parquet("roster.parquet")


with DAG(
    dag_id="workforce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    publish = PythonOperator(task_id="publish_roster", python_callable=publish_roster)
    transform = PythonOperator(task_id="spark_transform", python_callable=spark_transform)

    publish >> transform
