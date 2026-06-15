import logging
from datetime import datetime, timedelta
from time import sleep

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

from minio import Minio

MINIO_ENPOINT = Variable.get("MINIO_ENPOINT", "172.17.0.1:9000")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", "minioAdmin")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", "minioAdmin")

logger = logging.getLogger(__name__)
jakarta = pendulum.timezone("Asia/Jakarta")
minio_client = Minio(
    endpoint=f"{MINIO_ENPOINT}",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


@dag(
    dag_id="etl_hospital",
    # default_args={
    #     "depends_on_past": False,
    #     "retries": 2,
    #     "retry_delay": timedelta(minutes=0.5),
    # },
    schedule="30 23 * * *",
    start_date=datetime(2026, 6, 15, tzinfo=jakarta),
    catchup=False,
    tags=["etl", "hospital", "olap", "csv", "clickhouse"],
    description="ETL Hospital data CSV to ClickHouse",
)
def pipeline():
    @task(task_id="ping_conn")
    def ping_conn():
        # MinIO
        logger.info(
            f"MinIO: {requests.get(f'http://{MINIO_ENPOINT}/minio/health/live')} |\t\t"
        )

        # ClickHouse
        logger.info("ClickHouse")

    t_ping_conn = ping_conn()

    (t_ping_conn)


pipeline()
