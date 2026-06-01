import logging
from datetime import datetime, timedelta
from time import sleep

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)
RDS_EX = Variable.get("RDS_EX", False)


@dag(
    dag_id="rds_example_1",
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=0.5),
    },
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
    description="example ini merupakan cheat sheet untuk pembuatan DAG"
)
def pipeline():
    @task.branch(task_id="tsk_1_decision")
    def tsk_1_decision(**kwargs):
        logger.info("Task 1 |\t\t")
        sleep(10)
        if not RDS_EX:
            logger.info("TO Task End |\t\t")
            return "tsk_2_end"
        ti = kwargs["ti"]
        ti.xcom_push(key="key_tsk_1", value={"a": 1, "b": 2, "c": 3})
        return "tsk_2_parallel"

    @task(task_id="tsk_2_end")
    def tsk_2_end():
        logger.info("Task End |\t\t")

    @task(task_id="tsk_2_parallel")
    def tsk_2_parallel(**kwargs):
        logger.info("Task 2 |\t\t")
        ti = kwargs["ti"]
        val_tsk_1 = ti.xcom_pull(
            key="key_tsk_1", task_ids="tsk_1_decision")
        logger.info(f"Value Task 1 : {val_tsk_1} |\t\t")
        logger.info(f"Value b : {val_tsk_1["b"]} |\t\t")
        sleep(10)

    @task(task_id="tsk_3_1")
    def tsk_3_1(**kwargs):
        logger.info("Task 3 bagian 1 |\t\t")
        sleep(10)

    @task(task_id="tsk_3_2")
    def tsk_3_2(**kwargs):
        logger.info("Task 3 bagian 2 |\t\t")
        sleep(10)

    task_1 = tsk_1_decision()
    task_2 = tsk_2_parallel()
    task_2_end = tsk_2_end()
    task_3_1 = tsk_3_1()
    task_3_2 = tsk_3_2()

    task_1 >> task_2_end
    task_1 >> task_2 >> [task_3_1, task_3_2]


pipeline()
