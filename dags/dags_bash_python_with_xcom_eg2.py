from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import datetime
import pendulum

with DAG(
    dag_id="dags_bash_python_with_xcom_eg2",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo PUSH_START "
        '{{ti.xcom_push(key="bash_pushed", value=200)}} && '
        "echo PUSH_COMPLETE",
    )

    @task(task_id="python_pull")
    def python_pull_xcom(**kwargs):
        ti = kwargs["ti"]
        status_value = ti.xcom_pull(key="bash_pushed")
        return_value = ti.xcom_pull(task_ids="bash_push")
        print("status_value:" + str(status_value))
        print("return_value:" + return_value)

    bash_push >> python_pull_xcom()
