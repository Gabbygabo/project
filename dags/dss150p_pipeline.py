from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

def failure_callback(context):
    print(f"TASK FAILED: {context['task_instance'].task_id}")
    print(f"RUN ID: {context['run_id']}")
    print(f"ERROR: {context.get('exception')}")

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',   # daily at 2 AM
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(9, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            f'python -m src.cli extract --mode {{{{ params.run_mode }}}} '
            f'--year {{{{ params.year }}}} --month {{{{ params.month }}}}'
        ),
        execution_timeout=timedelta(minutes=30)
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            f'python -m src.cli transform --run-id "{{{{ run_id }}}}"'
        ),
        execution_timeout=timedelta(minutes=30)
    )

    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            f'python -m src.cli load --run-id "{{{{ run_id }}}}"'
        ),
        execution_timeout=timedelta(minutes=30)
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            f'python -m src.cli validate --run-id "{{{{ run_id }}}}"'
        ),
        execution_timeout=timedelta(minutes=30)
    )

    # ✅ Classic ETL pipeline chain
    extract >> transform >> load >> validate
