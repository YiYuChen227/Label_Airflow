try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.dates import days_ago
    from airflow.operators.python import ShortCircuitOperator
    from datetime import datetime
    from datetime import timedelta
    from croniter import croniter
    from airflow.models import Variable
    from pathlib import Path
    import json
    from airflow.hooks.base import BaseHook
    from os.path import abspath

except Exception as e:
    error_class = e.__class__.__name__ 
    detail = e.args[0]
    cl, exc, tb = sys.exc_info() 
    lastCallStack = traceback.extract_tb(tb)[-1] 
    fileName = lastCallStack[0]
    lineNum = lastCallStack[1]
    funcName = lastCallStack[2]
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(
        fileName, lineNum, funcName, error_class, detail)
    print(errMsg)


airflow_keytab = json.loads(Variable.get("airflow_keytab"))

args = {
    'owner': 'user_name',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        dag_id='renew_keytab',
        default_args=args,
        schedule_interval='0 * * * *',
        start_date=datetime(2021,10,27,13, 0),
        tags=['ETL_ADW_REGULAR'],
) as dag:
    start_task = DummyOperator(
        task_id='start_task',
    )

    authentication_task = BashOperator(
        task_id='renew_keytab',
        bash_command=f'kinit -kt {airflow_keytab} uairflow',
    )
    start_task >> authentication_task

if __name__ == "__main__":
    dag.cli()
                                                                              

                



