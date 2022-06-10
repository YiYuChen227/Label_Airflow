try: 
    from airflow import DAG, macros
    from airflow.operators.bash import BashOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.dates import days_ago
    from airflow.operators.python import ShortCircuitOperator
    from datetime import datetime,timezone, timedelta
    from croniter import croniter
    from airflow.models import Variable
    from pathlib import Path
    import json
    from airflow.hooks.base import BaseHook
    from os.path import abspath
    import pendulum
    from airflow.sensors.external_task_sensor  import ExternalTaskSensor, ExternalTaskMarker
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    import psycopg2
    from pyspark.sql import SparkSession    
except Exception as e:
    error_class = e.__class__.__name__  #get error type
    detail = e.args[0]
    cl, exc, tb = sys.exc_info()  # get call stack
    lastCallStack = traceback.extract_tb(tb)[-1]  # get last record from call stack
    fileName = lastCallStack[0]
    lineNum = lastCallStack[1]
    funcName = lastCallStack[2]
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(
        fileName, lineNum, funcName, error_class, detail)
    print(errMsg)


# SETUP

# timezone
local_tz = pendulum.timezone("Asia/Taipei")

# set thi root dir for airflow to fetch files
BASE_DIR = Path(__file__).resolve().parent.parent

# get airflow variables
sqljdbc_jar = json.loads(Variable.get("sqljdbc_jar"))

# hive host connection
hive_host_conn = BaseHook.get_connection("hive_host_conn")
hive_host_conn_extra = json.loads(hive_host_conn.extra)
hive_db = hive_host_conn_extra["hive_db"]

# hive metastore connection
hive_metastore_conn = BaseHook.get_connection("hive_metastore_conn")
hive_metastore = f"{hive_metastore_conn.host}:{hive_metastore_conn.port}"
hive_warehouse = abspath(f"hdfs://{hive_metastore_conn.host}/uetl_sa/etl/etl_sa/etl_sa/")



# check run time match 
def filter_execution_date(cron_text, **kwargs):
    print(cron_text)
    print(kwargs['execution_date'])
    execution_date = datetime.fromtimestamp(kwargs['execution_date'].timestamp()) #,tz=timezone.utc)
    print(execution_date)
    return croniter.match(cron_text, execution_date)

def skipcondition(cron_text, **kwargs):
    execution_date = datetime.fromtimestamp(kwargs['execution_date'].timestamp())
    first_valid_friday = cron_text
    return (execution_date - first_valid_friday).days % 14 == 0


# START ETL_PIPELINE


args = {
    'owner': '數位金融處',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'OHR-Vendor031@sinopac.com',
    'wait_for_downstream':True,
    'concurrency': 6,
    'catchup': True,
}
with DAG(
        dag_id='標籤模型分析',
        default_args=args,
        schedule_interval='0 18 * * *',
        start_date=datetime(2022, 4, 1, tzinfo=local_tz),
        end_date=datetime(2022, 4, 4, tzinfo=local_tz),
        tags=['產品標籤分析模型'],
) as dag:
    start_task = DummyOperator(
        task_id='start_task',
    )

    # dmp_mssql_db_connecion
    dmp_mssql_server_conn = BaseHook.get_connection("dmp_mssql_server_conn")
    dmp_mssql_server = f"{dmp_mssql_server_conn.host}:{dmp_mssql_server_conn.port}"

    # each subtask run time
    cron_text = '00 18 * * *'
       
    # check if a task needs to be run
    execute_checker = ShortCircuitOperator(
            task_id=f"execute_checker_for_run",
            python_callable=skipcondition,
            provide_context=True,
            op_kwargs={
                'cron_text': datetime(2022, 4, 1)
            }
        )
    
    # set spark BashOperator task
    etl_exec_HQL = BashOperator(
        task_id=f"run_analysis_product_label_all",
        bash_command=f"spark-submit "
                f"--driver-class-path {sqljdbc_jar} "
                f"--jars {sqljdbc_jar} "
                f"--executor-memory 32G "
                f"--driver-memory 64G "
                f"--conf spark.driver.maxResultSize=0 "
                f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                f"--conf spark.debug.maxToStringFields=350 "
                f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                f"--conf spark.ui.enabled=false "
                f"{BASE_DIR}/tools/dmp/dmp_process.py "
                f"--hql_template dmp_process_select_raw_data.hql "
                f"--hive_metastore {hive_metastore} "
                f"--hive_warehouse {hive_warehouse} "
                f"--sql_server {dmp_mssql_server} "
                f"--sql_username {dmp_mssql_server_conn.login} "
                f"--sql_password {dmp_mssql_server_conn.password}  "
                f"--sql_db FZSRD_AIRFLOW "
                f"--sql_table user_product_event "
                "--execution_date {{ds}} ",
    )
        
    # set ETL SOP
    start_task >>  execute_checker >> etl_exec_HQL 
        
if __name__ == "__main__":
    dag.cli()