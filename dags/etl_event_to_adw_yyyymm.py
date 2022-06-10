try: 
    from airflow import DAG, macros
    from airflow.operators.bash import BashOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.dates import days_ago
    from airflow.operators.python import ShortCircuitOperator
    from datetime import datetime,timezone, timedelta
    import dateutil.relativedelta
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
    cl, exc, tb = sys.exc_info()  #get call stack
    lastCallStack = traceback.extract_tb(tb)[-1]  #get last record from call stack
    fileName = lastCallStack[0]
    lineNum = lastCallStack[1]
    funcName = lastCallStack[2]
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(
        fileName, lineNum, funcName, error_class, detail)
    print(errMsg)


# SETUP

ftp_upload_server = BaseHook.get_connection("ftp_upload_server")
ftp_upload_username = ftp_upload_server.login
ftp_upload_password = ftp_upload_server.password
ftp_upload_ip = ftp_upload_server.host
#ftp_upload_folder = Variable.get('ftp_upload_folder')

#timezone
local_tz = pendulum.timezone("Asia/Taipei")

# set thi root dir for airflow to fetch files
BASE_DIR = Path(__file__).resolve().parent.parent

# get airflow variables
#target_tables = json.loads(Variable.get("DMP_variables")) 
sqljdbc_jar = json.loads(Variable.get("sqljdbc_jar"))

# hive host connection
hive_host_conn = BaseHook.get_connection("hive_host_conn")
hive_host_conn_extra = json.loads(hive_host_conn.extra)
hive_db = hive_host_conn_extra["hive_db"]

# hive metastore connection
hive_metastore_conn = BaseHook.get_connection("hive_metastore_conn")
hive_metastore = f"{hive_metastore_conn.host}:{hive_metastore_conn.port}"
hive_warehouse = abspath(f"hdfs://{hive_metastore_conn.host}/uetl_sa/etl/etl_sa/etl_sa/")

# mssql server connection
#mssql_server_conn = BaseHook.get_connection("dmp_mssql_server_conn")
#mssql_server = f"{mssql_server_conn.host}:{mssql_server_conn.port}"

# get execution date
execution_date = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"


#dmp_mssql_db_connecion
dmp_mssql_server_conn = BaseHook.get_connection("dmp_mssql_server_conn")
dmp_mssql_server = f"{dmp_mssql_server_conn.host}:{dmp_mssql_server_conn.port}"


# START ETL_PIPELINE
args = {
    'owner': '綜合企劃處',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'bill.wang@mail.itrd',
    'wait_for_downstream':True,
    'concurrency': 1,
    'catchup': True,
}

with DAG(
        dag_id='DMP_Event_資料表每日導入ADW',
        description='event etl to adw pipeline',
        default_args=args,
        schedule_interval='0 10 * * *',
        start_date=datetime(2022, 1, 8, tzinfo=local_tz),
        #dagrun_timeout=timedelta(minutes=300),
        #end_date=datetime(2022, 2, 8, tzinfo=local_tz),
        tags=['ETL_EVENT_TO_ADW_YYYYMM'],
) as dag:
    
    start_task = DummyOperator(
        task_id='start_task',
    )
    import_to_mssql = BashOperator(
        task_id=f"read_hive_hql_and_import_to_Mssql",
        bash_command=f"spark-submit "
                     f"--driver-class-path {sqljdbc_jar} "
                     f"--jars {sqljdbc_jar} "
                     f"--executor-memory 8g "
                     f"--driver-memory 8g "
                     f"--conf spark.ui.enabled=false "
                     f"{BASE_DIR}/tools/dmp/dmp_read_hive_to_mssql.py "
                     f"--mssql_jdbc_jar {sqljdbc_jar} "
                     f"--sql_server {dmp_mssql_server} "
                     f"--sql_username {dmp_mssql_server_conn.login} "
                     f"--sql_password {dmp_mssql_server_conn.password}  "
                     f"--sql_db  fz_DMP_Raw "
                     f"--run_type dmp_event_daily "                     
                     f"--sql_table event_yyyymm "
                     f"--hive_metastore {hive_metastore} "
                     f"--hive_warehouse {hive_warehouse} "
                     "--execution_date {{ds}} ",
    )

    start_task >> import_to_mssql
      
if __name__ == "__main__":
    dag.cli()
