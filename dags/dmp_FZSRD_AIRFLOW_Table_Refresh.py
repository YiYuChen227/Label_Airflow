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

#get_postgres_connection
postgres_server_conn = BaseHook.get_connection("postgres")
dbname="airflow"
conn_string = "host={0} user={1} dbname={2} password={3}".format(postgres_server_conn.host,postgres_server_conn.login, dbname,postgres_server_conn.login)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
cursor.execute("select  * from dmp.dmp_dags_list")
target_tables = cursor.fetchall()
print("Connection established")

#dmp_mssql_db_connecion
dmp_mssql_server_conn = BaseHook.get_connection("dmp_mssql_server_conn")
dmp_mssql_server = f"{dmp_mssql_server_conn.host}:{dmp_mssql_server_conn.port}"


def filter_execution_date(cron_text, **kwargs):
    print(cron_text)
    print(kwargs['execution_date'])
    execution_date = datetime.fromtimestamp(kwargs['execution_date'].timestamp()) #,tz=timezone.utc)
    print(execution_date)
    return croniter.match(cron_text, execution_date)

# START ETL_PIPELINE
args = {
    'owner': '數位金融處',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'OHR-Vendor031@sinopac.com',
    'wait_for_downstream':True,
    'concurrency': 6,
    'catchup': True,
}

with DAG(
        dag_id='FZSRD_AIRFLOW_Table_Refresh',
        default_args=args,
        schedule_interval='0 18 * * *',
        start_date=datetime(2022, 1, 1, tzinfo=local_tz),
        #dagrun_timeout=timedelta(minutes=300),
        #end_date=datetime(2021, 7, 31, tzinfo=local_tz),
        tags=['產品標籤分析模型'],
) as dag:
    start_task = DummyOperator(
        task_id='start_task',
    )
    refresh_rule_map = BashOperator(
        task_id=f"refresh_rule_map_table",
        bash_command=f"spark-submit "
                     f"--driver-class-path {sqljdbc_jar} "
                     f"--jars {sqljdbc_jar} "
                     f"--executor-memory 1g "
                     f"--driver-memory 1g "
                     f"--conf spark.ui.enabled=false "
                     f"{BASE_DIR}/tools/dmp/dmp_spark_import.py "
                     f"--mssql_jdbc_jar {sqljdbc_jar} "
                     f"--sql_server {dmp_mssql_server} "
                     f"--sql_username {dmp_mssql_server_conn.login} "
                     f"--sql_password {dmp_mssql_server_conn.password}  "
                     f"--sql_db FZSRD_AIRFLOW "
                     f"--sql_table dmp.dmp_table_info "
                     f"--postgres_host {postgres_server_conn.host} "
                     f"--postgres_db  airflow "
                     f"--postgres_table_name dmp.dmp_table_info "
                     f"--postgres_login {postgres_server_conn.login} "
                     f"--postgres_pwd {postgres_server_conn.password} "
                     f"--hive_metastore {hive_metastore} "
                     f"--hive_warehouse {hive_warehouse} "
                     f"--hive_db dm_tag "
                     f"--hive_table dmp_rule_map "
                     f"--execution_date {execution_date} ",
                     #f"--pattern_type test ",
    )
       
    start_task >> refresh_rule_map
      
if __name__ == "__main__":
    dag.cli()
