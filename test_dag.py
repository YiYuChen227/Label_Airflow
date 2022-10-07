try:
    from airflow import DAG, macros
    from airflow.operators.bash import BashOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.dates import days_ago
    from airflow.operators.python import ShortCircuitOperator
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    from datetime import timedelta, timezone, datetime
    from croniter import croniter
    from pathlib import Path
    import json
    from os.path import abspath
    import pendulum
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

# SETUP
# timezone
local_tz = pendulum.timezone("Asia/Taipei")

# set the root dir for airflow to fetch file
BASE_DIR = Path(__file__).resolve().parent.parent

# get airflow variables
target_tables = json.loads(Variable.get("202203data_yyyymm_pattern_tables"))
sqljdbc_jar = json.loads(Variable.get("sqljdbc_jar"))

# hive host connection
hive_host_conn = BaseHook.get_connection("hive_host_conn")
hive_host_conn_extra = json.loads(hive_host_conn.extra)
hive_db = hive_host_conn_extra["hive_db"]

# hive metastore connection
hive_metastore_conn = BaseHook.get_connection("hive_metastore_conn")
hive_metastore = f"{hive_metastore_conn.host}:{hive_metastore_conn.port}"
hive_warehouse = abspath(f"hdfs://{hive_metastore_conn.host}/user/hive/warehouse/")

# mssql server connection
mssql_server_conn = BaseHook.get_connection("mssql_server_2016_conn")
mssql_server = f"{mssql_server_conn.host}:{mssql_server_conn.port}"

# sql server 2016 connection
# mssql_server_2016_conn = BaseHook.get_connection("mssql_server_2016_conn")
# mssql_server_2016= f"{mssql_server_2016_conn.host}:{mssql_server_2016_conn.port}"

# start_date: 進入排程（DAG尚未被執行）
# - schedule_interval: 排程間隔。如：daily為每日執行。
# - execution_date: Airflow開始將參數代入(process 
# data)，DAG尚未被執行。


# start date and end date
execution_date = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"   ##


def filter_execution_date(cron_text, **kwargs):
    print(cron_text)
    print(kwargs['execution_date'])
    execution_date = datetime.fromtimestamp(kwargs['execution_date'].timestamp())
    print(execution_date)
    print(type(execution_date))
    return croniter.match(cron_text, execution_date)


# START ETL_PIPELINE
args = {
    'owner': '數位金融處',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),  ##下一次retry會在五分鐘後開始
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'OHR-Vendor031@sinopac.com',
    'wait_for_downstream': True,
    'concurrency': 6,
}

with DAG(
        dag_id='test_oliver_dag',
        default_args=args,
        schedule_interval='* * * * 5',
        start_date=datetime(2022, 10, 6, 0, 0, tzinfo=local_tz),
        #start_date=datetime(2022, 2, 21, 0, 0, tzinfo=local_tz),
        dagrun_timeout=timedelta(minutes=300),
        # end_date=datetime(2021, 11, 1, 0, 0, tzinfo=local_tz),
        tags=['Oliver 測試用'],
) as dag:
    start_task = DummyOperator(
        task_id='start_task',
    )
    # create tasks dynamically
    for target_table in target_tables:
        hive_db = target_table['hive_db']
        sql_db = target_table['sql_db']
        table = target_table['table']
        cron_text = target_table['crontab_update_frequency']
        hive_table = f"{sql_db}_{table}"
        sql_table = table
        key_to_new_data = target_table['key_to_new_data']
        pattern_type = target_table['pattern_type']
        executor_memory_create_table = target_table.get('executor_memory_create_table', '3g')
        driver_memory_create_table = target_table.get('driver_memory_create_table', '2g')
        executor_memory_import_data = target_table.get('executor_memory_import_data', '6g')
        driver_memory_import_data = target_table.get('driver_memory_import_data', '3g')

        # check if a task needs to be run
        # execute_checker = ShortCircuitOperator(
        #     task_id=f"execute_checker_for_{hive_table}",
        #     python_callable=filter_execution_date,
        #     provide_context=True,
        #     op_kwargs={
        #         'cron_text': cron_text
        #     }
        # )

        etl_gen_ddl = BashOperator(
            task_id=f"gen_ddl_{hive_table}",
            bash_command=f"spark-submit "
                         f"--driver-class-path {sqljdbc_jar} "
                         f"--jars {sqljdbc_jar} "
                         f"--executor-memory {executor_memory_import_data} "
                         f"--driver-memory {driver_memory_import_data} "
                         f"--conf spark.ui.enabled=false "
                         f"--conf spark.yarn.keytab=/opt/airflow/airflow.keytab "
                         f"--conf spark.yarn.principal=airflow@ITRD.RD "
                         f"{BASE_DIR}/tools/dmp_tag/test_oliver_label.py " ## 要跑的 python code name 寫在這裡
                         f"--mssql_jdbc_jar {sqljdbc_jar} "
                         f"--sql_server {mssql_server} "
                         f"--sql_username {mssql_server_conn.login} "
                         f"--sql_password {mssql_server_conn.password} "
                         f"--sql_db {sql_db} "
                         f"--sql_table {table} "
                         f"--hive_metastore {hive_metastore} "
                         f"--hive_warehouse {hive_warehouse} "
                         f"--hive_db {hive_db}"
                         f"--hive_table {hive_table} "
                         f"--key_to_new_data {key_to_new_data} "
                         f"--execution_date {execution_date} "
                         f"--pattern_type {pattern_type}",
        )


        start_task  >> etl_gen_ddl 
        #start_task >> execute_checker >> etl_create_table >> etl_import_data

if __name__ == "__main__":
    dag.cli()
