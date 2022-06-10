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
        dag_id='Level2_興趣標籤導入ADW',
        default_args=args,
        schedule_interval='0 18 * * *',
        start_date=datetime(2022, 3, 1, tzinfo=local_tz),
        end_date=datetime(2022, 3, 2, tzinfo=local_tz),
        tags=['產品標籤分析模型','test'],
) as dag:
    start_task = DummyOperator(
        task_id='開始執行',
    )
    end_task = DummyOperator(
            task_id='匯入MSSQL',
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
                'cron_text': datetime(2022, 3,1)
            }
        )
    refresh_interest_map = BashOperator(
        task_id=f"refresh_hadoop_dmp_rule_map2",
        bash_command=f"spark-submit "
                     f"--driver-class-path {sqljdbc_jar} "
                     f"--jars {sqljdbc_jar} "
                     f"--executor-memory 1g "
                     f"--driver-memory 1g "
                     f"--conf spark.ui.enabled=false "
                     f"{BASE_DIR}/tools/dmp/read_interest_table_to_hive.py "
                     f"--mssql_jdbc_jar {sqljdbc_jar} "
                     f"--sql_server 10.11.48.12 "
                     f"--sql_username AIRFLOW_USER "
                     f"--sql_password 2wsx@WSX "
                     f"--sql_db FZSRD_AIRFLOW "
                     f"--sql_table dmp.dmp_table_info "
                     f"--hive_metastore {hive_metastore} "
                     f"--hive_warehouse {hive_warehouse} "
                     f"--hive_db dm_tag "
                     f"--hive_table dmp_rule_map2 "
                     #f"--pattern_type test ",
    )

    # set spark BashOperator task '模板-平台'    
    for hql_templat_name in['模板-平台']:
        etl_exec_HQL = BashOperator(
            task_id=f"執行{hql_templat_name}",
            bash_command=f"spark-submit "
                    f"--driver-class-path {sqljdbc_jar} "
                    f"--jars {sqljdbc_jar} "
                    f"--executor-memory 8G "
                    f"--driver-memory 8G "
                    f"--conf spark.driver.maxResultSize=0 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                    f"--conf spark.debug.maxToStringFields=350 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                    f"--conf spark.ui.enabled=false "
                    f"--conf spark.sql.hive.filesourcePartitionFileCacheSize=4621440000 "
                    f"{BASE_DIR}/tools/dmp/dmp_run_level2_type1.py "
                    f"--hql_template {hql_templat_name}.hql "
                    f"--hive_metastore {hive_metastore} "
                    f"--hive_warehouse {hive_warehouse} "
                    f"--sql_server 10.11.48.12 "
                    f"--sql_username AIRFLOW_USER "
                    f"--sql_password 2wsx@WSX "
                    f"--sql_db FZSRD_AIRFLOW "
                    f"--sql_table {hql_templat_name} "
                    "--execution_date {{ds}} ",
            )
    for hql_templat_name in['模板-產品與功能']:
        etl_exec_HQL_type2 = BashOperator(
            task_id=f"執行{hql_templat_name}",
            bash_command=f"spark-submit "
                    f"--driver-class-path {sqljdbc_jar} "
                    f"--jars {sqljdbc_jar} "
                    f"--executor-memory 8G "
                    f"--driver-memory  8G "
                    f"--conf spark.driver.maxResultSize=0 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                    f"--conf spark.debug.maxToStringFields=350 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                    f"--conf spark.ui.enabled=false "
                    f"--conf spark.sql.hive.filesourcePartitionFileCacheSize=4621440000 "
                    f"{BASE_DIR}/tools/dmp/dmp_run_level2_type2.py "
                    f"--hql_template {hql_templat_name}.hql "
                    f"--hive_metastore {hive_metastore} "
                    f"--hive_warehouse {hive_warehouse} "
                    f"--sql_server 10.11.48.12 "
                    f"--sql_username AIRFLOW_USER "
                    f"--sql_password 2wsx@WSX "
                    f"--sql_db FZSRD_AIRFLOW "
                    f"--sql_table {hql_templat_name} "
                    "--execution_date {{ds}} ",
            )


    for hql_templat_name in['模板-尊榮理財_行銷活動及信用卡登錄_活動次數']:
        etl_exec_HQL_type3 = BashOperator(
            task_id=f"執行{hql_templat_name}",
            bash_command=f"spark-submit "
                    f"--driver-class-path {sqljdbc_jar} "
                    f"--jars {sqljdbc_jar} "
                    f"--executor-memory 8G "
                    f"--driver-memory 8G "
                    f"--conf spark.driver.maxResultSize=0 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                    f"--conf spark.debug.maxToStringFields=350 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                    f"--conf spark.ui.enabled=false "
                    f"--conf spark.sql.hive.filesourcePartitionFileCacheSize=4621440000 "
                    f"{BASE_DIR}/tools/dmp/dmp_run_level2_type3.py "
                    f"--hql_template {hql_templat_name}.hql "
                    f"--hive_metastore {hive_metastore} "
                    f"--hive_warehouse {hive_warehouse} "
                    f"--sql_server 10.11.48.12 "
                    f"--sql_username AIRFLOW_USER "
                    f"--sql_password 2wsx@WSX "
                    f"--sql_db FZSRD_AIRFLOW "
                    f"--sql_table {hql_templat_name} "
                    "--execution_date {{ds}} ",
            )
    for hql_templat_name in['模板-薪轉戶_活動']:
        etl_exec_HQL_type4 = BashOperator(
            task_id=f"執行{hql_templat_name}",
            bash_command=f"spark-submit "
                    f"--driver-class-path {sqljdbc_jar} "
                    f"--jars {sqljdbc_jar} "
                    f"--executor-memory 8G "
                    f"--driver-memory 8G "
                    f"--conf spark.driver.maxResultSize=0 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                    f"--conf spark.debug.maxToStringFields=350 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                    f"--conf spark.ui.enabled=false "
                    f"--conf spark.sql.hive.filesourcePartitionFileCacheSize=4621440000 "
                    f"{BASE_DIR}/tools/dmp/dmp_run_level2_type4.py "
                    f"--hql_template {hql_templat_name}.hql "
                    f"--hive_metastore {hive_metastore} "
                    f"--hive_warehouse {hive_warehouse} "
                    f"--sql_server 10.11.48.12 "
                    f"--sql_username AIRFLOW_USER "
                    f"--sql_password 2wsx@WSX "
                    f"--sql_db FZSRD_AIRFLOW "
                    f"--sql_table {hql_templat_name} "
                    "--execution_date {{ds}} ",
            )

    for hql_templat_name in['模板-大戶開戶活動']:
        etl_exec_HQL_type5 = BashOperator(
            task_id=f"執行{hql_templat_name}",
            bash_command=f"spark-submit "
                    f"--driver-class-path {sqljdbc_jar} "
                    f"--jars {sqljdbc_jar} "
                    f"--executor-memory 8G "
                    f"--driver-memory 8G "
                    f"--conf spark.driver.maxResultSize=0 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition=true "
                    f"--conf spark.debug.maxToStringFields=350 "
                    f"--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict "
                    f"--conf spark.ui.enabled=false "
                    f"--conf spark.sql.hive.filesourcePartitionFileCacheSize=4621440000 "
                    f"{BASE_DIR}/tools/dmp/dmp_run_level2_type5.py "
                    f"--hql_template {hql_templat_name}.hql "
                    f"--hive_metastore {hive_metastore} "
                    f"--hive_warehouse {hive_warehouse} "
                    f"--sql_server 10.11.48.12 "
                    f"--sql_username AIRFLOW_USER "
                    f"--sql_password 2wsx@WSX "
                    f"--sql_db FZSRD_AIRFLOW "
                    f"--sql_table {hql_templat_name} "
                    "--execution_date {{ds}} ",
            )
        refresh_interest_map  >> etl_exec_HQL >> end_task
        refresh_interest_map  >> etl_exec_HQL_type2 >> end_task
        refresh_interest_map  >> etl_exec_HQL_type3 >> end_task
        refresh_interest_map  >> etl_exec_HQL_type4 >> end_task
        refresh_interest_map  >> etl_exec_HQL_type5 >> end_task





    # set ETL SOP
    start_task >>  execute_checker >> refresh_interest_map  >> etl_exec_HQL >> end_task
        
if __name__ == "__main__":
    dag.cli()