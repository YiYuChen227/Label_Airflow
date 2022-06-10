import os
import json
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, macros
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from dateutil.relativedelta import *

# variable
variable = json.loads(Variable.get("appier_etl"))

appier_date = "{{ macros.ds_format(macros.ds_add(ds, -0), '%Y-%m-%d', '%Y%m%d') }}"

ftp_download_server = BaseHook.get_connection("ftp_download_server")
ftp_download_username = ftp_download_server.login
ftp_download_password = ftp_download_server.password
ftp_download_ip = ftp_download_server.host
ftp_download_folder = variable.get('ftp_download_folder')
ftp_download_local_folder = variable.get('ftp_download_local_folder')

ftp_upload_server = BaseHook.get_connection("ftp_upload_server")
ftp_upload_username = ftp_upload_server.login
ftp_upload_password = ftp_upload_server.password
ftp_upload_ip = ftp_upload_server.host
ftp_upload_folder = variable.get('ftp_upload_folder')

chunksize = variable.get('chunksize')
hdfs_pre_upload_local_path = variable.get('hdfs_pre_upload_local_path')
hdfs_tmp_path = variable.get('hdfs_tmp_path')

hive_host_conn = BaseHook.get_connection("hive_kerberos_conn")
hive_jdbc_uri = f"jdbc:hive2://{hive_host_conn.host}:{hive_host_conn.port}/;principal={hive_host_conn.extra}"
hql_local_path = variable.get('hql_local_path')

hive_tmp_input_database = variable.get('hive_tmp_database')
hive_pro_database = variable.get('hive_pro_database')

hive_tmp_output_database = variable.get('hive_tmp_output_database')

output_csv_folder = variable.get('output_csv_folder')
output_tsv_folder = variable.get('output_tsv_folder')

mssql_path = variable.get('mssql_path')
mssql_server_conn = BaseHook.get_connection("appier_mssql_server_conn")
mssql_uri = mssql_server_conn.host
mssql_user = mssql_server_conn.login
mssql_password = mssql_server_conn.password
tmp_mssql_database = variable.get('tmp_mssql_database')
pro_mssql_database = variable.get('pro_mssql_database')
tmp_mssql_schema = variable.get('tmp_mssql_schema')
pro_mssql_schema = variable.get('pro_mssql_schema')

python_script_path = variable.get('python_script_path')
shell_script_path = variable.get('shell_script_path')

local_tz = pendulum.timezone('Asia/Taipei')

default_args = {
    'owner': '數位金融處',
    'depends_on_past': False,
    'start_date': datetime(2021,12,24, tzinfo=local_tz), 
    #'end_date': datetime(2021,12,25, tzinfo=local_tz), 
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}


dag = DAG(
    dag_id='appier_dag',
    description='test appier etl pipeline',
    default_args=default_args,
    schedule_interval='0 12 * * 5'
)


for execute_type in ['dawho', 'sinopac']:
    # variable depend by execute type
    name_rule = variable.get(f'{execute_type}_name_rule')
    hive_pro_table_name = f'appier_{execute_type}'
    hive_tmp_input_table_name = f'appier_{execute_type}_tmp'
    mssql_pro_table_name = f'appier_{execute_type}'
    mssql_tmp_table_name = f'appier_{execute_type}_tmp'
    hdfs_pre_upload_local_file_path = f"{hdfs_pre_upload_local_path}/{execute_type}.tsv"
    execute_week=variable.get(f"{execute_type}_execute_week")

    download_ftp_file_to_local = BashOperator(
        task_id=f'{execute_type}_download_ftp_file_to_local',
        bash_command=f"""sh {shell_script_path}/test_dump_ftp_data.sh {ftp_download_local_folder} {ftp_download_username} {ftp_download_password} {ftp_download_ip} {ftp_download_folder} {name_rule} {appier_date} {execute_week}""",
        dag=dag
    )


    csv_transfer = BashOperator(
        task_id=f'{execute_type}_csv_transfer',
        bash_command=f"""sh {shell_script_path}/csv_transfer.sh {appier_date} {name_rule} {ftp_download_local_folder} {hdfs_pre_upload_local_file_path} {chunksize}""",
        dag=dag
    )

    upload_to_hive_external_table = BashOperator(
        task_id=f'{execute_type}_upload_to_hive_external_table',
        bash_command=f'/usr/hadoop-2.6.1/bin/hdfs dfs -put -f {hdfs_pre_upload_local_file_path} {hdfs_tmp_path}/{execute_type} && rm {hdfs_pre_upload_local_file_path}',
        dag=dag
    )

    move_to_hive_pro_table = BashOperator(
        task_id=f'{execute_type}_move_to_hive_pro_table',
        bash_command=f"""sh {shell_script_path}/update_pro_hive_table.sh \
                            "{hive_jdbc_uri}" \
                            {hql_local_path} \
                            {hive_tmp_input_database} \
                            {hive_tmp_input_table_name} \
                            {hive_pro_database} \
                            {hive_pro_table_name} \
                            {appier_date}
        """,
        dag=dag
    )

    # 因數金告知不需跑，先註解
    
    '''

    truncate_mssql_tmp_table = BashOperator(
        task_id=f'{execute_type}_truncate_mssql_tmp_table',
        bash_command=f"""sh {shell_script_path}/truncate_tmp_mssql_table.sh \
                            {python_script_path} \
                            {mssql_path} \
                            {tmp_mssql_database} \
                            {tmp_mssql_schema} \
                            {mssql_tmp_table_name} \
                            {mssql_uri} \
                            {mssql_user} \
                            {mssql_password}
        """,
        dag=dag
    )

    dump_hive_data_to_tsv = BashOperator(
        task_id=f'{execute_type}_dump_hive_data_to_tsv',
        bash_command=f"""sh {shell_script_path}/dump_hive_to_tsv.sh \
                            "{hive_jdbc_uri}" \
                            "{hive_pro_database}" \
                            "{hive_pro_table_name}" \
                            "{appier_date}" \
                            "{output_tsv_folder}/{execute_type}_{appier_date}.tsv"

        """,
        dag=dag
    )

    upload_to_mssql_tmp_table = BashOperator(
        task_id=f'{execute_type}_upload_to_mssql_tmp_table',
        bash_command=f"""sh {shell_script_path}/upload_to_mssql.sh \
                            {tmp_mssql_database} \
                            {tmp_mssql_schema} \
                            {mssql_tmp_table_name} \
                            {output_tsv_folder}/{execute_type}_{appier_date}.tsv \
                            {mssql_uri} \
                            {mssql_user} \
                            {mssql_password} && rm {output_tsv_folder}/{execute_type}_{appier_date}.tsv
        """,
        dag=dag
    )

    move_to_mssql_pro_table = BashOperator(
        task_id=f'{execute_type}_move_to_mssql_pro_table',
        bash_command=f"""sh {shell_script_path}/update_pro_mssql_table.sh \
                            {tmp_mssql_database} \
                            {tmp_mssql_schema} \
                            {mssql_tmp_table_name} \
                            {pro_mssql_database} \
                            {pro_mssql_schema} \
                            {mssql_pro_table_name} \
                            {mssql_uri} \
                            {mssql_user} \
                            {mssql_password}
        """,
        dag=dag
    )

    dump_hive_data_to_csv = BashOperator(
        task_id=f'{execute_type}_dump_hive_data_to_csv',
        bash_command=f"""sh {shell_script_path}/dump_hive_to_csv.sh "{hive_jdbc_uri}" "{hive_pro_database}" "{hive_pro_table_name}" "{appier_date}" "{output_csv_folder}/appier_{execute_type}_{appier_date}.csv"
	""",
        dag=dag
    )

    upload_csv_to_ftp = BashOperator(
        task_id=f'{execute_type}_upload_csv_to_ftp',
        bash_command=f"""ncftpput -u {ftp_upload_username} -p {ftp_upload_password} {ftp_upload_ip} {ftp_upload_folder} {output_csv_folder}/appier_{execute_type}_{appier_date}.csv && rm {output_csv_folder}/appier_{execute_type}_{appier_date}.csv""",
        dag=dag
    )
    '''

    # ftp -> hive
    download_ftp_file_to_local >> csv_transfer >> upload_to_hive_external_table >> move_to_hive_pro_table 
    # hive -> mssql
    #move_to_hive_pro_table >> truncate_mssql_tmp_table >> dump_hive_data_to_tsv >> upload_to_mssql_tmp_table  >> move_to_mssql_pro_table
    # hive -> ftp
    #move_to_hive_pro_table >> dump_hive_data_to_csv >> upload_csv_to_ftp 

