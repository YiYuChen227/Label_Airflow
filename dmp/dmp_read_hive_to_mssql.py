from pyspark.sql import SparkSession
from os.path import abspath
import click
import pathlib
from datetime import datetime,timedelta
from airflow.hooks.base import BaseHook
import psycopg2
from pandas import DataFrame
import os

@click.command()
@click.option('--execution_date', required=True)
@click.option('--mssql_jdbc_jar', required=True)
@click.option('--sql_server', required=True)
@click.option('--sql_username', required=True)
@click.option('--sql_password', required=True)
@click.option('--sql_db', required=True)
@click.option('--sql_table', required=True)
@click.option('--hive_warehouse', required=True)
@click.option('--hive_metastore', required=True)
@click.option('--execution_date', required=True)
@click.option('--run_type', required=True)

def run(hive_warehouse,hive_metastore,execution_date,mssql_jdbc_jar,sql_server,sql_username,sql_password,sql_db,sql_table,run_type):

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Insert_Data") \
        .config("spark.driver.extraClassPath", mssql_jdbc_jar) \
        .config("spark.executor.extraClassPath", mssql_jdbc_jar) \
        .config("hive.metastore.uris", f"thrift://{hive_metastore}") \
        .config("spark.sql.warehouse.dir", hive_warehouse) \
        .config("hive.warehouse.subdir.inherit.perms","false")\
        .enableHiveSupport() \
        .getOrCreate()

    # get hive data (hadoop)    
    spark.conf.set('spark.sql.session.timeZone', 'UTC+8')
    spark.conf.set("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY")
    spark.sparkContext.setLogLevel('WARN')
    hql_cmd = f"/opt/airflow/tools/dmp/run_type/" + run_type +".hql"
    tmpbaseTime=datetime.strptime(execution_date, "%Y-%m-%d")+timedelta(days=-7)  
    baseTime =str(tmpbaseTime.year)+ "-"+ str(tmpbaseTime.month).zfill(2) + "-" + str(tmpbaseTime.day).zfill(2)
    print(baseTime)
    sql_table=sql_table.replace('yyyymm', str(tmpbaseTime.year)+ str(tmpbaseTime.month).zfill(2))
    print(sql_table)

    df_with_duplicates=None
    df_without_duplicates=None
    tm='(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{3})'

    with open(hql_cmd) as f:
        line=f.read()      
        line=line.format(tm=tm,baseTime=baseTime,execution_date=execution_date)
        print(line)
        df_with_duplicates= spark.sql(line)     

    print('df_with_duplicates Count: '+str(len(df_with_duplicates.collect())))
    df_without_duplicates = df_with_duplicates.drop_duplicates(subset=["distinct_id","dt","tm","event_duration","title","screen_name","url","url_path","referrer","element_id","element_name","element_type","element_content","element_position","element_selector","element_target_url","element_class_name","city","province","utm_source","utm_medium","utm_term","utm_content","utm_campaign","manufacturer","model","os","os_version","platform","product","event"])
    print('df_without_duplicates Count: '+str(len(df_without_duplicates.collect()))) 
    df_without_duplicates.show()
    data_import_mssql(sql_server,sql_db,sql_username,sql_password,sql_table,baseTime,df_without_duplicates)

    spark.stop()
    
def data_import_mssql(sql_server,sql_db,sql_username,sql_password,sql_table,execution_date,hive_source):
    sqlUrl = f"jdbc:sqlserver://{sql_server};databaseName={sql_db}"
    sql_server_ip = sql_server.split(':')[0]
    sql_file = f"/opt/airflow/schema/mssql/create_table_event_yyyymm.sql"
    sql_cmd=''
    dt=execution_date
    with open(sql_file) as f:
        sql_cmd=f.read()      
        sql_cmd = sql_cmd.format(sql_db=sql_db,sql_table=sql_table,dt=dt)                   
    

    #create table delete insert 
    os.system(f'mssql-cli -U {sql_username} -P {sql_password} -d {sql_db} -S {sql_server_ip} -Q "{sql_cmd}"')
    print(sql_cmd)
   
    hive_source.write.format("jdbc") \
        .option("url", sqlUrl) \
        .option("dbtable",sql_table)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("user",sql_username)\
        .option("password",sql_password )\
        .mode('append').save()

if __name__=='__main__':
    run()
