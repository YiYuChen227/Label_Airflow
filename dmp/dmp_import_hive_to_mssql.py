from pyspark.sql import SparkSession
from os.path import abspath
import click
import pathlib
from datetime import datetime
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
@click.option('--hive_db', required=True)
@click.option('--hive_table', required=True)
@click.option('--execution_date', required=True)

def run(hive_warehouse,hive_metastore,execution_date,mssql_jdbc_jar,sql_server,sql_username,sql_password,sql_db,sql_table,hive_db,hive_table):

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Insert_Data") \
        .config("spark.driver.extraClassPath", mssql_jdbc_jar) \
        .config("spark.executor.extraClassPath", mssql_jdbc_jar) \
        .config("hive.metastore.uris", f"thrift://{hive_metastore}") \
        .config("spark.sql.warehouse.dir", hive_warehouse) \
        .enableHiveSupport() \
        .getOrCreate()

    # get hive data (hadoop)
    sql_cmd  = f"SELECT * FROM {hive_db}.{hive_table} LIMIT 10"

    done_data = spark.sql(sql_cmd)

    print('Count: '+str(len(done_data.collect())))  
    print(done_data.collect())
    data_import_mssql(sql_server,sql_db,sql_username,sql_password,sql_table,execution_date,done_data)

    spark.stop()
    
def data_import_mssql(sql_server,sql_db,sql_username,sql_password,sql_table,execution_date,hive_source):
    sqlUrl = f"jdbc:sqlserver://{sql_server};databaseName={sql_db}"
    sql_server_ip = sql_server.split(':')[0]

    print(sql_table)

    #delete insert 
    #os.system(f'mssql-cli -U {sql_username} -P {sql_password} -d {sql_db} -S {sql_server_ip} -Q "delete from {sql_table} where CAST(execution_date as date) like \'{execution_date}\' and dag_id=\'{sql_table}\'"')
 
    hive_source.write.format("jdbc") \
       .option("url", sqlUrl) \
       .option("dbtable",sql_table)\
       .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
       .option("user",sql_username)\
       .option("password",sql_password )\
       .mode('overwrite').save()

if __name__=='__main__':
    run()
