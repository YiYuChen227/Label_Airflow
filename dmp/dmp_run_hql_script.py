from pyspark.sql import SparkSession
import click
from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
from pyspark.sql.types import *



@click.command()
@click.option('--hive_warehouse', required=True)
@click.option('--hive_metastore', required=True)
@click.option('--hql_template', required=True)
@click.option('--execution_date', help='execution date', required=True)
@click.option('--sql_server', required=True)
@click.option('--sql_username', required=True)
@click.option('--sql_password', required=True)
@click.option('--sql_db', required=True)
@click.option('--sql_table', required=True)



def hql_process(hive_warehouse, hive_metastore,hql_template,execution_date,sql_server,sql_username,sql_password,sql_db,sql_table):
        
    # call spark session and set config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Insert_Data") \
        .config("spark.sql.warehouse.dir", hive_warehouse)\
        .config("hive.metastore.uris", f"thrift://{hive_metastore}") \
        .config("hive.warehouse.subdir.inherit.perms","false")\
        .enableHiveSupport() \
        .getOrCreate()
    spark.conf.set("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY")
    spark.sparkContext.setLogLevel('WARN')
    

    # get template type
    # get hql template from ./level2/hql/
    hql_cmd = f"/opt/airflow/tools/dmp/hql/interest_tag/{hql_template}"

    # check file position
    print(hql_cmd)

    with open(hql_cmd) as f:
        hql_case_when_cmd = f.read()
        # replace template parameter like {{baseTime}} or {{dt}}
        hql_case_when_cmd = hql_case_when_cmd.format(baseTime=execution_date)
        

    # check hql cmd
    print(hql_case_when_cmd)

    # run hql in hive spark
    hd_case_when = spark.sql(hql_case_when_cmd)
   
    # set jdbc connection
    sqlUrl = f"jdbc:sqlserver://{sql_server};databaseName={sql_db}"
    
    # define table name
    table_name = sql_table +'_'+ execution_date.replace('-','_')
    print('table_name: '+table_name)  

    # use spark dump to mssql
    hd_case_when.write.format("jdbc") \
       .option("url", sqlUrl) \
       .option("dbtable",table_name)\
       .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
       .option("user",sql_username)\
       .option("password",sql_password)\
       .mode('overwrite').save()

    spark.stop()

def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

def postgres_cmd(cmd_str):
    postgres_server_conn = BaseHook.get_connection("postgres")
    dbname="airflow"
    conn_string = "host={0} user={1} dbname={2} password={3}".format(postgres_server_conn.host,postgres_server_conn.login, dbname,postgres_server_conn.password)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute(cmd_str)
    return cursor

  
if __name__=='__main__':
    hql_process()

    