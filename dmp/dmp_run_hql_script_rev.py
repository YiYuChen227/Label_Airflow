from ast import JoinedStr
from operator import concat
import resource
from tkinter.tix import Tree
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
    hql_cmd = f"/opt/airflow/tools/dmp/hql/interest_tag/興趣_大類_濃度.hql"

    # check file position
    print(hql_cmd)

    with open(hql_cmd) as f:
        hql_cmd = f.read()

        # 取代變數
        hql_cmd = hql_cmd.format(baseTime=execution_date)        

    # check hql cmd
    print(hql_cmd)

    # 執行 hql 語法
    spark_result = spark.sql(hql_cmd)

    #篩選出欄位
    customuids =  spark_result.select('customuid').distinct().collect()
    interest_categorys =  spark_result.select('interest_category').distinct().collect()
    select_results =  spark_result.select('customuid','interest_category','degree').distinct().collect()
    #組出客戶ID
    customuid_list = []
    for customuid in customuids:
        customuid_list.append(customuid[0])
    #組出大類的欄位
    interest_category_list =[]
    for interest_categorys in interest_categorys:
        interest_category_list.append(interest_categorys[0])

    #組出大類的橫向表空白的表
    df = pd.DataFrame('', index =customuid_list,columns =interest_category_list)
    print(df.head(5))

    #確認數量
    print('第一階段總數：'+str(len(select_results)))

    select_results_int = 0
    
    #用迴圈組出個客戶的興趣濃度
    for select_result in select_results:

        select_results_int = select_results_int+1
        if select_results_int %100000 == 0:
            print(f'第一階段已完成：{str(select_results_int)}')
        value = ""
        #select_result[2]  = degree
        if select_result[2] != None:
            value = select_result[2]

        #替換pandas內的表值成 degree
        df.at[select_result[0],select_result[1]] = value
        
    #rename columns
    for col in interest_category_list:
        df = df.rename(columns = {col:'大類_濃度_'+col})    

    hql_cmd2 = f"/opt/airflow/tools/dmp/hql/interest_tag/興趣_細類.hql"
    # check file position
    print(hql_cmd2)

    with open(hql_cmd2) as f:
        hql_case_when_cmd2 = f.read()
        # replace template parameter like {{baseTime}} or {{dt}}
        hql_case_when_cmd2 = hql_case_when_cmd2.format(baseTime=execution_date)    

    # run hql in hive spark
    print(hql_case_when_cmd2)
    spark_result2 = spark.sql(hql_case_when_cmd2)
    customuids2 =  spark_result2.select('customuid').distinct().collect()
    interest_categorys2 =  spark_result2.select('interest_class').distinct().collect() #interest_category
    select_results2 =  spark_result2.select('customuid','interest_category','interest_class').distinct().collect()

    customuid_list2 = []
    interest_category_list2 =[]


    for customuid in customuids2:
        customuid_list2.append(customuid[0])
    for interest_categorys in interest_categorys2:
        interest_category_list2.append(interest_categorys[0])


    

    df2 = pd.DataFrame('0', index =customuid_list2,columns =interest_category_list2)
    print(df2.head(5))
    print('第二階段總數：'+str(len(select_results2)))



    select_results_int = 0
    for select_result in select_results2:
        select_results_int = select_results_int+1
        if select_results_int % 100000 == 0:
            print(f'第二階段已完成：{str(select_results_int)}')
        df2.at[select_result[0],select_result[2]] = 1 #select_result[2]
        

    print('已完成')

    df_result = pd.concat([df,df2],axis=1)
    

    df_result.reset_index(inplace=True)
    df_result = df_result.rename(columns = {'index':'customuid'})
    

    columns = list(df_result.columns)
    types = list(df_result.dtypes)
    
    # mapping columns types and relace to mssql types
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    parkDF = spark.createDataFrame(df_result, p_schema)
   
    # set jdbc connection
    sqlUrl = f"jdbc:sqlserver://{sql_server};databaseName={sql_db}"
    
    # define table name
    sql_table ='興趣_大類_濃度及細類'
    table_name = sql_table +'_'+ execution_date.replace('-','_')
    print('table_name: '+table_name)  

    # use spark dump to mssql

    parkDF.write.format("jdbc") \
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

    