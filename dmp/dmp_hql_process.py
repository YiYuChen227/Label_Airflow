from pyspark.sql import SparkSession
from os.path import abspath
import click
import pathlib
from datetime import datetime
from airflow.hooks.base import BaseHook
import psycopg2
import re

@click.command()
@click.option('--mssql_jdbc_jar', required=True)
@click.option('--hive_warehouse', required=True)
@click.option('--hive_metastore', required=True)
@click.option('--hive_db', help='The destination hive database where to create the hive table.', required=True)
@click.option('--target_table', help='The hive table you wish to create.', required=True)
@click.option('--task_id', required=True)
@click.option('--source_table', required=True)
@click.option('--code_map_table', required=True)
@click.option('--hql_template', required=True)
@click.option('--product_label', help='product label', required=True)
@click.option('--execution_date', help='execution date', required=True)
@click.option('--postgres_table', required=True) #partition_key
@click.option('--partition_key', required=True) 
@click.option('--product_label2', required=True) 
def hql_process(mssql_jdbc_jar, hive_warehouse, hive_metastore, hive_db,task_id, target_table,source_table,code_map_table,hql_template,product_label,execution_date,postgres_table,partition_key,product_label2):
        
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
    
    product_label = product_label.replace('_and_','&').replace('_or_','/').replace('__',' ')
    # get case when from hadoop hive
    mapping_table=product_label    
    hql_case_when_cmd = f"SELECT * from {code_map_table} where lower(product_label) ='{product_label.lower()}'"
    
    # get template type
    hql_cmd = f"/opt/airflow/tools/dmp/run_type/dmp_process_info_{hql_template}.hql"
    print(product_label)
    print(hql_cmd)
    print(hql_case_when_cmd)
    print(target_table)

    process_time = datetime.now().strftime("%H:%M:%S")
    
    # covert case when 
    case_when_Str = "( CASE "
    case_list=[]
    append_case=[]
    hd_case_when = spark.sql(hql_case_when_cmd)
    distinct_cols =  hd_case_when.select('mapping_col','value').distinct()
    user_tag =  hd_case_when.select('product_type','dt').distinct().collect()
    
    # get product type
    product_type = user_tag[0]['product_type']
    
    # get dt value
    dt = user_tag[0]['dt']
    
    # get partition value
    #partition_pt = re.search('\w+[-][0-9]+', product_type).group()
    print(f"usertag: {product_type} ")
    #print(f"partition_pt: {partition_pt}")
    for distinct_col in distinct_cols.collect():
        case_list =[]
        for case_cd in hd_case_when.collect():
             if distinct_col['mapping_col'] == case_cd['mapping_col'] :
                 print(case_cd['transfer_rule'])
                 c = case_cd['transfer_rule'].replace(' ','')
                 case_list.append(f" when {case_cd['mapping_col']} like '{c}' then '{distinct_col['value']}'")
        append_case.append(case_when_Str + " ".join(case_list) + f"  END ) as user_tag ")
    print(f" case when string: {','.join(append_case)} ")
    
    #postgres server connect
    postgres_server_conn = BaseHook.get_connection("postgres")        
    conn_string = "host={0} user={1} dbname={2} password={3}".format(postgres_server_conn.host,postgres_server_conn.login,'airflow',postgres_server_conn.login)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    #replace hql cmd
    with open(hql_cmd) as f:
        line=f.read()
        print(line.format(hive_db=hive_db,source_table=source_table,case_sql=','.join(append_case),product_label2=product_label2,partition_key=partition_key,product_type=product_type,dt=dt,baseTime=execution_date,process_time=process_time))
        done_data = spark.sql(line.format(hive_db=hive_db,source_table=source_table,case_sql=','.join(append_case),product_label2=product_label2,partition_key=partition_key,product_type=product_type,dt=dt,baseTime=execution_date,process_time=process_time))
        print('Count: '+str(len(done_data.collect())))       


        # Partition columns have already been defined for the table. It is not necessary to use partitionBy().
        if len(done_data.collect()) >0 :
            done_data.write.mode("append").format("parquet").insertInto(f"{target_table}", overwrite=True)
        else:
            print('No data')


        cursor.execute(f"delete from {postgres_table} where task_id = '{task_id}' and run_date = '{execution_date}'")
        cursor.execute(f"insert into {postgres_table} values('{task_id}','{execution_date}','{hive_db}','{target_table}','{str(len(done_data.collect()))}')")
        conn.commit()
        

    spark.stop()
    
  
if __name__=='__main__':
    hql_process()
   
