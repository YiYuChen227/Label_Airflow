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
    # get hql template from ./run_type/
    hql_cmd = f"/opt/airflow/tools/dmp/run_type/{hql_template}"

    # check file position
    print(hql_cmd)

    with open(hql_cmd) as f:
        hql_case_when_cmd = f.read()
        # replace template parameter like {{baseTime}} or {{dt}}
        hql_case_when_cmd = hql_case_when_cmd.format(baseTime=execution_date,dt='30')
        

    # check hql cmd
    print(hql_case_when_cmd)

    # run hql in hive spark
    hd_case_when = spark.sql(hql_case_when_cmd)

    # distinct id 
    id =  hd_case_when.select('distinct_id').distinct().collect()
    user_urls = hd_case_when.select('distinct_id','dollar_url_list').collect()    
  
    # check total id 
    print ('distinct_id = '+str(len(id)))

    spark.sql(hql_case_when_cmd)   

    id_list=[] 

    # append id list
    for distinct_id in id:
        id_list.append(distinct_id[0])  
    
    # get all rule from  hadoop dmp_rule_map
    rule_map_cmd  = f"SELECT * from dm_tag.dmp_rule_map"
    rule_map_spark = spark.sql(rule_map_cmd)

    # get transfer_rule and value from rule_map_spark
    rule_map =  rule_map_spark.select('transfer_rule','value').distinct().collect()

    cursor = postgres_cmd("select upper(product_label),product_label2 from dmp.dmp_table_info")
    product_info_query = cursor.fetchall()

    product_info = {}
    for product in product_info_query:
        product_info[product[0]] = product[1]
    
    # create default DataFrame with id and column
    df = pd.DataFrame(0, index =id_list,columns =product_info.keys())

    # check top 5 default DataFrame
    print(df.head(5))

    
    # mapping rule
    count_id = 0
    print("開始比對rule規則")
    print("總數："+str(len(user_urls)))
    for user_url in user_urls:
        count_id = count_id+1
        for rule in rule_map:
            list_to_str_user_url = ''.join(user_url[1])
            if rule[0].replace('%','') in list_to_str_user_url:
                col_ = str(rule[1]).upper().lstrip()

                df.at[user_url[0],col_] = 1

        if count_id % 10000 ==0:
            print('已處理：'+str(count_id))

    print('已處理完成：'+str(count_id))

    # fill all Nan value with zero
    df = df.fillna(0)
    
    # move index(distinct_id) to first column and rename column as distinct_id
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'distinct_id'})
    

    columns = list(df.columns)
    types = list(df.dtypes)
    
    # mapping columns types and relace to mssql types
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    parkDF = spark.createDataFrame(df, p_schema)


    # set jdbc connection
    sqlUrl = f"jdbc:sqlserver://{sql_server};databaseName={sql_db}"
    
    # define table name
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