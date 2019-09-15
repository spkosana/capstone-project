
import psycopg2
import os

import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import when,trim,lower
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType,DatetimeConverter
from pyspark.sql.functions import udf, col, monotonically_increasing_id,pandas_udf,PandasUDFType,unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,to_timestamp,dayofweek,from_unixtime

from rs_queries import drop_table_queries,create_table_queries,copy_table_queries,quality_table_queries

def get_spark():

    """
    Aim : To create a spark object for passing it as a paramter to other functions

    Input params: None 
    return :spark session objecte

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    return spark

def get_config(configfile):

    """
    Aim: to read the config file at a time and can be used to get any section from this object 
         there by avoiding reading multiple times

    Input params:
        configfile:  Takes the configfile location 
    return :config object

    """
    
    config = configparser.ConfigParser()
    config.read(configfile)
    
    return config

def get_connection(config, env):

    """
    Aim: Takes config object and env variable and get the details of that section

    Input params:
        config : config object 
        env : section of what you wanted to get (example: dev, qa, prod etc) 
    returns: 
        con : connction object to close the connection at the end 
        cur : cursor object to execute the script

    """
    
    host = config.get(env,"HOST")
    dbname = config.get(env,"DB_NAME")
    user = config.get(env, "DB_USER")
    password = config.get(env, "DB_PASSWORD")
    port = config.get(env, "DB_PORT")
    
    try:
        con = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password} port={port}")
        con.set_session(autocommit=True) 
        cur = con.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return con,cur


def get_aws_credentials(config, env):

    """
    Aim: Takes config object and env variable and get the details of that section

    Input params:
        config : config object 
        env : section of what you wanted to get (example:aws_dev, aws_qa,aws_prod)
    returns: 
        key : aws key
        cur : aws secret key

    """

    key_id = config.get(env,"AWS_ACCESS_KEY_ID") 
    secret_key = config.get(env,"AWS_SECRET_ACCESS_KEY") 

    return key_id,secret_key


def extract_df(spark,paths,custom_schema):

    """
    Aim: Takes spark  object , path of the files and custom schema

    Input params:
        spark : spark object 
        paths : path of the source files
        custome_schema : custom schema should be defined and passed as input
    returns: 
       df : dataframe which is collection of all the records from the file in source location

    """
    df = spark.read.format("csv").\
        option("header", "true").\
        schema(custom_schema).\
        option('delimiter', ',').\
        option('mode', 'DROPMALFORMED').\
        load(paths.split(','))
    
    return df

def load_df(df,key,secret,bucket):
    s3_output_data = f"s3a://{key}:{secret}@{bucket}/"
    df.write.parquet(os.path.join(s3_output_data,df.name),"overwrite")
    print("wrote dataframe to s3 successfully")

def transform_df(df,key,secret,bucket):
    """
    Aim: Takes the dataframe from extract and cleans and load the finalized data into given bucket 

    Input params: 
        df : The dataframe that need to be transformed 
        key: aws key passed to load function 
        secret : which is passed to load function 
        bucket: which is passed to load function

    return: 
        List : list of counts of all dataframes that got loaded 


    """


    """Transforming and writing Incidents dataframe to parquet"""
    
    icols = ['Incident Type Id','Incident Type Description']
    incidents = df[icols].drop_duplicates()
    incidents = incidents.select(col("Incident Type Id").alias("incident_type_id"), col("Incident Type Description").alias("incident_description"))
    incidents.name = "incidents"
    incidents_count = incidents.count() 
    
    load_df(df=incidents,key=key,secret=secret,bucket=bucket)
    
    
    """Transforming and writing priority dataframe to parquet"""
    
    pcols = ['Priority']
    priority = df[pcols].dropna().drop_duplicates()
    priority = priority.select(col("Priority").alias("priority_key"))
    priority = priority.withColumn("priority_level",when(col("priority_key")=="0","High").when(col("priority_key")=="1","Medium").when(col("priority_key")=="2","Low").otherwise("Unknown"))
    priority.name = "priority"
    priority_count = priority.count()
    
    load_df(df=priority,key=key,secret=secret,bucket=bucket)
    
    
    """Transforming and writing time dataframe in parquet"""

    tcol = ["Create Time"]
    time = df[tcol].dropna().dropDuplicates()
    time = time.select(col("Create Time").alias("create_time"))
    time = time.withColumn("start_time", to_timestamp(time.create_time))\
            .withColumn("year", year(time.create_time).cast(IntegerType()))\
            .withColumn("month", month(to_timestamp(time.create_time)))\
            .withColumn("hour", hour(to_timestamp(time.create_time)))\
            .withColumn("day", dayofmonth(to_timestamp(time.create_time)))\
            .withColumn("weekday", dayofweek(to_timestamp((time.create_time))))\
            .withColumn("week", weekofyear(to_timestamp(time.create_time)))
    time  = time[['create_time','start_time','year','month','hour','day','weekday','week']]
    time.name = "time"
    time_count = time.count()
    
    load_df(df=time,key=key,secret=secret,bucket=bucket)

    
    """ Fact table load """
    fact_incidents = df.join(priority, df["Priority"]==priority["priority_key"], how="inner")\
            .join(incidents, df["Incident Type Id"]==incidents["incident_type_id"], how="inner")\
            .join(time, df["Create Time"]==time["create_time"], how="inner")
    fact_incidents = fact_incidents[["create_time","start_time","Closed Time","Agency","Location","Area Id","Beat","priority_key","incident_type_id","Event Number"]].dropDuplicates()
    fact_incidents  = fact_incidents.withColumn("incident_id",monotonically_increasing_id())
    fact_incidents = fact_incidents.select(col('incident_id'),col('create_time'),col('start_time'),col('Closed Time').alias('end_time'),col('Agency').alias('agency'),col('Location').alias('location'),col('Area Id').alias('area_id'),col('Beat').alias('beat'),col('priority_key'),col('incident_type_id'),col('Event Number').alias('eventnumber'))
    fact_incidents = fact_incidents.withColumn("year",year("start_time")).withColumn("month",month("start_time"))
    fact_incidents.name = "fact_incidents"
    fact_incidents = fact_incidents.count()
    
    load_df(df=fact_incidents,key=key,secret=secret,bucket=bucket)
    
    
    """ Getting all the counts for data quality checks"""
    tables_count = [incidents_count,priority_count,time_count,fact_incidents]
    
    return tables_count


def redshift_copy(con,cur,qdict):

    """
    Aim: takes con and cur object along with dictionary of queries to execute on redshift

    Input params: 
        con : connection object for reshidt cluster  need to be passed 
        cur : cursor object for redshift connection need to be passed
        qdict : dictionary of queries which need to be passed and executed on redshift

    return :
        list: list of counts of the tables that are loaded in redshift cluster
    
    """
    
    rs_table_count = []
    for load,quality in qdict.items():
        cur.execute(load)
        con.commit()
        cur.execute(quality)
        row = cur.fetchone()
        rs_table_count.append(row[0])
        
        
    return rs_table_count


def data_quality_check(source,target):

    """

    Aim: Take source and target which are lists of counts of source and target 

    Input params: 
        source : list of counts before loading into redshfit 
        target : list of counts after the data loaded into redshift 

    return: None

    """

    if source==target:
        print("counts matched, quality check passsed")
    else:
        print("counts did not match, quality check failed")


def main():

    paths = "./rec*.csv"
    bucket = "capstone-kspr"
    configfile = "./dwh.cfg"
    aws_env = "AWS"
    cluster_env = "CLUSTER"

    custom_schema = StructType([\
            StructField("Agency", StringType()),\
            StructField("Create Time", StringType()), \
            StructField("Location", StringType()),\
            StructField("Area Id", StringType()),\
            StructField("Beat", StringType()),\
            StructField("Priority", StringType()),\
            StructField("Incident Type Id", StringType()),\
            StructField("Incident Type Description", StringType()),\
            StructField("Event Number", StringType()),\
            StructField("Closed Time", StringType())
        ])

    spark = get_spark()

    config = get_config(configfile=configfile)

    key,secret = get_aws_credentials(config=config,env=aws_env)

    df = extract_df(spark=spark, paths=paths, custom_schema=custom_schema)

    df_tables_count = transform_df(df,key=key,secret=secret,bucket=bucket)

    con,cur = get_connection(config=config, env=cluster_env)

   
    rs_table_load = dict(zip(copy_table_queries,quality_table_queries))

   
    rs_table_count = redshift_copy(con=con, cur=cur, qdict=rs_table_load)

    con.close()

    data_quality_check(source=df_tables_count, target=rs_table_count)


if __name__ == "__main__":
    main()