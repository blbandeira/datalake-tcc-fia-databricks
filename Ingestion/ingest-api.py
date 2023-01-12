# Databricks notebook source
import requests
import pandas as pd
import json
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, TimestampType, MapType 
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
import datetime

class IngestCoinlayerApi:
    def __init__(self, spark ,ACCESS_KEY, date, load_path):
        self.ACCESS_KEY = ACCESS_KEY
        self.date = date
        self.spark = spark
        self.load_path = load_path
    
    def get_request_from_api(self):
        return(
            requests.get(f'https://api.coinlayer.com/{self.date}?access_key={self.ACCESS_KEY}&expand=1')
        )
    
    def save_json_on_landing_zone(self, api_result):
        json_result = json.loads(api_result.text)
        json_object = json.dumps(json_result, indent=4)
    
        with open(f'/dbfs/mnt/datalake/landing/coinlayer_api_results/{self.date}_result.json', 'w') as f:
            f.write(json_object)
    
    def read_json_spark(self):
        schema = StructType([
            StructField('success', StringType(), True),
            StructField('terms', StringType(), True),
            StructField('privacy', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('target', StringType(), True),
            StructField('rates', MapType(StringType(), StructType([
                    StructField('rate', DoubleType() , True),
                    StructField('high', DoubleType() , True),
                    StructField('low', DoubleType() , True),
                    StructField('vol', DoubleType() , True),
                    StructField('cap', DoubleType() , True),
                    StructField('sup', DoubleType() , True),
                    StructField('change', DoubleType() , True),
                    StructField('change_pct', DoubleType() , True)
                ])), True)
        ])  
      
        return (
            self
            .spark
            .read
            .option('multiline', 'true')
            .schema(schema)
            .format('json')
            .load(f'/mnt/datalake/landing/coinlayer_api_results/{self.date}_result.json')
            .selectExpr('timestamp', 'target', 'timestamp as date', 'explode(rates)')
            .withColumnRenamed('key', 'name')
            .withColumnRenamed('value', 'rates')
            .select('name'
                    , 'timestamp'
                    , 'target'
                    , 'date'
                    , 'rates'
                   )
            .withColumn('DT_PARTITION', fn.to_date(fn.col('date')))
            )
        
    def load_df(self, df):
        (
        df
        .write
        .partitionBy('DT_PARTITION')
        .mode('overwrite')
        .format('parquet')
        .option("partitionOverwriteMode", "dynamic")
        .save(self.load_path)
        )
    
    def repair_table(self):
        self.spark.sql(""" MSCK REPAIR TABLE raw.request_api_coinlayer_by_date """)

def main(Extract:IngestCoinlayerApi):
    result = Extract.get_request_from_api()
    Extract.save_json_on_landing_zone(result)
    df = Extract.read_json_spark()
    Extract.load_df(df)
    Extract.repair_table()
    

if __name__ == '__main__':
    ACCESS_KEY = dbutils.widgets.get('ACCESS_KEY')
    date = dbutils.widgets.get('date')
    load_path =  dbutils.widgets.get('load_path')
    
    spark = (
       SparkSession
        .builder
        .appName('ingest_api')
        .getOrCreate()
       )
        
    Extract = IngestCoinlayerApi(
      spark = spark,
      ACCESS_KEY = ACCESS_KEY,
      date = date,
      load_path = load_path
    )

    main(Extract)
