# Databricks notebook source
import requests
import pandas as pd
import json
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, BooleanType, TimestampType, ArrayType 
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
        df_pd = pd.read_json(f'/dbfs/mnt/datalake/landing/coinlayer_api_results/{self.date}_result.json')
        df_pd = (
            df_pd
            .reset_index()
            .rename(columns={'index': 'name'})
        )

        df_pd['rates'] = df_pd['rates'].astype(str)
        
        schema = StructType([
            StructField('rate', DoubleType() , True),
            StructField('high', DoubleType() , True),
            StructField('low', DoubleType() , True),
            StructField('vol', DoubleType() , True),
            StructField('cap', DoubleType() , True),
            StructField('sup', DoubleType() , True),
            StructField('change', DoubleType() , True),
            StructField('change_pct', DoubleType() , True)
    ])
        
        df_spark = self.spark.createDataFrame(df_pd)

        return (
            df_spark.withColumn('rates', fn.from_json('rates', schema=schema)
                     )
            .select('name'
                    , 'timestamp'
                    , 'target'
                    , 'date'
                    , 'rates'
                   )
            .withColumn('DT_PARTITION', fn.to_date('date'))
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
    load_path = dbutils.widgets.get('load_path')
    
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
