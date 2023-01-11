# Databricks notebook source
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import requests
from pyspark.sql.types import StructType,StructField, StringType,MapType,LongType 

class IngestCryptoInfo:
    def __init__(self, spark, date, ACCESS_KEY, load_path):
        self.date = date
        self.ACCESS_KEY = ACCESS_KEY
        self.spark = spark
        self.load_path = load_path
    
    def request_list_endpoint(self):
        return(
            requests.get(f'https://api.coinlayer.com/list?access_key={self.ACCESS_KEY}')
        )
    
    def save_result_on_landing_as_json(self, resp):
        json_resp = json.loads(resp.text)
        json_object = json.dumps(json_resp, indent= 4)

        with open(f'/dbfs/mnt/datalake/landing/crypto_dimension_request/{date}_response.json', 'w') as f:
            f.write(json_object)
    
    def transform_json(self):
        
        schema = StructType([
                        StructField('success', StringType(), True),
                        StructField('crypto', MapType(StringType(), StructType([
                            StructField('symbol', StringType(), True),
                            StructField('name', StringType(), True),
                            StructField('name_full', StringType(), True),
                            StructField('icon_url', StringType(), True)
                    ])), True)
                ])
        
        return (
            self
            .spark
            .read
            .option('multiline', 'true')
            .schema(schema)
            .format('json')
            .load(f'/mnt/datalake/landing/crypto_dimension_request/{self.date}_response.json')
            .selectExpr('explode(crypto)')
            .withColumnRenamed('key', 'name')
            .withColumnRenamed('value', 'crypto_info')
            .withColumn('DT_PARTITION', fn.lit(self.date))
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
        self.spark.sql(""" MSCK REPAIR TABLE raw.request_crypto_info """)

def main(Extract: IngestCryptoInfo):
    resp = Extract.request_list_endpoint()
    Extract.save_result_on_landing_as_json(resp)
    df = Extract.transform_json()
    Extract.load_df(df)
    Extract.repair_table()

if __name__ == '__main__':
    ACCESS_KEY = dbutils.widgets.get('ACCESS_KEY')
    date = dbutils.widgets.get('date')
    load_path = dbutils.widgets.get('load_path')
    
    spark = (
        SparkSession
        .builder
        .appName('ingestion_crypto_info')
        .getOrCreate()
    )
    
    Extraction = IngestCryptoInfo(
        date = date,
        load_path = load_path,
        ACCESS_KEY = ACCESS_KEY,
        spark = spark
    )
    
    main(Extraction)
