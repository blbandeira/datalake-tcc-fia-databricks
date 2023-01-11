# Databricks notebook source
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession

class EtlExchangeRates:
    def __init__(self,spark, date, load_path):
        self.spark = spark
        self.date = date
        self.load_path = load_path
    
    def extract_raw_request_api_coinlayer_by_date(self):
        return (
            self
            .spark
            .table('raw.request_api_coinlayer_by_date')
            .filter(fn.col('DT_PARTITION') == self.date)
        )
    
    def transform_table(self, df):
        return(
            df
            .selectExpr(
                   'name as coin_name',
                   'timestamp as timestamp_data_collected',
                   'target as currency',
                   'date(date) as date',
                   'rates.rate as exchange_rate',
                   'rates.high as highest_rate',
                   'rates.low as lowest_rate',
                   'rates.vol as volume_exchanged',
                   'rates.cap as market_cap',
                   'rates.sup as supply_ammount',
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
        self.spark.sql(""" MSCK REAPAIR TABLE refined.crypto_exchange_rates_daily_incremental""")

def main(ETL:EtlExchangeRates):
    df = ETL.extract_raw_request_api_coinlayer_by_date()
    df_transformed = ETL.transform_table(df)
    ETL.load_df(df_transformed)

if __name__ == '__main__':
    date = dbutils.widgets.get('date')
    load_path = dbutils.widgets.get('load_path')
    
    spark = (
       SparkSession
        .builder
        .appName('create_exchange_rates_refined')
        .getOrCreate()
       )
    
    ETL = EtlExchangeRates(
        spark = spark,
        date = date,
        load_path = load_path
    )
    
    main(ETL)
