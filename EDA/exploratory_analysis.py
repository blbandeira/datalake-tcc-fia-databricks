# Databricks notebook source
# MAGIC %sql
# MAGIC select * 
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(*) as total_rows
# MAGIC , count(distinct coin_name) as total_coins
# MAGIC , count(distinct date) as total_period_in_days
# MAGIC , min(date) as first_date
# MAGIC , max(date) as max_date
# MAGIC from refined.crypto_exchange_rates_daily_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC coin_name,
# MAGIC market_cap / 1000000000 as market_cap_billion 
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC where DT_PARTITION = '2023-01-11'
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC coin_name,
# MAGIC supply_ammount / 1000000000 as supply_ammount 
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC where DT_PARTITION = '2023-01-11'
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC coin_name,
# MAGIC volume_exchanged / 1000000 as volume_exchanged_million
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC where DT_PARTITION = '2023-01-11'
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC coin_name,
# MAGIC exchange_rate
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC where DT_PARTITION = '2023-01-11'
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

pandas_df = spark.sql("""
select
coin_name 
from refined.crypto_exchange_rates_daily_incremental
where DT_PARTITION = '2023-01-11'
order by market_cap desc
limit 10 
""").toPandas()

top_10_crypto = list(pandas_df['coin_name'])

# COMMAND ----------

import pyspark.sql.functions as fn
df = spark.table(""" refined.crypto_exchange_rates_daily_incremental """)

# COMMAND ----------

display(
     df
    .filter(fn.col('coin_name').isin(top_10_crypto))
    .select('date', 'coin_name', 'exchange_rate')
)

# COMMAND ----------

display(
    df
    .filter(fn.col('coin_name').isin(top_10_crypto))
    .withColumn('market_cap', fn.col('market_cap')/ 1000000000 )
    .select('date', 'coin_name', 'market_cap')
)

# COMMAND ----------

display(
    df
    .filter(fn.col('coin_name').isin(top_10_crypto))
    .withColumn('supply_ammount', fn.col('supply_ammount')/ 1000000000 )
    .select('date', 'coin_name', 'supply_ammount')
)

# COMMAND ----------

display(
    df
    .filter(fn.col('coin_name').isin(top_10_crypto))
    .withColumn('volume_exchanged', fn.col('volume_exchanged')/ 1000000000 )
    .select('date', 'coin_name', 'volume_exchanged')
)

# COMMAND ----------

display(
    df
    .filter(fn.col('coin_name').isin(top_10_crypto))
    .withColumn('spread', (fn.col('highest_rate') - fn.col('lowest_rate')))
    .select('date', 'coin_name', 'spread')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC with open_close as (
# MAGIC select
# MAGIC date,
# MAGIC coin_name,
# MAGIC lag(exchange_rate, 1) over (partition by coin_name order by date asc) as open,
# MAGIC exchange_rate as close
# MAGIC from refined.crypto_exchange_rates_daily_incremental
# MAGIC )
# MAGIC select
# MAGIC date,
# MAGIC coin_name,
# MAGIC (close - open) / open as perc_diff
# MAGIC from open_close
# MAGIC where coin_name in ('BTC', 'ETH', 'USDT', 'BNB', 'XRP', 'ADA', 'DOGE', 'LINK', 'LTC', 'TRX') --and date in ('2023-01-11', '2023-01-10')

# COMMAND ----------

top_10_crypto
