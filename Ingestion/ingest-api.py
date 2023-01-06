# Databricks notebook source
AIRFLOW_PARAM_TEXT = dbutils.widgets.get("param1")

with open('/mnt/datalake/raw/hello_world.txt', 'w') as f:
    f.write(AIRFLOW_PARAM_TEXT)
