-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Run this notebook when decided to go with incremental type load data**

-- COMMAND ----------

drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/databricksudemyf1dl/processed"

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/databricksudemyf1dl/presentation"

-- COMMAND ----------

select * from f1_processed.results

-- COMMAND ----------

