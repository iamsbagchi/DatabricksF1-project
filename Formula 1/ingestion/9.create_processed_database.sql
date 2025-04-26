-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

create database if not exists f1_processed
location '/mnt/databricksudemyf1dl/processed'

-- COMMAND ----------

desc database f1_processed