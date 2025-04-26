-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

create database if not exists f1_presentation
location '/mnt/databricksudemyf1dl/presentation'

-- COMMAND ----------

desc database f1_presentation;

-- COMMAND ----------

