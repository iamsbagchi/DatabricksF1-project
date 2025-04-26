# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file
# MAGIC This file contains nested json, so we need to create a schema for each nested objects

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC Read the JSON file usinf spark dataframe reader API

# COMMAND ----------

driver_name_schema = StructType(fields = [
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True),
])

# below "name" is a custom schema defination which is done in the above code
driver_schema = StructType(fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", driver_name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),

])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema=driver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename columns and add new columns**<br>
# MAGIC 1. driverId to driver_id, driverRef to driver_ref
# MAGIC 2. add ingestion date, name field as a concat of name.forename and name.surname

# COMMAND ----------

driver_addColumns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
        .withColumn("name", concat(drivers_df.name.forename, lit(" "), drivers_df.name.surname))\
        .withColumn("data_source", lit(v_data_source))\
            .transform(add_ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop any unwanted columns

# COMMAND ----------

driver_final_df = driver_addColumns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Writting to deltalake managed table under f1_processed

# COMMAND ----------

driver_final_df.write.format("delta").saveAsTable("hive_metastore.f1_processed.drivers", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("Success")