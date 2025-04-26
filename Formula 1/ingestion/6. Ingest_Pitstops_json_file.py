# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest pitstops.json file via dataframe reader API and defined schema**

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

from pyspark.sql.functions import lit

# COMMAND ----------

pitstops_schema = StructType(fields= [
  StructField("raceId", IntegerType(), False),
  StructField("driverId", IntegerType(), True),
  StructField("stop", IntegerType(), True),
  StructField("lap", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("duration", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", schema=pitstops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming the required columns, adding new column (ingestion date as current_timestamp)**

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
        .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))\
            .transform(add_ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **Writting to deltalake managed table under f1_processed, this is an inclemental type load <br>
# MAGIC Using upsert/merge functionality**

# COMMAND ----------

# pitstops_final_df.write.format("parquet").saveAsTable("hive_metastore.f1_processed.pit_stops", mode = "overwrite")

merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta(pitstops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from hive_metastore.f1_processed.pit_stops;