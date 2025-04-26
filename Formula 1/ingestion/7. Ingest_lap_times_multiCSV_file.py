# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest lap_times folder via dataframe reader API and defined schema**

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

laptimes_schema = StructType(fields= [
  StructField("raceId", IntegerType(), False),
  StructField("driverId", IntegerType(), True),
  StructField("lap", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times", schema=laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming the required columns, adding new column (ingestion date as current_timestamp)**

# COMMAND ----------

laptimes_final_df = laptimes_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
        .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))\
            .transform(add_ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **Writting to deltalake managed table under f1_processed, this is an inclemental type load <br>
# MAGIC Using upsert/merge functionality**

# COMMAND ----------

# laptimes_final_df.write.format("parquet").saveAsTable("hive_metastore.f1_processed.lap_times", mode = "overwrite")

merge_condition = "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta(laptimes_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")