# Databricks notebook source
# MAGIC %md
# MAGIC ### Intermental load type data<br>
# MAGIC **Process for each date starting with cutover and moving on**

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest Result.json file via dataframe reader api and defined schema**

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

result_schema = StructType(fields =[
                            StructField("resultId", IntegerType(), False),
                            StructField("raceId",IntegerType(), True),
                            StructField("driverId",IntegerType(), True),
                            StructField("constructorId",IntegerType(), True),
                            StructField("number",IntegerType(), True),
                            StructField("grid",IntegerType(), True),
                            StructField("position",IntegerType(), True),
                            StructField("positionText",StringType(), True),
                            StructField("positionOrder",IntegerType(), True),
                            StructField("points",FloatType(), True),
                            StructField("laps",IntegerType(), True),
                            StructField("time",StringType(), True),
                            StructField("milliseconds",IntegerType(), True),
                            StructField("fastestLap",IntegerType(), True),
                            StructField("rank",IntegerType(), True),
                            StructField("fastestLapTime",StringType(), True),
                            StructField("fastestLapSpeed",FloatType(), True),
                            StructField("statusId",IntegerType(), True)
                            ])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", schema = result_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming the required columns, adding new column (ingestion date as current_timestamp) and dropping unwanted columns (statusId)**

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
        .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))\
            .transform(add_ingestion_date)\
                .drop("statusId")


# COMMAND ----------

"""
The data from the source is already contains some duplicate records, like same driver races 2 time in a race, which by F1 logic is not correct
So we can just write it to the table after de-duping
"""

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Writting to deltalake managed table under f1_processed, this is an inclemental type load<br>
# MAGIC Using upsert/merge functionality

# COMMAND ----------

# results_final_df.write.format("parquet").saveAsTable("hive_metastore.f1_processed.results", mode="overwrite", partitionBy="race_id")

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

