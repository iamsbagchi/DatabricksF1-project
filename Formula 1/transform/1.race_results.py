# Databricks notebook source
# MAGIC %md
# MAGIC **Transformation of the raw data to produce race results.<br>
# MAGIC Using drivers, constructors, circuits, races, results DELTA files**

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

drivers_df = spark.read.load(f"{processed_folder_path}/drivers", format="delta")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.load(f"{processed_folder_path}/constructors", format="delta").withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.load(f"{processed_folder_path}/circuits", format="delta").withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.load(f"{processed_folder_path}/races", format="delta")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df = spark.read.load(f"{processed_folder_path}/results", format="delta")\
    .filter(f"file_date = '{v_file_date}'") \
        .withColumnRenamed("time", "race_time") \
        .withColumnRenamed("race_id", "result_race_id") \
        .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC **Join circuits_df to races_df**

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Join results_df to race_circuits_df, drivers_df, constructors_df**<br>
# MAGIC **Add the created_date column**

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

race_results_final_df = race_results_df.select(races_df["race_id"],"race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points", "position","result_file_date")\
    .withColumn("created_date", current_timestamp())\
    .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental type processing due to results file being incremental**

# COMMAND ----------

# race_results_final_df.write.parquet(f"{presentation_folder_path}/race_results", mode="overwrite")
# race_results_final_df.write.format("parquet").saveAsTable("hive_metastore.f1_presentation.race_results", mode="overwrite")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta(race_results_final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')