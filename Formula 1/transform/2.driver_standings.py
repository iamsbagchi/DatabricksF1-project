# Databricks notebook source
# MAGIC %md
# MAGIC **Produce driver standings transformation logic.<br>
# MAGIC Using race_results delta file from presentation folder**

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, count_if, col, desc, asc, rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.load(f"{presentation_folder_path}/race_results", format="delta")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

driver_standings_df = race_results_df\
    .groupBy("race_year", "driver_name", "driver_nationality")\
    .agg(sum("points").alias("total_points"),
         count_if(col("position") == 1).alias("wins")
         )

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("driver_rank", rank().over(driverRankSpec))

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental type processing due to race_results file being incremental**

# COMMAND ----------

# final_df.write.format("parquet").saveAsTable("hive_metastore.f1_presentation.driver_standings", mode="overwrite")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')