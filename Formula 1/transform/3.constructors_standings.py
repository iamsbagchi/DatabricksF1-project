# Databricks notebook source
# MAGIC %md
# MAGIC **Produce constructors standings transformation logic.<br>
# MAGIC Using race_results parquet file from processed folder**

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import sum, count_if, col, desc, asc, rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.load(f"{presentation_folder_path}/race_results", format="delta")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

constructors_standings_df = race_results_df\
    .groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"),
         count_if(col("position") == 1).alias("wins")
         )

# COMMAND ----------

constructorsRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructors_standings_df.withColumn("constructor_rank", rank().over(constructorsRankSpec))

# COMMAND ----------

# final_df.write.format("parquet").saveAsTable("hive_metastore.f1_presentation.constructor_standings", mode="overwrite")

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from hive_metastore.f1_presentation.constructor_standings

# COMMAND ----------

