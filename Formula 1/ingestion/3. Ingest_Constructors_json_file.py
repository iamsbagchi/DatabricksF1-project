# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema=constructor_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping any unwanted column(s) from the dataframe, using drop(\<col\>) function from DataFrame API

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md 
# MAGIC Rename columns as needed and add ingestion_date field

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
        .withColumn("data_source", lit(v_data_source))\
            .transform(add_ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Writting to deltalake managed table under f1_processed

# COMMAND ----------

constructor_final_df.write.format("delta").saveAsTable("hive_metastore.f1_processed.constructors", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("Success")