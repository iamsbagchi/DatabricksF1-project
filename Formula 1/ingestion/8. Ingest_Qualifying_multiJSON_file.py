# Databricks notebook source
# MAGIC %md
# MAGIC **Ingest qualifying folder via datafram reader api and defined schema**

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

qualifying_schema = StructType(fields =[
                            StructField("qualifyId", IntegerType(), False),
                            StructField("raceId",IntegerType(), True),
                            StructField("driverId",IntegerType(), True),
                            StructField("constructorId",IntegerType(), True),
                            StructField("number",IntegerType(), True),
                            StructField("position",IntegerType(), True),
                            StructField("q1",StringType(), True),
                            StructField("q2",StringType(), True),
                            StructField("q3",StringType(), True)
                            ])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying", schema = qualifying_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming the required columns, adding new column (ingestion date as current_timestamp)**

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
        .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))\
            .transform(add_ingestion_date)


# COMMAND ----------

# MAGIC %md
# MAGIC **Writting to deltalake managed table under f1_processed, this is an inclemental type load <br>
# MAGIC Using upsert/merge functionality**

# COMMAND ----------

# qualifying_final_df.write.format("parquet").saveAsTable("hive_metastore.f1_processed.qualifying", mode="overwrite")

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")