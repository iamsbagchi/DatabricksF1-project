# Databricks notebook source
# MAGIC %md
# MAGIC Ingest races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "2021-03-21")

v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %md
# MAGIC Defining own schema and spark.read.csv to read the file from DBFS mount

# COMMAND ----------

race_schema = StructType(fields= 
                              [StructField("raceId", IntegerType(), False),
                               StructField("year", IntegerType(), True),
                               StructField("round", IntegerType(), True),
                               StructField("circuitId", StringType(), True),
                               StructField("name", StringType(), True),
                               StructField("date", DateType(), True),
                               StructField("time", StringType(), True),
                               StructField("url", StringType(), True)]
                             )

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=race_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC Add new column to the dataframe<br>
# MAGIC Ex :- Ingestion date, datasource, race_timestamp (comibe date and time with to_timestamp())

# COMMAND ----------

races_addColumns_df = races_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),"yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source))\
        .transform(add_ingestion_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Taking only columns we need in the dataframe and renaming using alias()

# COMMAND ----------

races_final_df = races_addColumns_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("circuitId", "circuit_id")\
        .drop(col("date"), col("time"), col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC Writting to deltalake managed table under f1_processed

# COMMAND ----------

races_final_df.write.format("delta").saveAsTable("hive_metastore.f1_processed.races", mode="overwrite", partitionBy="race_year") 
#partitionBy("raceyear") might be required

# COMMAND ----------

dbutils.notebook.exit("Success")