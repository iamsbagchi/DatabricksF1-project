# Databricks notebook source
# MAGIC %md
# MAGIC Ingest circuits.csv file
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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC Defining own schema

# COMMAND ----------

circuits_schema = StructType(fields= 
                              [StructField("circuitId", IntegerType(), False),
                               StructField("circuitRef", StringType(), True),
                               StructField("name", StringType(), True),
                               StructField("location", StringType(), True),
                               StructField("country", StringType(), True),
                               StructField("lat", DoubleType(), True),
                               StructField("lng", DoubleType(), True),
                               StructField("alt", IntegerType(), True),
                               StructField("url", StringType(), True)]
                             )

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Taking only columns we need in the dataframe

# COMMAND ----------

# Ways to select only the required columns

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

circuits_selected_df = circuits_df.select(col("circuitId"),
     col("circuitRef"),
     col("name"), 
     col("location"),
     col("country"),
     col("lat"),
     col("lng"),
     col("alt")
     )

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns with \<dataframe\>.withColumnRenamed() function

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("name", "name")\
.withColumnRenamed("location", "location")\
.withColumnRenamed("country", "country")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC Add new column to the dataframe<br>
# MAGIC Ex :- Ingestion date

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# with lit("<name>") you can parse a literal value for a specified column 
# .withColumn("env",lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to the DBFS and DeltaLake as delta file

# COMMAND ----------

circuits_final_df.write.format("delta").saveAsTable("hive_metastore.f1_processed.circuits", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

