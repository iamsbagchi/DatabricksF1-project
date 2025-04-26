# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/databricksudemyf1dl/demo'

# COMMAND ----------

race_results_df = spark.read.option("inferSchema",True).\
  parquet("/mnt/databricksudemyf1dl/presentation/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC Saving as managed table and running a select query

# COMMAND ----------

# Save the DataFrame to a table using delta format(managed table)
race_results_df.limit(100).write.format("delta").mode("overwrite").saveAsTable("hive_metastore.f1_demo.race_results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   hive_metastore.f1_demo.race_results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Saving as external table and running a select query after making a table with external location

# COMMAND ----------

# Save the DataFrame to a location only using delta format (external table)
race_results_df.limit(100).write.format("delta").save("/mnt/databricksudemyf1dl/demo/race_results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Making a table out of an external location and type exxternal table as we saved it at a location
# MAGIC
# MAGIC USE CATALOG hive_metastore;
# MAGIC create table f1_demo.race_results_external
# MAGIC using delta
# MAGIC location '/mnt/databricksudemyf1dl/demo/race_results_external';

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended hive_metastore.f1_demo.race_results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC How to read data from delta using pyspark

# COMMAND ----------

# reading into the DataFrame from a location only using delta format (external table)
race_results_ext_df = spark.read.load("/mnt/databricksudemyf1dl/demo/race_results_external", format="delta")

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Updating delta table <br>
# MAGIC 2. Delete from delta table**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.race_results_managed
# MAGIC set points = 11-position
# MAGIC where position <= 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/databricksudemyf1dl/demo/race_results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21-position" }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.race_results_managed 
# MAGIC WHERE position > 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/databricksudemyf1dl/demo/race_results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select command for tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.race_results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert into a table using merge

# COMMAND ----------

# MAGIC %md
# MAGIC Making 3 dataframe to do the upsert merge

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_1_df = spark.read.option("inferSchema",True).\
  json("/mnt/databricksudemyf1dl/raw/drivers.json")\
      .filter("driverId <= 10")\
          .select("driverId","dob","name.forename","name.surname")

drivers_2_df = spark.read.option("inferSchema",True).\
  json("/mnt/databricksudemyf1dl/raw/drivers.json")\
      .filter("driverId between 6 and 15")\
          .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

drivers_3_df = spark.read.option("inferSchema",True).\
  json("/mnt/databricksudemyf1dl/raw/drivers.json")\
      .filter("driverId between 1 and 5 or driverId between 16 and 20")\
          .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %md
# MAGIC Creating temp views to be used in SQL syntax

# COMMAND ----------

drivers_1_df.createOrReplaceTempView("drivers_1")
drivers_2_df.createOrReplaceTempView("drivers_2")

# COMMAND ----------

# MAGIC %md
# MAGIC Making a delta table for the merged to happen<br>
# MAGIC In production cases, this will be the real tables/files where the data would be written

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   cretared_date date,
# MAGIC   updated_date date
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert into a table using merge**
# MAGIC 1. SQL <br>
# MAGIC 2. Python
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     cretared_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     cretared_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

delta_table = DeltaTable.forPath(spark, '/mnt/databricksudemyf1dl/demo/drivers_merge')

delta_table.alias('tgt') \
  .merge(drivers_3_df.alias('upd'), 'tgt.driverId = upd.driverId') \
  .whenMatchedUpdate(set =
                     {
                        "tgt.dob" : "upd.dob",
                        "tgt.forename" : "upd.forename",
                        "tgt.surname" : "upd.surname",
                        "tgt.updated_date" : "current_timestamp()"
                        })\
  .whenNotMatchedInsert(values =
                        {
                        "tgt.driverId" : "upd.driverId",
                        "tgt.dob" : "upd.dob",
                        "tgt.forename" : "upd.forename",
                        "tgt.surname" : "upd.surname",
                        "tgt.cretared_date" : "current_timestamp()"
                        })\
                            .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge order by driverId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### History & Versioning
# MAGIC ### Time Travel
# MAGIC ### Vaccum

# COMMAND ----------

# MAGIC %md
# MAGIC **History & Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_demo.drivers_merge version as of 1;
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2025-04-17T10:55:44.000+00:00'

# COMMAND ----------

# df = spark.read.option("timestampAsOf", '2025-04-17T10:56:54.000+00:00').load("/mnt/databricksudemyf1dl/demo/drivers_merge", format="delta")
df = spark.read.option("versionAsOf", '2').load("/mnt/databricksudemyf1dl/demo/drivers_merge", format="delta")
display(df)

# COMMAND ----------

