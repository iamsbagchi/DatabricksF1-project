# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

def add_ingestion_date(inputDf):
    return inputDf.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

def merge_delta(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}", f"{table_name}")):
        delta_table = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")

        delta_table.alias('tgt') \
            .merge(input_df.alias('src'), merge_condition) \
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.format("delta").saveAsTable(f"hive_metastore.{db_name}.{table_name}", 
                                                          partitionBy = partition_column, 
                                                          mode="overwrite")

# COMMAND ----------

