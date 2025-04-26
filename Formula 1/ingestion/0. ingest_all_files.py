# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Get file list from raw folder

# COMMAND ----------

file_name=[]
for filePoints in dbutils.fs.ls("/mnt/databricksudemyf1dl/raw") :
    file_name.append(filePoints.name[:-1])

print(file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest all files by running this notebook using dbutils.notebook.run()

# COMMAND ----------

file1 = "1. Ingest_Circuits_csv_file"
file2 = "2. Ingest_Races_csv_file"
file3 = "3. Ingest_Constructors_json_file"
file4 = "4. Ingest_Drivers_json_file"
file5 = "5. Ingest_Results_json_file"
file6 = "6. Ingest_Pitstops_json_file"
file7 = "7. Ingest_lap_times_multiCSV_file"
file8 = "8. Ingest_Qualifying_multiJSON_file"

# COMMAND ----------

# MAGIC %md
# MAGIC Cutover data load

# COMMAND ----------

if dbutils.notebook.run(file1, 0, {"p_data_source": "Ergast API",
                                   "p_file_date": file_name[0]}) == "Success":
    print(f"{file1}: Success")
else:
    print(f"{file1}: Failed")

if dbutils.notebook.run(file2, 0, {"p_data_source": "Ergast API",
                                   "p_file_date": file_name[0]}) == "Success":
    print(f"{file2}: Success")
else:
    print(f"{file2}: Failed")

if dbutils.notebook.run(file3, 0, {"p_data_source": "Ergast API",
                                   "p_file_date": file_name[0]}) == "Success":
    print(f"{file3}: Success")
else:
    print(f"{file3}: Failed")

if dbutils.notebook.run(file4, 0, {"p_data_source": "Ergast API",
                                   "p_file_date": file_name[0]}) == "Success":
    print(f"{file4}: Success")
else:
    print(f"{file4}: Failed")



# COMMAND ----------

# MAGIC %md
# MAGIC Incremental type data load

# COMMAND ----------

for file in file_name:

    if dbutils.notebook.run(file5, 0, {"p_data_source": "Ergast API",
                                       "p_file_date": file}) == "Success":
        print(f"{file5}: Success for load {file}")
    else:
        print(f"{file5}: Failed for load {file}")

# COMMAND ----------


for file in file_name:

    if dbutils.notebook.run(file6, 0, {"p_data_source": "Ergast API",
                                       "p_file_date": file}) == "Success":
        print(f"{file6}: Success for load {file}")
    else:
        print(f"{file6}: Failed for load {file}")

# COMMAND ----------

for file in file_name:

    if dbutils.notebook.run(file7, 0, {"p_data_source": "Ergast API",
                                       "p_file_date": file}) == "Success":
        print(f"{file7}: Success for load {file}")
    else:
        print(f"{file7}: Failed for load {file}")

# COMMAND ----------

for file in file_name:

    if dbutils.notebook.run(file8, 0, {"p_data_source": "Ergast API",
                                       "p_file_date": file}) == "Success":
        print(f"{file8}: Success for load {file}")
    else:
        print(f"{file8}: Failed for load {file}")