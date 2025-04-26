# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake using Service Principle
# MAGIC - get and set the App/Client id, Directory/Tenant id, Secret key
# MAGIC - call the file system utility mount to mount the storage
# MAGIC - explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
storage_acount = dbutils.widgets.get("storage_account")

# COMMAND ----------

def mount_azure_storage(storage_account_name, container_name):
  # getting secret from scope and keyVault
  client_id = "3384e6c6-74f0-464e-b0fd-eb3fada10da9"
  tenant_id = "17d74602-6531-4b1d-b217-fdcad54d1e13"
  secret = dbutils.secrets.get(scope= "formula1-scope", key="databricksudemyf1dl-sp-secret")

  #Spark config
  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  
  # Unmount if already mounted for robustness
  for mount in dbutils.fs.mounts():
    if mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}":
      dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

  # Mount the storage account container to DBFS 
  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)


# COMMAND ----------

# MAGIC %md 
# MAGIC Mount the rest of containers (raw,processed,presentation)
# MAGIC

# COMMAND ----------

if storage_acount!= '':
    mount_azure_storage(storage_acount, "raw")
    mount_azure_storage(storage_acount, "processed")
    mount_azure_storage(storage_acount, "presentation")
else:
    print("Please provide a storage account name")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

