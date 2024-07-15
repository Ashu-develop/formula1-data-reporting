# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'dataisreporting-secret_scope', key = 'dataisreporting-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'dataisreporting-secret_scope', key = 'dataisreporting-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'dataisreporting-secret_scope', key = 'dataisreporting-app-client-secret')

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="dataisreporting-secret_scope")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dataisreportingdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dataisreportingdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dataisreportingdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dataisreportingdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dataisreportingdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dataisreportingdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dataisreportingdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


