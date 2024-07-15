# Databricks notebook source
v_result=dbutils.notebook.run("1_ingest_circuits_csv_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("2_ingest_races_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("3_ingest_constructor_json_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("4_ingest_drivers_json_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("5_ingest_results_json_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("6_ingest_pit_stops_json_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("7_ingest_laptimes_csv_files",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("8_ingest_qualifying_json_files",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result
