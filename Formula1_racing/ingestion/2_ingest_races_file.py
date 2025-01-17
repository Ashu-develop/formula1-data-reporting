# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DateType


# COMMAND ----------

races_schema = StructType(fields= [StructField("raceId",IntegerType(), False),
                                   StructField("year",IntegerType(), True),
                                   StructField("round",IntegerType(), True),
                                   StructField("circuitId",IntegerType(), True),
                                   StructField("name",StringType(), True),
                                   StructField("date",DateType(), True),
                                   StructField("time",StringType(), True),
                                   StructField("url",StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , to_timestamp , concat , col , lit

# COMMAND ----------



races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

race_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),
                                                  col("year").alias("race_year"),
                                                  col("round"),
                                                  col("circuitId").alias("circuit_id"),
                                                  col("name"),
                                                  col("ingestion_date"),
                                                  col("race_timestamp"))

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

race_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dataisreportingdl/processed/races

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

race_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
