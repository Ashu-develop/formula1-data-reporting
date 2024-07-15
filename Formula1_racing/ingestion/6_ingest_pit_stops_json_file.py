# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType ,FloatType , DateType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                       StructField("driverId",IntegerType(),True),
                                       StructField("stop",StringType(),True),
                                       StructField("lap",IntegerType(),True),
                                       StructField("time",StringType(),True),
                                       StructField("duration",StringType(),True),
                                       StructField("milliseconds",IntegerType(),True)
                                       ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
