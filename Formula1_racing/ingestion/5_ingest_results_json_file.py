# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("grid",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("positionText",StringType(),True),
                                    StructField("positionOrder",IntegerType(),True),
                                    StructField("points",FloatType(),True),
                                    StructField("laps",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True),
                                    StructField("fastestLap",IntegerType(),True),
                                    StructField("rank",IntegerType(),True),
                                    StructField("fastestLapTime",StringType(),True),
                                    StructField("fastestLapSped",FloatType(),True),
                                    StructField("statusId",StringType(),True)
                                    ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

results_with_column_df = results_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastetst_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_column_df)

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop("statusId")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
