-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_table_df = spark.read.option("header", True).csv(f"{raw_folder_path}/circuits.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_table_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.circuits")

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS ( header = "true")
LOCATION "mnt/dataisreportingdl/raw/circuits.csv"

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_table_df = spark.read.option("header", True).csv(f"{raw_folder_path}/races.csv")
-- MAGIC races_table_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.races")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_table_df = spark.read.option("header", True).json(f"{raw_folder_path}/constructors.json")
-- MAGIC races_table_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.constructors")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DateType
-- MAGIC name_schma = StructType(fields=[StructField("forename",StringType(),True),
-- MAGIC                                 StructField("surname",StringType(),True)
-- MAGIC ])
-- MAGIC drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
-- MAGIC                                     StructField("driverRef",StringType(),True),
-- MAGIC                                     StructField("number",IntegerType(),True),
-- MAGIC                                     StructField("code",StringType(),True),
-- MAGIC                                     StructField("name",name_schma),
-- MAGIC                                     StructField("dob",DateType(),True),
-- MAGIC                                     StructField("nationality",StringType(),True),
-- MAGIC                                     StructField("url",StringType(),True)
-- MAGIC                                     ])
-- MAGIC                                 
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")
-- MAGIC drivers_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.drivers")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType
-- MAGIC results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
-- MAGIC                                     StructField("raceId",IntegerType(),True),
-- MAGIC                                     StructField("driverId", IntegerType(), True),
-- MAGIC                                     StructField("constructorId",IntegerType(),True),
-- MAGIC                                     StructField("number",IntegerType(),True),
-- MAGIC                                     StructField("grid",IntegerType(),True),
-- MAGIC                                     StructField("position",IntegerType(),True),
-- MAGIC                                     StructField("positionText",StringType(),True),
-- MAGIC                                     StructField("positionOrder",IntegerType(),True),
-- MAGIC                                     StructField("points",FloatType(),True),
-- MAGIC                                     StructField("laps",IntegerType(),True),
-- MAGIC                                     StructField("time",StringType(),True),
-- MAGIC                                     StructField("milliseconds",IntegerType(),True),
-- MAGIC                                     StructField("fastestLap",IntegerType(),True),
-- MAGIC                                     StructField("rank",IntegerType(),True),
-- MAGIC                                     StructField("fastestLapTime",StringType(),True),
-- MAGIC                                     StructField("fastestLapSped",FloatType(),True),
-- MAGIC                                     StructField("statusId",StringType(),True)
-- MAGIC                                     ])
-- MAGIC
-- MAGIC results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/results.json")
-- MAGIC results_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType , StructField , IntegerType , StringType ,FloatType , DateType
-- MAGIC pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
-- MAGIC                                        StructField("driverId",IntegerType(),True),
-- MAGIC                                        StructField("stop",StringType(),True),
-- MAGIC                                        StructField("lap",IntegerType(),True),
-- MAGIC                                        StructField("time",StringType(),True),
-- MAGIC                                        StructField("duration",StringType(),True),
-- MAGIC                                        StructField("milliseconds",IntegerType(),True)
-- MAGIC                                        ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", True).json(f"{raw_folder_path}/pit_stops.json")
-- MAGIC pit_stops_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.pit_stops")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType
-- MAGIC lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
-- MAGIC                                        StructField("driverId",IntegerType(),True),
-- MAGIC                                        StructField("lap",IntegerType(),True),
-- MAGIC                                        StructField("posiion",IntegerType(),True),
-- MAGIC                                        StructField("time",StringType(),True),
-- MAGIC                                        StructField("milliseconds",IntegerType(),True)
-- MAGIC                                        ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")
-- MAGIC lap_times_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.lap_times")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType , StructField , IntegerType , StringType ,FloatType , DateType
-- MAGIC qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
-- MAGIC                                        StructField("raceId",IntegerType(),True),
-- MAGIC                                        StructField("driverId",IntegerType(),True),
-- MAGIC                                        StructField("constructorId",IntegerType(),True),
-- MAGIC                                        StructField("number",IntegerType(),True),
-- MAGIC                                        StructField("position",IntegerType(),True),
-- MAGIC                                        StructField("q1",StringType(),True),
-- MAGIC                                        StructField("q2",StringType(),True),
-- MAGIC                                        StructField("q3",StringType(),True)
-- MAGIC                                        ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC qualifying_df = spark.read.schema(qualifying_schema).option("multiline",True).json(f"{raw_folder_path}/qualifying")
-- MAGIC qualifying_df.write.mode("overwrite").format("delta").mode("ignore").saveAsTable("f1_raw.qualifying")

-- COMMAND ----------


