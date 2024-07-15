-- Databricks notebook source
SELECT driver_name,
count(1) AS total_races,
avg(calculated_points) AS avg_points,
sum(calculated_points) AS total_points
 FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC

-- COMMAND ----------


