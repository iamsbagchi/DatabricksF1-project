-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **All time dominant team**

-- COMMAND ----------

select team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(1) > 100
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Dominat teams in 2011 to 2020 decade**

-- COMMAND ----------

select team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name
having count(1) > 100
order by avg_points desc;

-- COMMAND ----------

