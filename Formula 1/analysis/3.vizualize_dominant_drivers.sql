-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center;">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

use catalog hive_metastore;

-- COMMAND ----------

create or replace temp view v_dominant_drivers as
select driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points,
rank() over (order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1) > 50
order by avg_points desc;

-- COMMAND ----------

select * from v_dominant_drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Vizualization of top 10 dominant drivers over all time**

-- COMMAND ----------

select race_year, driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
order by race_year, avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Vizualization of top 10 drivers and their total races and points**

-- COMMAND ----------

select race_year, driver_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
order by race_year, avg_points desc;

-- COMMAND ----------

