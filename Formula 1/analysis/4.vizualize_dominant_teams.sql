-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center;">Report on Dominant Formula 1 Team</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_team as
select team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points,
rank() over (order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having count(1) > 100
order by avg_points desc;

-- COMMAND ----------

select * from v_dominant_team;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Vizualize dominant teams over the years**

-- COMMAND ----------

select race_year, team_name, count(1) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rank <= 5)
group by race_year, team_name
order by race_year, avg_points desc;

-- COMMAND ----------

