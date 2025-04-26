-- Databricks notebook source
use catalog hive_metastore

-- COMMAND ----------

create database if not exists f1_raw;
use f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Circuits.csv, Races.csv as a table schema(s) with header = true
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits
(
circuitId integer,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
using csv
options (path "/mnt/databricksudemyf1dl/raw/circuits.csv", header true);

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races
(
raceId integer,
year integer,
round integer,
circuitId string,
name string,
date date,
time string,
url string
)
using csv
options (path "/mnt/databricksudemyf1dl/raw/races.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Constructors.json as a table schema (single line, simple structure)

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors
(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
using json
options (path "/mnt/databricksudemyf1dl/raw/constructors.json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drivers.json as a table schema (single line, complex nested structure for one column)

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers
(
  driverId int,
  driverRef string,
  number int,
  code string,
  name STRUCT<forename: string, surname: string>,
  dob date,
  nationality string,
  url string
)
using json
options (path "/mnt/databricksudemyf1dl/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC results.json as a table schema (single line, simple structure)

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results
(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId int
)
using json
options (path "/mnt/databricksudemyf1dl/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC pitstops.json as a table schema (multi line, simple structure)

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops
(
  raceId int,
  driverId int,
  stop int,
  lap int,
  time string,
  duration string,
  milliseconds int
)
using json
options (path "/mnt/databricksudemyf1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC lap_times folder with multi csv

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times
(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv
options (path "/mnt/databricksudemyf1dl/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC qualifying folder with multi json multi-line files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying
(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string
)
using json
options (path "/mnt/databricksudemyf1dl/raw/qualifying", multiLine true)