# Databricks notebook source
# MAGIC %md
# MAGIC ### data prep

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

dbutils.fs.rm("/mnt/filesystem/delta_disjoint_predicates/flights", recurse= True)
windowSpec = Window.orderBy(["date", "origin", "destination"])

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/databricks-datasets/flights/departuredelays.csv")
df = df.withColumn("id",row_number().over(windowSpec))
df.write.format("delta").mode("overwrite").partitionBy("origin").save("/mnt/filesystem/delta_disjoint_predicates/flights")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### intro to delta where clause predicates

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/filesystem/delta_disjoint_predicates/flights`

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/filesystem/delta_disjoint_predicates/flights/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`/mnt/filesystem/delta_disjoint_predicates/flights`
# MAGIC set delay = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`/mnt/filesystem/delta_disjoint_predicates/flights`
# MAGIC set delay = 0
# MAGIC where origin = "ABE"

# COMMAND ----------

# MAGIC %sql
# MAGIC ;with dict as 
# MAGIC (
# MAGIC   select 'ABE' as origin
# MAGIC )
# MAGIC update delta.`/mnt/filesystem/delta_disjoint_predicates/flights` as f
# MAGIC set delay = 0
# MAGIC where origin in (select origin from dict)

# COMMAND ----------

origin = "'ATL', 'ABE'"

query = """
  update delta.`/mnt/filesystem/delta_disjoint_predicates/flights`
  set delay = 0
  where origin in ({})
""".format(origin)

spark.sql(query)

# COMMAND ----------

dbutils.widgets.text("myOrigin", "ABE")

# COMMAND ----------

getArgument("myOrigin")

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`/mnt/filesystem/delta_disjoint_predicates/flights` as f
# MAGIC set delay = 0
# MAGIC where origin IN (getArgument("myOrigin"))

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### run data prep again

# COMMAND ----------

import time
import random
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

airports = ["ABE", "ATL", "DTW", "ORD"]
  
def insert_row(path_to_delta_table):
  max_id = spark.read.format("delta").load(path_to_delta_table).groupBy().max("id").collect()[0].asDict()['max(id)']
  schema = StructType([ \
    StructField("date",IntegerType(),True), \
    StructField("delay",IntegerType(),True), \
    StructField("distance",IntegerType(),True), \
    StructField("origin", StringType(), True), \
    StructField("destination", StringType(), True), \
    StructField("id", IntegerType(), True) \
  ])
  
  new_row = [(int(time.time())\
             ,random.randrange(-10, 10)\
             ,random.randrange(1000, 10000)\
             ,random.choice(airports)\
             ,random.choice(airports)\
             ,max_id+1)]
  
  df = spark.createDataFrame(data = new_row, schema=schema)
  df.write.format("delta").mode("append").save(path_to_delta_table)
  return max_id+1


def update_row(path_to_delta_table):
  max_id = spark.read.format("delta").load(path_to_delta_table).groupBy().max("id").collect()[0].asDict()['max(id)']
  row_id_to_update = random.randrange(1, max_id)
  
  deltaTable = DeltaTable.forPath(spark, path_to_delta_table)
  deltaTable.update(col("id") == row_id_to_update, { "delay": lit(0) } )  
  return row_id_to_update

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists flights;
# MAGIC 
# MAGIC create table flights
# MAGIC using delta
# MAGIC location "/mnt/filesystem/delta_disjoint_predicates/flights";
# MAGIC 
# MAGIC alter table flights
# MAGIC set tblproperties (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history flights

# COMMAND ----------

for i in range(2):
  insert_row("/mnt/filesystem/delta_disjoint_predicates/flights")

# COMMAND ----------

for i in range(2):
  update_row("/mnt/filesystem/delta_disjoint_predicates/flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, delay, distance, origin, destination, id FROM table_changes("flights", 2)
# MAGIC WHERE _change_type != "update_preimage"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS flights_dump;
# MAGIC 
# MAGIC CREATE TABLE flights_dump CLONE flights VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO flights_dump AS tgt
# MAGIC USING (
# MAGIC   SELECT date, delay, distance, origin, destination, id FROM table_changes("flights", 2)
# MAGIC   WHERE _change_type != "update_preimage"
# MAGIC ) AS src
# MAGIC ON src.id = tgt.id AND src.origin = tgt.origin
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.delay = src.delay
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO flights_dump AS tgt
# MAGIC USING (
# MAGIC   SELECT date, delay, distance, origin, destination, id FROM table_changes("flights", 2)
# MAGIC   WHERE _change_type != "update_preimage"
# MAGIC ) AS src
# MAGIC ON src.id = tgt.id AND src.origin = tgt.origin AND tgt.origin IN ("ABE")
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.delay = src.delay
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------


spark.sql("""
MERGE INTO flights_dump AS tgt
USING (
  SELECT date, delay, distance, origin, destination, id FROM table_changes("flights", 2)
  WHERE _change_type != "update_preimage"
) AS src
ON src.id = tgt.id AND src.origin = tgt.origin --AND tgt.origin IN ({})
WHEN MATCHED 
  THEN UPDATE SET tgt.delay = src.delay
WHEN NOT MATCHED 
  THEN INSERT *
""".format("'ABE', 'ATL'"))