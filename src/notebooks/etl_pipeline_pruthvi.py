# Databricks notebook source
# MAGIC %md ##Workshop- Data Pipeline in Practice (GR5068)

# COMMAND ----------

# MAGIC %md ##### Read in  Dataset

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

df_laptimes.count()

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_drivers.count()

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ##### Transform Data

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

df_drivers= df_drivers.withColumn("age",datediff(current_date(),df_drivers.dob)/365)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers =df_drivers.withColumn('age', df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

df_lap_drivers = df_drivers.select('driverId','driverRef', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers= df_lap_drivers.groupby('age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers.write.csv('s3://pp-gr5069/processed/laptimes_by_drivers.csv')

# COMMAND ----------

