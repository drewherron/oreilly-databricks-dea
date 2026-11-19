# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Auto Loader in Action

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/School-Setup

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_school}/enrollments-json-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setting up Auto Loader

# COMMAND ----------

# DBTITLE 1,Setting up Auto Loader (writeStream)
(spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.inferColumnTypes","true")
           # Book path dbfs:/mnt/DEA-Book/... → volume path
           .option("cloudFiles.schemaLocation", "/Volumes/main/de_associate_school/dea_book/checkpoints/enrollments")
           .load(f"{dataset_school}/enrollments-json-raw")
     .writeStream
           # Book path dbfs:/mnt/DEA-Book/... → volume path
           .option("checkpointLocation", "/Volumes/main/de_associate_school/dea_book/checkpoints/enrollments")
           # The book relies on Spark's default (continuous) trigger, which
           # is not supported on serverless — every streaming query needs an
           # explicit Trigger.AvailableNow there. One-shot: re-run this cell
           # after load_new_data() to ingest files added since the last run.
           .trigger(availableNow=True)
           .table("enrollments_updates")
           .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM enrollments_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Observing Auto Loader

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_school}/enrollments-json-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The book's continuous stream updated this count automatically.
# MAGIC -- With the one-shot availableNow trigger, re-run the Auto Loader cell
# MAGIC -- above to ingest the file added by load_new_data().
# MAGIC SELECT count(*) FROM enrollments_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY enrollments_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS enrollments_updates

# COMMAND ----------

# Book path dbfs:/mnt/DEA-Book/... → volume path (same dir as the schemaLocation)
dbutils.fs.rm("/Volumes/main/de_associate_school/dea_book/checkpoints/enrollments", True)

# COMMAND ----------

