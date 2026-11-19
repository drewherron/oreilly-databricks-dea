# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Building Medallion Architectures

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
# MAGIC ## Establishing the bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Configuring Auto Loader

# COMMAND ----------

# DBTITLE 1,Bronze layer (Auto Loader writeStream)
import pyspark.sql.functions as F

(spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.inferColumnTypes","true")
           .option("cloudFiles.schemaLocation", f"{checkpoint_path}/enrollments_bronze")
           .load(f"{dataset_school}/enrollments-json-raw")
           .select("*",
                   F.current_timestamp().alias("arrival_time"),
                   # Book uses F.input_file_name() — blocked in Unity Catalog
                   # (UC_COMMAND_NOT_SUPPORTED); _metadata.file_path is the UC replacement.
                   F.col("_metadata.file_path").alias("source_file"))
     .writeStream
           .format("delta")
           .option("checkpointLocation", f"{checkpoint_path}/enrollments_bronze")
           .outputMode("append")
           # The book relies on Spark's default (continuous) trigger, which is
           # not supported on serverless — every streaming query needs an
           # explicit Trigger.AvailableNow. One-shot: after load_new_data(),
           # re-run this cell to ingest files added since the last run.
           .trigger(availableNow=True)
           .table("enrollments_bronze")
           .awaitTermination()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM enrollments_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The book's continuous stream ingested the new file automatically.
# MAGIC -- With the one-shot availableNow trigger, re-run the bronze cell above
# MAGIC -- first to see the count grow here.
# MAGIC SELECT count(1) FROM enrollments_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating a static lookup table

# COMMAND ----------

students_lookup_df = (spark.read
                       .format("json")
                       .load(f"{dataset_school}/students-json"))

# COMMAND ----------

display(students_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transitioning to the silver layer

# COMMAND ----------

enrollments_enriched_df = (spark.readStream
     .table("enrollments_bronze")
     .where("quantity > 0")
     .withColumn("formatted_timestamp", F.from_unixtime("enroll_timestamp", "yyyy-MM-dd HH:mm:ss").cast("timestamp") )
     .join(students_lookup_df, "student_id")
     .select("enroll_id", "quantity", "student_id", "email", "formatted_timestamp", "courses")
)

# COMMAND ----------

# DBTITLE 1,Silver layer (writeStream)
(enrollments_enriched_df.writeStream
                       .format("delta")
                       .option("checkpointLocation", f"{checkpoint_path}/enrollments_silver")
                       .outputMode("append")
                       # Serverless requires an explicit trigger on every
                       # streaming query. One-shot: re-run this cell after
                       # re-running the bronze cell to pick up new rows.
                       .trigger(availableNow=True)
                       .table("enrollments_silver")
                       .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-run the bronze and silver cells above (in that order) to see the
# MAGIC -- newly loaded enrollments flow through to silver.
# MAGIC SELECT count(1) FROM enrollments_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advancing to the gold layer

# COMMAND ----------

enrollments_agg_df = (spark.readStream
                            .table("enrollments_silver")
                            .withColumn("day", F.date_trunc("DD", "formatted_timestamp"))
                            .groupBy("student_id", "email", "day")
                            .agg(F.sum("quantity").alias("courses_counts"))
                            .select("student_id", "email", "day", "courses_counts")
)

# COMMAND ----------

# DBTITLE 1,Gold layer (writeStream)
(enrollments_agg_df.writeStream
                   .format("delta")
                   .outputMode("complete")
                   .option("checkpointLocation", f"{checkpoint_path}/daily_student_courses")
                   .trigger(availableNow=True)
                   .table("daily_student_courses")
                   # awaitTermination() added: with a one-shot trigger it
                   # returns as soon as the batch completes, so the table is
                   # ready for the next cell's SELECT.
                   .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_student_courses

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-run the bronze, silver, and gold cells above (in that order) to
# MAGIC -- refresh the aggregates with the newly loaded enrollments.
# MAGIC SELECT * FROM daily_student_courses

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

# With the one-shot availableNow triggers used above, the streams
# terminate themselves after each run — this loop from the book simply
# finds nothing to stop. (Always-on streams would need Lakeflow pipelines
# continuous mode or classic compute; time-based triggers are blocked on
# serverless.)
for s in spark.streams.active:
   print("Stopping stream: " + s.id)
   s.stop()
   s.awaitTermination()