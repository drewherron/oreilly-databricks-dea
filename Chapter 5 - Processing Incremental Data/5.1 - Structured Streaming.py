# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Implementing Structured Streaming

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/School-Setup

# COMMAND ----------

stream_df = spark.readStream.table("courses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Data Manipulations in SQL

# COMMAND ----------

stream_df.createOrReplaceTempView("courses_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This book cell fails by design (in any Spark environment, not just
# MAGIC -- UC/serverless): a plain batch SELECT cannot read a temp view backed by
# MAGIC -- a streaming DataFrame. Error: "Queries with streaming sources must be
# MAGIC -- executed with writeStream.start(), or from a streaming table or flow
# MAGIC -- definition within a Lakeflow Declarative Pipeline."
# MAGIC -- Commented out so Run All continues:
# MAGIC -- SELECT * FROM courses_streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same limitation as the cell above: aggregating a streaming view with a
# MAGIC -- batch SELECT fails ("must be executed with writeStream.start()").
# MAGIC -- The working path is cell 12's CREATE TEMP VIEW (consumed by
# MAGIC -- writeStream in cell 14), which keeps the whole logic in SQL.
# MAGIC -- Book cell (commented out so Run All continues):
# MAGIC -- SELECT instructor, count(course_id) AS total_courses
# MAGIC -- FROM courses_streaming_tmp_vw
# MAGIC -- GROUP BY instructor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT *
# MAGIC -- FROM courses_streaming_tmp_vw
# MAGIC -- ORDER BY instructor

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Persisting streaming data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW instructor_counts_tmp_vw AS (
# MAGIC  SELECT instructor, count(course_id) AS total_courses
# MAGIC  FROM courses_streaming_tmp_vw
# MAGIC  GROUP BY instructor
# MAGIC )

# COMMAND ----------

result_stream_df = spark.table("instructor_counts_tmp_vw")

# COMMAND ----------

# DBTITLE 1,Persisting streaming data (writeStream)
(result_stream_df.writeStream 
                 # Book cell: .trigger(processingTime='3 seconds') — a continuous, time-based
# trigger. Time-based triggers are NOT supported on serverless compute
# (INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED); use the one-shot availableNow
# trigger instead and re-run this cell to process data inserted since the
# last run.
                 .trigger(availableNow=True)
                 .outputMode("complete")
                 # Book checkpoint path dbfs:/mnt/DEA-Book/checkpoints/... → volume path
                 .option("checkpointLocation", "/Volumes/main/de_associate_school/dea_book/checkpoints/instructor_counts")
                 .table("instructor_counts")
                 .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM instructor_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO courses
# MAGIC values ("C16", "Generative AI", "Pierre B.", "Computer Science", 25),
# MAGIC        ("C17", "Embedded Systems", "Julia S.", "Computer Science", 30),
# MAGIC        ("C18", "Virtual Reality", "Bernard M.", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The book's continuous stream would refresh this table automatically.
# MAGIC -- With the one-shot availableNow trigger, re-run the stream cell above
# MAGIC -- to process the 3 new courses and refresh these counts.
# MAGIC SELECT * FROM instructor_counts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incremental batch processing

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO courses
# MAGIC values ("C19", "Compiler Design", "Sophie B.", "Computer Science", 25),
# MAGIC        ("C20", "Signal Processing", "Sam M.", "Computer Science", 30),
# MAGIC        ("C21", "Operating Systems", "Mark H.", "Computer Science", 35)

# COMMAND ----------

# DBTITLE 1,Incremental batch processing (writeStream)
(result_stream_df.writeStream                                    
                 # Incremental batch: the one-shot run processes whatever is new since
# the last checkpoint run and rewrites the complete-mode counts.
                 .trigger(availableNow=True)
                 .outputMode("complete")
                 .option("checkpointLocation", "/Volumes/main/de_associate_school/dea_book/checkpoints/instructor_counts")
                 .table("instructor_counts")
                 .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM instructor_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Data Manipulations in Python

# COMMAND ----------

import pyspark.sql.functions as F

output_stream_df = (stream_df.groupBy("instructor")
                   .agg(F.count("course_id").alias("total_courses")))

# COMMAND ----------

# Not headless-validatable: display() on a streaming DataFrame runs a
# display-backed streaming query. If serverless rejects it with
# INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED, the availableNow write in the
# next cell is the validated path.
display(output_stream_df)

# COMMAND ----------

(output_stream_df.writeStream                                    
                 .trigger(availableNow=True)
                 .outputMode("complete")
                 # Book checkpoint path dbfs:/mnt/DEA-Book/checkpoints/... → volume path
                 .option("checkpointLocation", "/Volumes/main/de_associate_school/dea_book/checkpoints/instructor_counts_py")
                 .table("instructor_counts_py")
                 .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM instructor_counts_py

# COMMAND ----------

instructor_counts_df = spark.read.table("instructor_counts_py")

display(instructor_counts_df)