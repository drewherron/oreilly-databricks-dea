-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Examining DLT pipelines

-- COMMAND ----------

-- DBTITLE 1,About this notebook (this fork)
-- MAGIC %md
-- MAGIC ### About this notebook (this fork)
-- MAGIC
-- MAGIC Explores the output of the **6.1 pipeline** — run that pipeline first
-- MAGIC (see the setup cell at the top of 6.1). The book browses the pipeline's
-- MAGIC DBFS storage (`dbfs:/mnt/DEA-Book/dlt/...`); in a UC pipeline there is no
-- MAGIC browsable storage root — the event log is a hidden Delta table queried with
-- MAGIC `event_log()`, and the published tables live in `main.school_dlt_db`.
-- MAGIC Cells 4 and 6 need a placeholder swapped for your pipeline ID / assume the
-- MAGIC pipeline has run.

-- COMMAND ----------

-- (Book cell: %fs ls "dbfs:/mnt/DEA-Book/dlt")
-- A UC pipeline has no browsable storage root like this. Its event log is a
-- hidden Delta table in the pipeline's target schema (main.school_dlt_db),
-- and its published tables are Unity Catalog objects. The cells below give
-- the modern equivalent of each book cell.

-- COMMAND ----------

-- (Book cell: %fs ls "dbfs:/mnt/DEA-Book/dlt/system/events")
-- The pipeline's system/events directory is not browsable in a UC pipeline.
-- The event log is queried with the event_log() function instead — see the
-- next cell.

-- COMMAND ----------

-- (Book cell: SELECT * FROM delta.`dbfs:/mnt/DEA-Book/dlt/system/events`)
-- Modernized: query the pipeline's event log with event_log(), passing your
-- pipeline's ID — find it in the Pipelines UI (pipeline details pane) or the
-- pipeline URL. By default the log is a hidden Delta table named
-- event_log_<pipeline_id> in the pipeline's target schema; event_log() is
-- the supported way to read it. Run this after the 6.1 pipeline has
-- completed at least one update.
SELECT * FROM event_log("<your-pipeline-id>")

-- Alternative: the pipeline_events system table (Beta) — same log, no
-- event_log() ownership rules, and filterable columns. Swap the ID in the
-- same way:
-- SELECT event_time, level, event_type, message
-- FROM system.lakeflow_pipeline_events_preview.pipeline_events
-- WHERE pipeline_id = "<your-pipeline-id>"
-- ORDER BY event_time

-- COMMAND ----------

-- (Book cell: %fs ls dbfs:/mnt/DEA-Book/dlt/schemas/school_dlt_db/tables)
-- Modernized: the pipeline's published tables are Unity Catalog objects in
-- main.school_dlt_db — list them there instead of browsing a storage path.
SHOW TABLES IN main.school_dlt_db

-- COMMAND ----------

-- (Book cell: SELECT * FROM hive_metastore.school_dlt_db.uk_daily_student_courses)
-- Modernized: the pipeline publishes to Unity Catalog — main.school_dlt_db
-- (created by ../Includes/School-Setup), not hive_metastore. Requires the 6.1
-- pipeline to have run at least once.
SELECT * FROM main.school_dlt_db.uk_daily_student_courses