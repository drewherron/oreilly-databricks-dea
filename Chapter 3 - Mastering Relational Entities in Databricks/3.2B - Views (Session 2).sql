-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## View Types

-- COMMAND ----------

-- DBTITLE 1,Use catalog main, show tables
-- The book uses the legacy hive_metastore catalog, which doesn't exist in
-- Unity Catalog-only workspaces (e.g. Databricks Free Edition).
-- See MODERNIZATION_NOTES.md in the repo root.
USE CATALOG main;

-- This is "Session 2" of the views topic: run in a fresh session after
-- 3.2A. Stored views (view_tesla_cars) persist across sessions, but the
-- session-scoped temp view (temp_view_cars_brands) from 3.2A is gone.
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Global temporary views

-- COMMAND ----------

-- DBTITLE 1,List global temp views (skipped)
-- (Book cell: SHOW TABLES IN global_temp;)
-- Listed the global temp views created in the previous session. The
-- global_temp namespace doesn't exist in a UC workspace, and global temp
-- views are unsupported on serverless anyway (see 3.2A).

-- COMMAND ----------

-- DBTITLE 1,Query global temp view (skipped)
-- (Book cell: SELECT * FROM global_temp.global_temp_view_recent_cars;)
-- Queried the global temp view across sessions. Skipped: the view was
-- never created (see 3.2A) and the global_temp namespace is unavailable
-- on serverless.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

-- DBTITLE 1,Drop stored view
-- IF EXISTS keeps the notebook re-runnable (a plain DROP VIEW fails
-- with VIEW_NOT_FOUND on re-run once the view is gone).
DROP VIEW IF EXISTS view_tesla_cars;

-- COMMAND ----------

-- DBTITLE 1,Drop global temp view (skipped)
-- (Book cell: DROP VIEW global_temp.global_temp_view_recent_cars;)
-- Skipped: the view was never created (see 3.2A) and the global_temp
-- namespace is unavailable on serverless.

-- COMMAND ----------

-- DBTITLE 1,Drop cars table
-- Clean-up cell (same IF EXISTS pattern for re-runnability).
DROP TABLE IF EXISTS cars;