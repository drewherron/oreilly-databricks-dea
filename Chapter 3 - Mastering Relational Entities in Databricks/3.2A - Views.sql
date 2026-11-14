-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploring Views

-- COMMAND ----------

-- DBTITLE 1,Use catalog main, create cars table
-- The book uses the legacy hive_metastore catalog, which doesn't exist in
-- Unity Catalog-only workspaces (e.g. Databricks Free Edition).
-- See MODERNIZATION_NOTES.md in the repo root.
USE CATALOG main;

-- The book's CREATE TABLE IF NOT EXISTS + INSERT would append duplicate
-- rows on every re-run (IF NOT EXISTS skips the CREATE but the INSERT
-- still runs). CREATE OR REPLACE resets the table instead.
CREATE OR REPLACE TABLE cars
(id INT, model STRING, brand STRING, year INT);

INSERT INTO cars
VALUES (1, 'Cybertruck', 'Tesla', 2024),
     (2, 'Model S', 'Tesla', 2023),
     (3, 'Model Y', 'Tesla', 2022),
     (4, 'Model X 75D', 'Tesla', 2017),
     (5, 'G-Class G63', 'Mercedes-Benz', 2024),
     (6, 'E-Class E200', 'Mercedes-Benz', 2023),
     (7, 'C-Class C300', 'Mercedes-Benz', 2016),
     (8, 'Everest', 'Ford', 2023),
     (9, 'Puma', 'Ford', 2021),
     (10, 'Focus', 'Ford', 2019)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View Types

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stored views

-- COMMAND ----------

-- DBTITLE 1,Create view_tesla_cars
-- CREATE OR REPLACE keeps the notebook re-runnable from the top
-- (a plain CREATE VIEW fails with VIEW_ALREADY_EXISTS on re-run).
CREATE OR REPLACE VIEW view_tesla_cars
AS SELECT *
   FROM cars
   WHERE brand = 'Tesla';

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM view_tesla_cars;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Temporary views

-- COMMAND ----------

-- DBTITLE 1,Create temp view car brands
CREATE OR REPLACE TEMP VIEW temp_view_cars_brands
AS  SELECT DISTINCT brand
   FROM cars;

SELECT * FROM temp_view_cars_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Global temporary views

-- COMMAND ----------

-- DBTITLE 1,Global temp view (skipped)
-- (Book cell: CREATE GLOBAL TEMP VIEW global_temp_view_recent_cars
--   AS SELECT * FROM cars WHERE year >= 2022 ORDER BY year DESC;)
-- Global temp views are NOT supported on serverless compute in a UC
-- workspace (NOT_SUPPORTED_WITH_SERVERLESS, validated 2026-11-14) --
-- a session-wide temp view has no place to live when the session catalog
-- is Unity Catalog. Plain TEMP VIEWs (previous section) still work; they
-- are just scoped to the current session instead of shared across
-- sessions. See MODERNIZATION_NOTES.md in the repo root.

-- COMMAND ----------

-- DBTITLE 1,Query global temp view (skipped)
-- (Book cell: SELECT * FROM global_temp.global_temp_view_recent_cars;)
-- Queried the global temp view through the special global_temp
-- namespace. Skipped because the view was never created.

-- COMMAND ----------

-- DBTITLE 1,Show tables (temp views not listed)
-- The book's point: the global temp view doesn't appear here --
-- SHOW TABLES lists only persistent tables and views in the current
-- schema, never temp views of either kind.
SHOW TABLES;

-- COMMAND ----------

-- DBTITLE 1,List global temp views (skipped)
-- (Book cell: SHOW TABLES IN global_temp;)
-- Listed the session's global temp views. The global_temp namespace
-- doesn't exist in a UC workspace, and global temp views are unsupported
-- on serverless anyway.