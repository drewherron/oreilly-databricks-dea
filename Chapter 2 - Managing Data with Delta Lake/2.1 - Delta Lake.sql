-- Databricks notebook source
-- DBTITLE 1,Use catalog
-- MAGIC %md
-- MAGIC # Working with Delta Lake Tables

-- COMMAND ----------

-- DBTITLE 1,Use catalog
-- The book uses the legacy hive_metastore catalog, which doesn't exist in
-- Unity Catalog-only workspaces (e.g. Databricks Free Edition).
-- See MODERNIZATION_NOTES.md in the repo root.
USE CATALOG main

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Tables

-- COMMAND ----------

-- DBTITLE 1,Create product_info table
-- The book uses plain CREATE TABLE, which only works on a first run. If a
-- previous session left the table behind (the DROP TABLE at the end of this
-- notebook never runs when an earlier cell errors), a re-run fails with
-- TABLE_OR_VIEW_ALREADY_EXISTS. CREATE OR REPLACE makes the notebook
-- re-runnable from the top: it drops any leftover table and recreates it
-- fresh. See MODERNIZATION_NOTES.md in the repo root.
CREATE OR REPLACE TABLE product_info (
  product_id INT,
  product_name STRING,
  category STRING,
  price DOUBLE,
  quantity INT
)
USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

INSERT INTO product_info (product_id, product_name, category, price, quantity)
VALUES (1, 'Winter Jacket', 'Clothing', 79.95, 100);


INSERT INTO product_info (product_id, product_name, category, price, quantity)
VALUES
 (2, 'Microwave', 'Kitchen', 249.75, 30),
 (3, 'Board Game', 'Toys', 29.99, 75),
 (4, 'Smartcar', 'Electronics', 599.99, 50);

-- COMMAND ----------

SELECT * FROM product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring the Table Directory

-- COMMAND ----------

DESCRIBE DETAIL product_info

-- COMMAND ----------

-- The book listed the table's storage directory here
-- (dbfs:/user/hive/warehouse/product_info) to show the data files created by
-- the INSERT. In a Unity Catalog workspace, managed tables live in
-- UC-controlled storage that %fs cannot browse. The DESCRIBE DETAIL output
-- above shows the storage location, the number of files (numFiles), and the
-- total size (sizeInBytes) instead.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Delta Lake Tables

-- COMMAND ----------

UPDATE product_info
SET price = price + 10
WHERE product_id = 3

-- COMMAND ----------

-- The book re-listed the directory here to show that the UPDATE created new
-- data files: Delta tables are immutable, so an UPDATE rewrites affected rows
-- to new files rather than modifying files in place. That effect shows up in
-- the numFiles column of the DESCRIBE DETAIL output below, which has grown
-- since the insert.

-- COMMAND ----------

DESCRIBE DETAIL product_info

-- COMMAND ----------

-- See updated price on product_id 3
SELECT * FROM product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY product_info

-- COMMAND ----------

-- The book listed the _delta_log directory here, where Delta records each
-- table version as a JSON "commit" file (00000000000000000000.json,
-- 00000000000000000001.json, and so on). Unity Catalog manages this storage
-- and it cannot be browsed with %fs -- DESCRIBE HISTORY above reads the same
-- transaction log and is its table-form view: one row per commit file.

-- COMMAND ----------

-- The book printed the raw commit JSON (00000000000000000003.json) here,
-- showing the individual actions (add file, remove file, ...) that make up
-- version 3 of the table. Those raw files aren't readable in UC-managed
-- storage; the version-3 row of DESCRIBE HISTORY carries the same
-- information in table form.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exploring Delta Time Travel

-- COMMAND ----------

DESCRIBE HISTORY product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying Older Versions

-- COMMAND ----------

SELECT * FROM product_info VERSION AS OF 2

-- COMMAND ----------

SELECT * FROM product_info@v2
ORDER BY product_id

-- COMMAND ----------

SELECT * FROM product_info@v3
ORDER BY product_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rolling Back to Previous Versions

-- COMMAND ----------

DELETE FROM product_info

-- COMMAND ----------

DESCRIBE HISTORY product_info

-- COMMAND ----------

RESTORE TABLE product_info TO VERSION AS OF 3

-- COMMAND ----------

SELECT * FROM product_info
ORDER BY product_id

-- COMMAND ----------

DESCRIBE HISTORY product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimizing Delta Lake Tables

-- COMMAND ----------

DESCRIBE DETAIL product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Z-Order Indexing

-- COMMAND ----------

OPTIMIZE product_info
ZORDER BY product_id

-- COMMAND ----------

DESCRIBE DETAIL product_info

-- COMMAND ----------

DESCRIBE HISTORY product_info

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Vacuuming

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # The book listed the table directory here to count the files before
-- MAGIC # vacuuming. DESCRIBE DETAIL can't be used as a SQL subquery, but in Python
-- MAGIC # it returns a DataFrame, so you can select just the file count and size:
-- MAGIC spark.sql("DESCRIBE DETAIL product_info").select("numFiles", "sizeInBytes").show(truncate=False)

-- COMMAND ----------

VACUUM product_info

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # VACUUM with the default 7-day retention removed nothing here, because no
-- MAGIC # files are old enough to delete yet -- numFiles and sizeInBytes are
-- MAGIC # unchanged from the cell above.
-- MAGIC spark.sql("DESCRIBE DETAIL product_info").select("numFiles", "sizeInBytes").show(truncate=False)

-- COMMAND ----------

-- This cell fails ON PURPOSE: 0 hours is below Delta's 7-day safety minimum
-- for VACUUM retention. The book's next cell disables the safety check; on
-- serverless compute that config is blocked, so we use a table property
-- instead (see the next cell).
VACUUM product_info RETAIN 0 HOURS

-- COMMAND ----------

-- The book disabled the safety check with:
--   SET spark.databricks.delta.retentionDurationCheck.enabled = false
-- On serverless compute that config is blocked (CONFIG_NOT_AVAILABLE) in both
-- SQL SET and spark.conf.set. The working alternative: set the retention as
-- a TABLE property, then run a plain VACUUM with no RETAIN clause (next cell).
-- (For a real production table, reset this property to 'interval 1 week'
-- afterwards.)
ALTER TABLE product_info
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'INTERVAL 0 HOURS')

-- COMMAND ----------

-- With the 0-hour retention table property in place, a plain VACUUM removes
-- every file not needed by the current table version:
VACUUM product_info

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # After the plain VACUUM, the files belonging to old table versions are gone.
-- MAGIC # numFiles may not visibly drop here -- OPTIMIZE already consolidated the
-- MAGIC # active data into one file, and numFiles counts only active files. The real
-- MAGIC # proof comes in the next cell: time travel to @v1 no longer works.
-- MAGIC spark.sql("DESCRIBE DETAIL product_info").select("numFiles", "sizeInBytes").show(truncate=False)

-- COMMAND ----------

-- Expect an error: the vacuum deleted the files for older versions, so time
-- travel beyond the current version fails. This is VACUUM's trade-off --
-- it reclaims storage but destroys time-travel history.
SELECT * FROM product_info@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Dropping Delta Lake Tables

-- COMMAND ----------

DROP TABLE product_info

-- COMMAND ----------

-- The book listed the table directory to confirm the files were gone after
-- DROP TABLE. With a UC-managed table, DROP deletes the data files along
-- with the metadata. Confirm the table no longer exists in the catalog:
SELECT table_catalog, table_schema, table_name
FROM main.information_schema.tables
WHERE table_name = 'product_info'
-- (no rows expected)