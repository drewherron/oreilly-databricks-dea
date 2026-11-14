-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Putting Relational Entities into Practice

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Working in the Default Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating managed tables

-- COMMAND ----------

-- DBTITLE 1,Use catalog main, create managed_default
-- The book uses the legacy hive_metastore catalog, which doesn't exist in
-- Unity Catalog-only workspaces (e.g. Databricks Free Edition).
-- See MODERNIZATION_NOTES.md in the repo root.
USE CATALOG main;

-- CREATE OR REPLACE (instead of the book's plain CREATE TABLE) keeps the
-- notebook re-runnable from the top, same fix as 2.1 - Delta Lake.
CREATE OR REPLACE TABLE managed_default
 (country STRING, code STRING, dial_code STRING);

INSERT INTO managed_default
VALUES ('France', 'Fr', '+33')

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating external tables

-- COMMAND ----------

-- DBTITLE 1,External table in default schema (skipped)
-- The book creates an external table on a DBFS mount:
--   CREATE TABLE external_default (country STRING, code STRING, dial_code STRING)
--   LOCATION 'dbfs:/mnt/demo/external_default';
--   INSERT INTO external_default VALUES ('France', 'Fr', '+33');
-- DBFS mounts (dbfs:/mnt/...) don't exist in this Unity Catalog-only
-- workspace, and UC requires external tables to live in a governed
-- external location or UC volume. With nothing to point at, the cells that
-- depend on external_default below are kept as comments describing what
-- the book showed. See MODERNIZATION_NOTES.md in the repo root.

-- COMMAND ----------

-- DBTITLE 1,Describe external_default (skipped)
-- (Book cell: DESCRIBE EXTENDED external_default)
-- Would show Type = EXTERNAL and the dbfs:/mnt/demo/... location. Skipped
-- because the external table was not created (see the cell above).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dropping tables

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- DBTITLE 1,Select after drop (expected error)
-- Expect an error (TABLE_OR_VIEW_NOT_FOUND): dropping a managed table
-- deletes its data files along with the metadata. Note that Run All stops
-- at this error -- run the remaining cells individually.
SELECT * FROM managed_default

-- COMMAND ----------

-- DBTITLE 1,Managed table files (skipped)
-- (Book cell: %fs ls 'dbfs:/user/hive/warehouse/managed_default')
-- Browsed the Hive warehouse directory to show the managed table's files
-- were deleted by the DROP above. UC-managed storage can't be browsed
-- with %fs; in UC, DROP TABLE on a managed table removes the data files
-- along with the metadata.

-- COMMAND ----------

-- DBTITLE 1,Drop external_default (skipped)
-- (Book cell: DROP TABLE external_default)
-- Showed that dropping an external table removes only the catalog
-- metadata -- its data files stay in the external location (next cells).

-- COMMAND ----------

-- DBTITLE 1,Select external_default (skipped)
-- (Book cell: SELECT * FROM external_default)
-- Demonstrated the table no longer exists in the catalog after the DROP.

-- COMMAND ----------

-- DBTITLE 1,External files still present (skipped)
-- (Book cell: %fs ls 'dbfs:/mnt/demo/external_default')
-- Showed the dropped external table's data files still on disk.

-- COMMAND ----------

-- DBTITLE 1,Read dropped external table by path (skipped)
-- (Book cell: SELECT * FROM DELTA.`dbfs:/mnt/demo/external_default`)
-- Read the dropped external table's data directly by file path, proving
-- the data survived the DROP. Path-based access to a DBFS mount isn't
-- possible in this UC workspace.

-- COMMAND ----------

-- DBTITLE 1,Remove external files (skipped)
-- MAGIC %python
-- MAGIC # (Book cell: dbutils.fs.rm('dbfs:/mnt/demo/external_default', True))
-- MAGIC # Manually deleted the dropped external table's orphaned files. dbutils.fs
-- MAGIC # is not available on serverless compute, and the mount doesn't exist
-- MAGIC # anyway.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Working in a New Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a new database

-- COMMAND ----------

-- DBTITLE 1,Create schema new_default
-- CREATE SCHEMA IF NOT EXISTS (instead of the book's plain CREATE SCHEMA)
-- keeps the notebook re-runnable from the top. (CREATE OR REPLACE SCHEMA
-- is not supported Databricks SQL syntax.)
CREATE SCHEMA IF NOT EXISTS new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables in the new database

-- COMMAND ----------

-- DBTITLE 1,Create managed table in new_default
USE DATABASE new_default;

-- create a managed table
-- CREATE OR REPLACE keeps the notebook re-runnable from the top.
CREATE OR REPLACE TABLE managed_new_default
 (country STRING, code STRING, dial_code STRING);

INSERT INTO managed_new_default
VALUES ('France', 'Fr', '+33');
-----------------------------------
-- The book also created an external table here:
--   CREATE TABLE external_new_default (country STRING, code STRING, dial_code STRING)
--   LOCATION 'dbfs:/mnt/demo/external_new_default';
--   INSERT INTO external_new_default VALUES ('France', 'Fr', '+33');
-- Skipped for the same reason as external_default above: no DBFS mounts
-- in a UC-only workspace. See MODERNIZATION_NOTES.md in the repo root.

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

-- DBTITLE 1,Describe external_new_default (skipped)
-- (Book cell: DESCRIBE EXTENDED external_new_default)
-- Would show the external table's mount location. Skipped because the
-- external table was not created.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dropping tables

-- COMMAND ----------

-- DBTITLE 1,Drop tables in new_default
DROP TABLE managed_new_default;

-- (The book also dropped external_new_default here; skipped because that
-- table was never created.)

-- COMMAND ----------

-- DBTITLE 1,Managed table files in schema (skipped)
-- (Book cell: %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default')
-- Browsed the schema's warehouse directory to see the managed table's
-- files. UC-managed storage can't be browsed with %fs; DESCRIBE EXTENDED
-- managed_new_default above shows the table's managed location instead.

-- COMMAND ----------

-- DBTITLE 1,External files in new schema (skipped)
-- (Book cell: %fs ls 'dbfs:/mnt/demo/external_new_default')
-- Showed the dropped external table's files still in the mount.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Working In a Custom-Location Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating the database

-- COMMAND ----------

-- DBTITLE 1,Custom-location schema (skipped)
-- (Book cell: CREATE SCHEMA custom LOCATION 'dbfs:/Shared/schemas/custom.db')
-- Sent a schema's managed tables to a custom storage directory. In a UC
-- workspace, managed storage is UC-controlled and a schema can only
-- override it with a MANAGED LOCATION in a governed external location,
-- which this workspace doesn't have. The remaining cells of this section
-- are kept as comments. See MODERNIZATION_NOTES.md in the repo root.

-- COMMAND ----------

-- DBTITLE 1,Describe custom schema (skipped)
-- (Book cell: DESCRIBE DATABASE EXTENDED custom)
-- Would show the schema's custom dbfs:/Shared/... storage location.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating tables

-- COMMAND ----------

-- DBTITLE 1,Tables in custom schema (skipped)
-- (Book cell: USE DATABASE custom;)

-- (The book created managed_custom (managed) and external_custom at
-- dbfs:/mnt/demo/external_custom, and inserted a row into each.)
-- Skipped: the custom schema was not created and the mount doesn't exist.

-- COMMAND ----------

-- DBTITLE 1,Describe managed_custom (skipped)
-- (Book cell: DESCRIBE EXTENDED managed_custom)
-- Would show the managed table stored under the schema's custom location.

-- COMMAND ----------

-- DBTITLE 1,Describe external_custom (skipped)
-- (Book cell: DESCRIBE EXTENDED external_custom)
-- Would show the external table's mount location.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dropping tables

-- COMMAND ----------

-- DBTITLE 1,Drop custom tables (skipped)
-- (Book cell: DROP TABLE managed_custom; DROP TABLE external_custom;)
-- Dropped both tables, then the two cells below compared what happened
-- to their files: managed gone, external still present.

-- COMMAND ----------

-- DBTITLE 1,Managed custom files (skipped)
-- (Book cell: %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom')
-- Showed the managed table's files deleted by the DROP.

-- COMMAND ----------

-- DBTITLE 1,External custom files (skipped)
-- (Book cell: %fs ls 'dbfs:/mnt/demo/external_custom')
-- Showed the dropped external table's files still on disk.