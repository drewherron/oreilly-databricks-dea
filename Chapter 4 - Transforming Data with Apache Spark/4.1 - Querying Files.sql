-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Querying Data Files

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON Format

-- COMMAND ----------

-- MAGIC %run ../Includes/School-Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_school}/students-json")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 1,Query single JSON file
-- The book's ${dataset.school} substitution (spark.conf-based) is gone on
-- serverless, and widget parameter markers (:param) don't work for direct
-- file-path queries in SQL cells (FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY
-- when the path is bound at runtime). SQL cells therefore use the literal
-- volume path -- the same value as the dataset_school variable defined
-- by School-Setup. See MODERNIZATION_NOTES.md in the repo root.
SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json/export_001.json`

-- COMMAND ----------

-- DBTITLE 1,Query JSON files by wildcard
SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json/export_*.json`

-- COMMAND ----------

-- DBTITLE 1,Query JSON directory
SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`

-- COMMAND ----------

-- DBTITLE 1,Count JSON records
SELECT COUNT(1) FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`

-- COMMAND ----------

-- DBTITLE 1,JSON with source file names
-- input_file_name() is blocked in UC workspaces; the UC replacement is
-- the _metadata column group (the engine's suggested fix). See
-- MODERNIZATION_NOTES.md in the repo root.
SELECT *, _metadata.file_path AS source_file
FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying Using the text Format

-- COMMAND ----------

-- DBTITLE 1,Query as text format
SELECT * FROM text.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying Using binaryFile Format

-- COMMAND ----------

-- DBTITLE 1,Query as binaryFile format
SELECT * FROM binaryFile.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying Non-Self-Describing Formats

-- COMMAND ----------

-- DBTITLE 1,Query CSV directory
SELECT * FROM csv.`/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Registering Tables from Files with CTAS

-- COMMAND ----------

-- DBTITLE 1,CTAS students table from JSON
-- CREATE OR REPLACE keeps the notebook re-runnable from the top.
CREATE OR REPLACE TABLE students AS
SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`;

DESCRIBE EXTENDED students;

-- COMMAND ----------

-- DBTITLE 1,CTAS courses_unparsed from CSV
CREATE OR REPLACE TABLE courses_unparsed AS
SELECT * FROM csv.`/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv`;

SELECT * FROM courses_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Registering Tables on Foreign Data Sources

-- COMMAND ----------

-- DBTITLE 1,Create external CSV table in volume
-- UC RETHINK (see MODERNIZATION_NOTES.md): in Unity Catalog an external
-- table's LOCATION must be a cloud storage URI (s3://, abfss://, gs://)
-- inside a registered EXTERNAL LOCATION. A volume path can't be used
-- ("Missing cloud file system scheme"; the dbfs:/ prefix is rejected as
-- UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED), and Free Edition has
-- no external locations. So the book's
--   CREATE TABLE ... USING CSV ... LOCATION "<courses-csv dir>"
-- can't run here. The runnable equivalent is a session-scoped TEMP VIEW
-- over the same files -- and it still demonstrates the section's point:
-- the file listing is cached per session (see the cells below).
CREATE OR REPLACE TEMP VIEW courses_csv
 (course_id STRING, title STRING, instructor STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
 path = "/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv",
 header = "true",
 delimiter = ";")

-- COMMAND ----------

SELECT * FROM courses_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Limitation

-- COMMAND ----------

-- DBTITLE 1,Describe courses_csv
-- A temp view carries no table metadata, so unlike the book's external
-- table (which showed Type: EXTERNAL and a Location), this lists only
-- the columns.
DESCRIBE EXTENDED courses_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Impact of not having a Delta table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_school}/courses-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 1,Count before copy
-- Expect 12 (4 export files x 3 courses each).
SELECT COUNT(1) FROM courses_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp(f"{dataset_school}/courses-csv/export_001.csv",
-- MAGIC               f"{dataset_school}/courses-csv/copy_001.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_school}/courses-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 1,Count after copy (cached listing)
-- Still 12: the view's file listing was cached when it was first used
-- in this session, so the new copy_001.csv is invisible -- exactly the
-- caching limitation the book demonstrated with its external table.
SELECT COUNT(1) FROM courses_csv

-- COMMAND ----------

-- DBTITLE 1,Refresh the view (serverless way)
-- REFRESH TABLE is not supported on serverless compute
-- (NOT_SUPPORTED_WITH_SERVERLESS). The temp-view equivalent is to
-- re-create the view: that re-lists the directory and the copied file
-- becomes visible (validated 3 -> 6 in a scratch copy of this dataset).
CREATE OR REPLACE TEMP VIEW courses_csv
 (course_id STRING, title STRING, instructor STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
 path = "/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv",
 header = "true",
 delimiter = ";")

-- COMMAND ----------

-- DBTITLE 1,Count after refresh
-- Now 15: the re-created view's listing includes copy_001.csv
-- (12 original + 3 copied courses).
SELECT COUNT(1) FROM courses_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Hybrid approach

-- COMMAND ----------

-- DBTITLE 1,Hybrid temp view then CTAS
-- (The book's ${dataset.school} path in OPTIONS is written literally.
-- The export_*.csv wildcard deliberately EXCLUDES copy_001.csv from the
-- Limitation section above, so the managed `courses` table gets only
-- the 12 original courses -- the book's "hybrid approach": view for
-- ad-hoc file access, table for durable, Delta-backed data.)
CREATE OR REPLACE TEMP VIEW courses_tmp_vw
  (course_id STRING, title STRING, instructor STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
 path = "/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv/export_*.csv",
 header = "true",
 delimiter = ";"
);

CREATE OR REPLACE TABLE courses AS
 SELECT * FROM courses_tmp_vw;

-- COMMAND ----------

DESCRIBE EXTENDED courses

-- COMMAND ----------

SELECT * FROM courses