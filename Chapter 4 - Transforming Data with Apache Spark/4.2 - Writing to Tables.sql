-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Writing to Tables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/School-Setup

-- COMMAND ----------

-- DBTITLE 1,Create enrollments (book: CTAS from parquet)
-- (Book cell: CREATE TABLE ... FROM parquet.`${dataset.school}/enrollments`.
-- ${dataset.school} substitution is dead on serverless — literal volume
-- path instead. CREATE OR REPLACE (not the book's plain CREATE TABLE) so
-- the notebook is re-runnable from the top without a leftover-table error.)
CREATE OR REPLACE TABLE enrollments AS
SELECT * FROM parquet.`/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments`

-- COMMAND ----------

SELECT * FROM enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Replacing Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. CREATE OR REPLACE TABLE statement

-- COMMAND ----------

-- DBTITLE 1,Replace via CREATE OR REPLACE TABLE
-- (Book cell used ${dataset.school}; literal volume path instead.)
CREATE OR REPLACE TABLE enrollments AS
SELECT * FROM parquet.`/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments`

-- COMMAND ----------

DESCRIBE HISTORY enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. INSERT OVERWRITE

-- COMMAND ----------

-- DBTITLE 1,Replace via INSERT OVERWRITE
-- (Book cell used ${dataset.school}; literal volume path instead.)
INSERT OVERWRITE enrollments
SELECT * FROM parquet.`/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments`

-- COMMAND ----------

DESCRIBE HISTORY enrollments

-- COMMAND ----------

-- DBTITLE 1,input_file_name() note (UC-blocked)
-- (Book cell: commented out — it shows you *could* add input_file_name()
-- to trace which source file each row came from. In UC workspaces
-- input_file_name() is blocked (UC_COMMAND_NOT_SUPPORTED); the UC
-- replacement is the _metadata column group:
--   SELECT *, _metadata.file_path AS source_file FROM parquet.`...`)
-- INSERT OVERWRITE enrollments
-- SELECT *, input_file_name() FROM parquet.`/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

-- DBTITLE 1,Append via INSERT INTO
-- (Book cell used ${dataset.school}; literal volume path instead.)
INSERT INTO enrollments
SELECT * FROM parquet.`/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments-new`

-- COMMAND ----------

SELECT count(1) FROM enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

-- DBTITLE 1,Temp view over students-json-new
-- (Book cell used ${dataset.school}; literal volume path instead.)
CREATE OR REPLACE TEMP VIEW students_updates AS
SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json-new`;

-- COMMAND ----------

-- DBTITLE 1,MERGE into students (typo fixed)
-- (Fixes the book's typo 'c. student_id' / 'u. student_id' → no space after
-- the dot, which Spark SQL rejects. The students table was created in 4.1.)
MERGE INTO students c
USING students_updates u
ON c.student_id = u.student_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
 UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 1,Temp view over courses-csv-new
CREATE OR REPLACE TEMP VIEW courses_updates
  (course_id STRING, title STRING, instructor STRING,
   category STRING, price DOUBLE)
USING CSV
OPTIONS (
 path = "/Volumes/main/de_associate_school/dea_book/datasets/school/courses-csv-new",
 header = "true",
 delimiter = ";"
);

-- COMMAND ----------

MERGE INTO courses c
USING courses_updates u
ON c.course_id = u.course_id AND c.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN
 INSERT *