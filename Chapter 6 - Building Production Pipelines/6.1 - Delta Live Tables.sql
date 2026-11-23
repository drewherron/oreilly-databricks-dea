-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Implementing DLT Pipelines

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/oreilly-databricks-dea/main/Includes/Images/school_schema.png" alt="School Schema">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 1,Pipeline setup (this fork)
-- MAGIC %md
-- MAGIC ## Pipeline setup (this fork)
-- MAGIC
-- MAGIC The book's *DLT pipeline* is now a **Lakeflow Spark Declarative Pipeline (SDP)**
-- MAGIC ("Pipelines" in the workspace UI). To run this notebook as the book intends:
-- MAGIC
-- MAGIC 1. Run `../Includes/School-Setup` once (creates the `school_dlt_db` schema and
-- MAGIC    downloads the dataset into the `dea_book` volume).
-- MAGIC 2. Go to **Pipelines > Create pipeline** — serverless is the default (and the
-- MAGIC    only option on Free Edition; note the free-tier limit of one active
-- MAGIC    pipeline per pipeline type).
-- MAGIC 3. Set **Destination**: catalog `main`, schema `school_dlt_db`, and add this
-- MAGIC    notebook as the pipeline source. Then start the update.
-- MAGIC
-- MAGIC The cells below are pipeline source code — they only execute inside the
-- MAGIC pipeline, not as normal notebook cells. Changes made for this workspace (see
-- MAGIC `MODERNIZATION_NOTES.md`): the legacy `LIVE.` schema prefix is removed, the
-- MAGIC legacy `cloud_files()` call is now `read_files()`, and `${school.dataset_path}`
-- MAGIC substitutions are literal volume paths.

-- COMMAND ----------

-- (Book cell: SET school.dataset_path=dbfs:/mnt/DE-Associate-Book/datasets/school;)
-- This is gone on this platform: SQL SET of arbitrary keys is blocked on
-- serverless compute (CONFIG_NOT_AVAILABLE), ${...} substitution no longer
-- happens in SQL cells, and DBFS mounts don't exist in a UC-only workspace.
-- The dataset now lives in the UC volume created by ../Includes/School-Setup,
-- so the queries below use literal volume paths instead.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a streaming table

-- COMMAND ----------

-- (Book cell: cloud_files("${school.dataset_path}/enrollments-dlt-raw", "json",
--   map("cloudFiles.inferColumnTypes", "true")))
-- Modernized: cloud_files() is the legacy DLT SQL name — current pipeline SQL
-- uses read_files() with named options, and the STREAM keyword configures the
-- streaming read. The ${school.dataset_path} conf no longer exists, so the
-- volume path is written literally (option keys containing dots need backticks).
CREATE OR REFRESH STREAMING TABLE enrollments_raw
COMMENT "The raw courses enrollments, ingested from enrollments-dlt-raw folder"
AS SELECT * FROM STREAM read_files("/Volumes/main/de_associate_school/dea_book/datasets/school/enrollments-dlt-raw",
                                   format => "json",
                                   `cloudFiles.inferColumnTypes` => "true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a materialized view

-- COMMAND ----------

-- (Book cell: CREATE OR REPLACE MATERIALIZED VIEW ... FROM
--   json.`${school.dataset_path}/students-json`)
-- Modernized: pipeline SQL uses CREATE OR REFRESH (not OR REPLACE), and the
-- path is a literal volume path — the ${school.dataset_path} conf is gone.
-- (Note: this students lands in the pipeline's target schema school_dlt_db,
-- separate from the students table built in chapter 4.)
CREATE OR REFRESH MATERIALIZED VIEW students
COMMENT "The students lookup table, ingested from students-json"
AS SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Silver layer

-- COMMAND ----------

-- (Book cells referenced sources with the legacy LIVE. schema prefix:
--   STREAM(LIVE.enrollments_raw), LIVE.students. In new pipelines the LIVE
--   prefix is silently ignored, and current pipeline SQL just uses the names.)
CREATE OR REFRESH STREAMING TABLE enrollments_cleaned (
 CONSTRAINT valid_order_number EXPECT (enroll_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned courses enrollments with valid enroll_id"
AS
 SELECT enroll_id, quantity, o.student_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
        cast(from_unixtime(enroll_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) formatted_timestamp,
        o.courses, c.profile:address:country as country
 FROM STREAM(enrollments_raw) o
 LEFT JOIN students c
   ON o.student_id = c.student_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold layer

-- COMMAND ----------

-- (Book cell: CREATE OR REPLACE MATERIALIZED VIEW ... FROM LIVE.enrollments_cleaned)
-- Modernized: CREATE OR REFRESH is the current pipeline SQL form; the legacy
-- LIVE. prefix is dropped.
CREATE OR REFRESH MATERIALIZED VIEW uk_daily_student_courses
COMMENT "Daily number of courses per student in United Kingdom"
AS
 SELECT student_id, f_name, l_name, date_trunc("DD", formatted_timestamp) order_date, sum(quantity) courses_counts
 FROM enrollments_cleaned
 WHERE country = "United Kingdom"
 GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Configuring DLT Pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Modifying DLT pipelines

-- COMMAND ----------

-- (Book cell: CREATE OR REPLACE MATERIALIZED VIEW ... FROM LIVE.enrollments_cleaned)
-- Modernized: CREATE OR REFRESH is the current pipeline SQL form; the legacy
-- LIVE. prefix is dropped.
CREATE OR REFRESH MATERIALIZED VIEW fr_daily_student_courses
COMMENT "Daily number of courses per student in France"
AS
 SELECT student_id, f_name, l_name, date_trunc("DD", formatted_timestamp) order_date, sum(quantity) courses_counts
 -- FROM enrollments_cleaned
 FROM enrollments_cleaned
 WHERE country = "France"
 GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)