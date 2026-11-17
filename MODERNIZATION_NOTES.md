# Modernization Notes

> Context for future assistant sessions (and anyone else using this fork): this
> fork of the O'Reilly *Databricks Certified Data Engineer Associate* study guide
> is being updated as we go to match current Databricks. The book predates several
> platform changes, so some exercises fail as written.

## Environment

* Workspace: Databricks **Free Edition** (serverless-only).
* **Unity Catalog–only**: there is no legacy `hive_metastore` catalog, no Hive
  warehouse directory (`dbfs:/user/hive/warehouse/...`), and no DBFS mounts
  (`dbfs:/mnt/...`). The book's claim that `hive_metastore` exists "by default in
  every Databricks workspace" is out of date — new workspaces are UC-enabled by
  default, and Free Edition excludes all legacy features.
* Catalogs available: `main` (default), `dbacademy` (created by a Learn/dbdemos
  lesson — **not** by this book's setup), `samples`, `system`, `workspace`.

## Conventions for edits

* Replace `USE CATALOG hive_metastore` with `USE CATALOG main`; unqualified
  table names then land in `main.default`.
* Cells like `%fs ls dbfs:/user/hive/warehouse/<table>` assume the legacy Hive
  warehouse path. In UC workspaces, managed-table storage is UC-controlled and
  not browsable that way — use `DESCRIBE DETAIL <table>` (see the `location`
  column) to show where the data lives instead.
* Where the book says "Delta Live Tables"/"DLT", the current product name is
  **Lakeflow Spark Declarative Pipelines** (still created with `@dlt.table`-style
  code, but renamed on the platform).

## Known issues

| Location | Issue | Status |
| --- | --- | --- |
| Includes/School-Setup | `hive_metastore` + DBFS mounts + `s3://` source for the school dataset; chapters 5–6 depend on its output | Not yet reworked |
| Chapter 2/2.1 – Delta Lake, cell 2 | `USE CATALOG hive_metastore` fails | **Fixed 2026-11-13** → `USE CATALOG main` |
| Chapter 2/2.1 – Delta Lake | 8 cells doing `%fs ls`/`%fs head` on `dbfs:/user/hive/warehouse/product_info` | **Fixed 2026-11-13 (Option A)** – explanatory comments + `DESCRIBE DETAIL` + `information_schema` check |
| Chapter 2/2.1 – Delta Lake, cell 4 | Plain `CREATE TABLE` fails on re-run (`TABLE_OR_VIEW_ALREADY_EXISTS`) when a previous session left the table behind | **Fixed 2026-11-14** → `CREATE OR REPLACE TABLE` (drops + recreates, notebook re-runnable from the top) |
| Chapter 3: 3.1, 3.2A, 3.2B | `hive_metastore` references — may be pedagogical (teaching legacy vs UC); fix in context when reached | 3.1 & 3.2A **Fixed 2026-11-14**; 3.2B **Fixed 2026-11-16** — Chapter 3 complete |
| Chapter 6/6.2 – Output Exploration | `hive_metastore` reference | Not yet reached |

## Changelog

* 2026-11-13: Created this file. Fixed `2.1 - Delta Lake` cell 2 to use
  `USE CATALOG main`.
* 2026-11-13: Modernized the 8 `%fs` cells in `2.1 - Delta Lake` (Option A):
  comment-only cells where the book was just showing files, `DESCRIBE DETAIL`
  where the file count was the point, and an `information_schema` check to
  verify the managed-table DROP. Full notebook validated end-to-end.
* 2026-11-13: Notebook owner asked to see just `numFiles` — confirmed
  `SELECT ... FROM (DESCRIBE DETAIL t)` is NOT valid SQL; the working pattern
  is a Python cell: `spark.sql("DESCRIBE DETAIL t").select("numFiles",
  "sizeInBytes").show()`. Applied to the 3 vacuum-section cells (formerly
  cells 39/41/45). Note: `COUNT(DISTINCT input_file_name())` also fails
  (non-deterministic expression in aggregate).
* 2026-11-13: Vacuum section reworked. The book's
  `SET spark.databricks.delta.retentionDurationCheck.enabled = false` is
  blocked on serverless compute (CONFIG_NOT_AVAILABLE) via both SQL `SET` and
  `spark.conf.set`, and `DeltaTable.vacuum()` has no `enforceRetentionDuration`
  parameter in this runtime. Working pattern (validated on a scratch table):
  `ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' =
  'INTERVAL 0 HOURS')` followed by a plain `VACUUM t` (no RETAIN clause).
  Afterward time travel fails as expected
  (DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION).
* 2026-11-14: Made cell 4 of `2.1 - Delta Lake` idempotent (`CREATE OR REPLACE
  TABLE`). Root cause of the "Command skipped" cascade: Run All aborts at the
  first error, so the notebook's final DROP TABLE never executes and the
  leftover table blocks the next session's CREATE. Verified the replace resets
  the table (0 rows). Note: UC preserves the original `created` timestamp
  across CREATE OR REPLACE — use row count, not `created`, to confirm.
* 2026-11-14: Modernized `3.1 - Databases and Tables` (24 cells) the same
  way as 2.1: `USE CATALOG main`; `CREATE OR REPLACE` for the schema and
  re-created tables so the notebook is re-runnable; every external-table
  (`dbfs:/mnt/...` mounts), `dbfs:/Shared` custom-location schema, `%fs`,
  and `dbutils.fs.rm` cell converted to comment-only "(Book cell: ...)"
  notes explaining what the book showed. Cell 11 keeps its intentional
  error (SELECT after DROP) — Run All stops there, run remaining cells
  individually. Not executed end-to-end (Auto Mode blocked the
  destructive DROP/CREATE OR REPLACE cells); user will run it themselves.
* 2026-11-14: `CREATE OR REPLACE SCHEMA` is NOT valid Databricks SQL
  (PARSE_SYNTAX_ERROR at 'SCHEMA', validated on serverless). The working
  idempotent form is `CREATE SCHEMA IF NOT EXISTS` — fixed cell 20 of
  `3.1 - Databases and Tables` to use it (re-runnability still holds
  because the notebook's tables use `CREATE OR REPLACE TABLE`).
* 2026-11-14: Modernized `3.2A - Views` and validated all 8 working cells
  end-to-end: `USE CATALOG main`; `CREATE OR REPLACE TABLE cars` (the
  book's `CREATE TABLE IF NOT EXISTS` + INSERT duplicated 10 rows on every
  re-run); `CREATE OR REPLACE VIEW`. Global temp views do NOT work on
  serverless (NOT_SUPPORTED_WITH_SERVERLESS, validated) — cells 13/14/16
  converted to comment-only; plain TEMP VIEWs work fine. New gotcha:
  `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` is NOT idempotent — the
  INSERT runs even when the CREATE is skipped, appending duplicates.
  Use `CREATE OR REPLACE TABLE` for seed-data cells.
* 2026-11-16: Modernized `3.2B - Views (Session 2)` (5 of 9 cells):
  `USE CATALOG main` (cell 2, validated by running it — confirms the
  session-2 point: stored views persist, session temp views don't); the
  three `global_temp` cells (SHOW/SELECT/DROP) converted to comment-only
  "(Book cell: ...)" notes; `DROP VIEW`/`DROP TABLE` made idempotent with
  `IF EXISTS`. Destructive DROP cells left for the user to run.
