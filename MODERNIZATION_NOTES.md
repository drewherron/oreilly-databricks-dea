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
* `input_file_name()` is blocked in UC workspaces
  (UC_COMMAND_NOT_SUPPORTED). The UC replacement is the `_metadata`
  column group: use `_metadata.file_path AS source_file`.
* UC external tables require a cloud-storage LOCATION (s3://, abfss://,
  gs://) inside a registered external location. Volume paths are rejected
  ("Missing cloud file system scheme" — also via OPTIONS (path)); the
  dbfs:/ prefix is rejected as UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_
  SUPPORTED. Free Edition has no external locations, so the runnable
  equivalent is a TEMP VIEW with OPTIONS (path = "<volume dir>").
* REFRESH TABLE is not supported on serverless compute
  (NOT_SUPPORTED_WITH_SERVERLESS). A temp view's file listing is cached
  per session; to pick up new files, re-CREATE the view (validated:
  listing refreshes on re-creation, not on re-query).
* School-dataset infrastructure (created by `Includes/School-Setup`):
  schemas `main.de_associate_school` (the book's `DE_Associate_School`) and
  `main.school_dlt_db`; dataset/checkpoints/dlt paths under the volume
  `main.de_associate_school.dea_book` (`/Volumes/main/de_associate_school/dea_book/...`).
* The book's `${dataset.school}` substitution in SQL cells (spark.conf-based)
  is dead on serverless. The ONLY reliable replacement for direct file-path
  queries (json./csv./text./binaryFile. format notation) in SQL cells is a
  literal volume path, e.g.
  `SELECT * FROM json.`/Volumes/main/de_associate_school/dea_book/datasets/school/students-json``.
  Widget `:param` markers bind server-side in native SQL cells, and a
  runtime-bound path breaks direct-file plan creation
  (FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY — confirmed in the user's own
  UI run). The marker form `IDENTIFIER('json.`' || :param || ...)` works
  only via the %sql magic path (IPython textual substitution), which is
  NOT how native SQL cells execute — validating there is a false
  positive. `IDENTIFIER(:param)` stays valid for object names
  (tables/views/schemas), just not for file-path notation. Python cells
  keep using the `dataset_school` variable directly.

## Known issues

| Location | Issue | Status |
| --- | --- | --- |
| Includes/School-Setup | `hive_metastore` + DBFS mounts + `s3://` source for the school dataset; chapters 5–6 depend on its output | **Fixed 2026-11-16** — UC rework (see changelog); dataset now in volume `main.de_associate_school.dea_book`, pulled from the author's public bucket over HTTPS |
| Chapters 4–6 notebooks | SQL cells use `${dataset.school}` substitution, which no longer works on serverless | **4.1–4.5 Fixed (Chapter 4 complete); 5.1 Fixed 2026-11-17** — 5.1 had no `${dataset.school}` cells (its fixes were streaming-related, see the 5.1 row below); 5.2–6.4 to be converted with literal volume paths as reached |
| 5.1 – Structured Streaming | `dbfs:/mnt/DEA-Book/checkpoints` paths (cells 14/20/25) and continuous `processingTime` trigger (cell 14) — both impossible on UC-only serverless | **Fixed 2026-11-17** — volume checkpoints + availableNow rework (see changelog) |
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
* 2026-11-16: Reworked `Includes/School-Setup` for UC (6 of 11 cells) and
  validated end-to-end:
  * `USE CATALOG main`; schemas `main.de_associate_school` +
    `main.school_dlt_db`; all storage under volume
    `main.de_associate_school.dea_book` (dataset, checkpoints, dlt).
  * Dataset sourcing: `dbfs:/mnt` mounts + `s3://` are impossible in a
    UC-only workspace, but the author's bucket `dalhussein-books` is
    public — School-Setup now lists it via the S3 HTTPS API and downloads
    with `requests` into the volume. Idempotent (skips existing files).
    Verified: 51 files / 4,599,831 bytes on disk exactly match the S3
    listing (12 dataset dirs incl. parquet — binary writes required).
  * Serverless gotchas (validated): `spark.conf.set` AND SQL `SET` are
    both blocked for arbitrary keys (CONFIG_NOT_AVAILABLE — not just
    Delta confs), and `${param}` is no longer substituted in SQL cells.
    Working replacement: widget + parameter marker in IDENTIFIER():
    `SELECT COUNT(1) FROM IDENTIFIER('json.`' || :dataset_school ||
    '/students-json`')` → 1700 rows, tested in a real SQL cell after
    `%run ../Includes/School-Setup` from 4.1. Widget names for `:param`
    markers must be identifier-valid (no dots).
  * FUSE file API (`open`/`os.path`/`os.walk`) and `dbutils.fs` both work
    on `/Volumes/...` paths on serverless — binary writes OK.
  * `clean_up()` now drops both schemas CASCADE (the volume inside
    `de_associate_school` goes with it). Auto Mode blocked a full notebook
    run over the DROP definition; validated by skipping that definition
    cell (it only defines a function; the `clean` widget defaults to 0).
  * 4.1 not modified today (its `${dataset.school}` SQL cells get the
    IDENTIFIER pattern next session); a temp validation cell was added
    to 4.1, run, and deleted.
* 2026-11-16: Modernized `4.1 - Querying Files` (12 of 36 cells, after the
  user hit the first failure at cell 6): all `${dataset.school}` SQL
  cells converted to the IDENTIFIER/:dataset_school pattern (cells 6–10,
  12, 14, 16); CTAS cells made `CREATE OR REPLACE` (18, 19, 34's table);
  the external-table `LOCATION` and `OPTIONS` path values written as
  literal volume paths (21, 34) since they are plain string values.
  All query forms validated in the user's live notebook session:
  json single file 300 rows, wildcard/directory 1700, text 1700,
  binaryFile 6 files, csv 16 rows.
  * NEW GOTCHA (validated): `:widget` parameter markers bind only in the
    notebook's own execution context. The assistant's headless cell-run
    tool executes SQL cells in a separate context (even a different
    Python version) where the marker is not bound, and direct file
    queries then fail with FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY — an
    artifact of the tool, not a broken cell. An identical statement run
    in the notebook's live session succeeds.
* 2026-11-16 (4.1 continued, external-table section): the book's external
  CSV table (cells 21-32) cannot exist in UC Free Edition — external
  tables need cloud-storage LOCATIONs under registered external
  locations; volume paths are rejected by the UC credential machinery
  (validated: "Missing cloud file system scheme" for both LOCATION and
  OPTIONS (path); dbfs:/ rejected as UC_FILE_SCHEME_FOR_TABLE_CREATION_
  NOT_SUPPORTED; UC external tables also don't support CREATE OR REPLACE).
  Reworked to preserve the lesson: cell 21 creates a TEMP VIEW courses_csv
  over the same volume files. The Limitation demo still works because a
  temp view's file listing is cached per session (validated in a scratch
  dir copy: count unchanged after adding a file); REFRESH TABLE (blocked
  on serverless) is replaced by re-creating the view (validated: 3 -> 6).
  DESCRIBE EXTENDED on a temp view lists only columns (no table metadata).
  Hybrid section (34-36) kept — wildcard OPTIONS path validated (12 rows,
  deliberately excluding copy_001.csv). Cells 21/22/24/27 validated in the
  notebook: SELECT shows all 12 courses C01-C12, count 12; expected flow
  for the Limitation demo: 12 -> copy file -> still 12 -> re-create view
  -> 15 -> hybrid CTAS courses = 12.
* 2026-11-16 (correction to the 4.1 entry above): the
  IDENTIFIER/:dataset_school marker pattern does NOT work in native SQL
  cells — the user's own UI run failed with
  FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY. Root cause: native SQL cells
  bind `:param` markers server-side; a runtime-bound path cannot be
  planned for direct queries on files. The earlier "validation" ran
  through the %sql magic path (textual widget substitution), a false
  positive. Re-converted 4.1's file-path cells (6–10, 12, 14, 16, 18,
  19) to literal volume paths (static queries — no binding machinery,
  identical behavior on every execution path). Validated: json single
  file 300 rows / directory 1700, text 1700, binaryFile 6 files, csv 16
  rows. The `dataset_school` widget was removed from School-Setup (no
  remaining consumer); a stale `dataset_school` widget may linger in
  already-open notebooks and can be cleared from the widget panel.
* 2026-11-17: Modernized `4.2 - Writing to Tables` (8 of 22 cells): all
  `${dataset.school}` SQL converted to literal volume paths (cells 4, 8,
  11, 13, 15, 18, 20); cell 4's plain CREATE TABLE made `CREATE OR REPLACE`
  for re-runnability; cell 13 (the commented input_file_name() demo)
  annotated with the UC replacement `_metadata.file_path`; fixed the book's
  `c. student_id` / `u. student_id` typo in the students MERGE (Spark SQL
  rejects the space after the dot).
  Source queries validated headless with the literal paths: parquet
  enrollments 2150 rows, enrollments-new 700, json students-json-new 301
  (schema matches the `students` table: email/gpa/profile/student_id/
  updated), courses-csv-new temp view 5 rows. MERGE targets `students`
  (1700 rows) and `courses` (12 rows) — both are created by 4.1, so run
  4.1 (hybrid section) before 4.2. Managed-table writes (CREATE/INSERT/
  MERGE) left for the user to run; unqualified table names resolve via
  School-Setup's `USE de_associate_school`.
* 2026-11-17: Modernized `4.3 - Advanced Transformations` (1 of 33 cells —
  the only `${dataset.school}` in the notebook): cell 25's enrollments_updates
  temp view converted to a literal volume path. Everything else reads the
  `students`/`courses`/`enrollments` tables created by 4.1/4.2, so run those
  first (incl. 4.2's INSERT INTO append — the set-operation section contrasts
  enrollments 2850 vs enrollments-new 700: UNION ALL 3550, INTERSECT 700,
  MINUS 2150).
  Validated headless: cell 25's view (700 rows); the other query shapes —
  explode(courses), collect_set(courses.course_id) + array_distinct(flatten),
  from_json + schema_of_json, the enrollments⋈courses join behind cell 23,
  and the cell-33 PIVOT — all run clean against the current tables.
  Note: cell 23's `CREATE OR REPLACE VIEW enrollments_enriched` was created
  during validation (identical to what 4.3 will re-create). No other
  UC/serverless issues in this notebook (no hive_metastore, %fs, or widget
  machinery).
* 2026-11-17: Checked `4.4 - Higher Order Functions` — **no edits needed**.
  It has no `${dataset.school}` cells (its 4 SQL cells only read the
  `enrollments` table created by 4.2) and no legacy patterns. All four
  query shapes validated headless against the live table: FILTER,
  FILTER + size() predicate, TRANSFORM with a scalar lambda, and TRANSFORM
  with named struct fields. Runs as-is once 4.2's enrollments table exists.
* 2026-11-17: Checked `4.5 - SQL UDFs` — one small edit, no path issues (no
  `${dataset.school}`; only reads the `students` table). Cell 15's plain
  `DROP FUNCTION` made `DROP FUNCTION IF EXISTS` for re-runnability.
  Validated the full UDF flow headless in main.de_associate_school (then
  dropped everything, leaving the schema as the book's end state):
  * `CREATE OR REPLACE FUNCTION` (SQL UDFs) works on serverless + UC;
    both book UDFs created and applied correctly (gpa 1.48 -> 37, 3.31 -> 83;
    letter grades as expected).
  * `DESCRIBE FUNCTION` / `DESCRIBE FUNCTION EXTENDED` work; the routine
    metadata lives in `information_schema.routines` (columns: routine_name,
    routine_schema — NOT function_name/function_schema).
  * `DROP FUNCTION IF EXISTS` validated idempotent (second drop on a
    missing function succeeds).
  * Two gotchas, both tool-side, not notebook-side: `spark.sql()` takes one
    statement per call (a notebook SQL cell runs multi-statement fine —
    4.1 cell 34 precedent), and `USE SCHEMA catalog.schema` two-part form is
    rejected (UC_INVALID_NAMESPACE) — use `USE CATALOG x` then
    `USE SCHEMA y` (School-Setup already does this).
* 2026-11-17: Modernized `5.1 - Structured Streaming` (5 of 27 cells; no
  `${dataset.school}` in this notebook — its issues were streaming-related):
  * `dbfs:/mnt/DEA-Book/checkpoints/...` (cells 14/20/25) → literal volume
    paths `/Volumes/main/de_associate_school/dea_book/checkpoints/...`.
    The `checkpoints/` root is new — School-Setup only created `datasets/`
    (the dir was created by this session's validation).
  * NEW GOTCHA (validated): time-based triggers are blocked on serverless
    — `.trigger(processingTime='3 seconds')` fails with
    INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED ("Use a different trigger type
    e.g. AvailableNow, Once"); docs say use Trigger.AvailableNow for
    triggered runs or Lakeflow pipelines continuous mode for always-on
    streams. Cell 14 (continuous demo) converted to availableNow +
    awaitTermination (the book's own one-shot pattern from cell 20);
    re-running it after the cell-16/19 INSERTs replaces the book's
    auto-update demo. Cells 17/20 annotated accordingly.
  * Cells 7/9 (plain SELECT on the streaming temp view) fail with "Queries
    with streaming sources must be executed with writeStream.start()" — a
    Spark rule, not a UC/serverless regression (cell 10 is the book's own
    commented-out ORDER BY limitation demo). Per user request, both cells
    are commented out with explanatory notes so Run All completes (note:
    unlike 3.1's intentional cell-11 error, this book kept as-is).
    Update 2026-11-18: Run All can still stop at cell 24 if serverless
    rejects display() of a streaming DF — unconfirmed either way; if it
    fails, comment it out the same way (cell 25 is the validated path).
  * Validated headless: readStream.table(courses) + temp view (cells
    4/6); CREATE TEMP VIEW over the streaming view + spark.table →
    isStreaming (cells 12/13); availableNow + complete + .table() sink +
    volume checkpoint writes 15 rows; re-run from the same checkpoint
    completes without duplicating (complete mode rewrites state). The
    INSERT-driven update flow (cells 16→17, 19→20) is standard
    Delta-source/checkpoint behavior — expected, left for the user to run
    (Auto Mode blocked the destructive validation).
  * Cell 24 (display of a streaming DF) cannot be validated headless (no
    display() outside the notebook UI); annotated in-cell with the
    availableNow fallback. Cells 26/27 read the sink table — fine.
  * Validation leftovers for the user to drop: table
    `main.de_associate_school._dea_51_scratch` and checkpoint dir
    `/Volumes/main/de_associate_school/dea_book/checkpoints/_51_scratch_ck`.
