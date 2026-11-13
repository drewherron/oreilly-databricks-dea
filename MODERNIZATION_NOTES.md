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
| Chapter 2/2.1 – Delta Lake | 8 cells doing `%fs ls`/`%fs head` on `dbfs:/user/hive/warehouse/product_info` | Not yet fixed; use `DESCRIBE DETAIL` |
| Chapter 3: 3.1, 3.2A, 3.2B | `hive_metastore` references — may be pedagogical (teaching legacy vs UC); fix in context when reached | Not yet reached |
| Chapter 6/6.2 – Output Exploration | `hive_metastore` reference | Not yet reached |

## Changelog

* 2026-11-13: Created this file. Fixed `2.1 - Delta Lake` cell 2 to use
  `USE CATALOG main`.
