# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Use catalog main
# MAGIC %sql
# MAGIC -- The book uses the legacy hive_metastore catalog, which doesn't exist in
# MAGIC -- Unity Catalog-only workspaces (e.g. Databricks Free Edition).
# MAGIC -- See MODERNIZATION_NOTES.md in the repo root.
# MAGIC USE CATALOG main

# COMMAND ----------

# DBTITLE 1,Define UC volume paths
# The book downloaded the school dataset from the author's s3:// bucket
# through a DBFS mount (dbfs:/mnt/...), which doesn't exist in
# Unity Catalog-only workspaces. Instead we pull the same public bucket
# over HTTPS into the UC volume main.de_associate_school.dea_book
# (created in the last cell of this notebook).
data_source_uri = "https://dalhussein-books.s3.amazonaws.com/DEA-Book/datasets/school/v1/"

dea_book_root = "/Volumes/main/de_associate_school/dea_book"
dataset_school = f"{dea_book_root}/datasets/school"
checkpoint_path = f"{dea_book_root}/checkpoints"
dlt_path = f"{dea_book_root}/dlt"

db_name = "de_associate_school"
dlt_db_name = "school_dlt_db"

# The book set the spark conf "dataset.school" so SQL cells could use
# ${dataset.school} substitution. That mechanism is gone on serverless:
# spark.conf.set and SQL SET are both blocked for arbitrary keys
# (CONFIG_NOT_AVAILABLE), ${...} is no longer substituted in SQL cells,
# and widget parameter markers (:param) don't work for direct file-path
# queries either (FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY when the path
# is bound at runtime instead of written literally). Chapter SQL cells
# therefore use the literal volume path (= dataset_school); Python
# cells keep using this variable directly.

# COMMAND ----------

# DBTITLE 1,Clean-up drops UC schemas
def clean_up():
    print("Dropping schemas (and the dea_book volume: dataset, checkpoints, dlt storage) ...")
    spark.sql(f"DROP SCHEMA IF EXISTS main.{db_name} CASCADE")
    spark.sql(f"DROP SCHEMA IF EXISTS main.{dlt_db_name} CASCADE")
    print("Done")

# COMMAND ----------

try:
    clean = int(dbutils.widgets.get("clean"))
except:
    clean = 0

if clean:
    clean_up()

# COMMAND ----------

# DBTITLE 1,Path check via os.path
import os

def path_exists(path):
    return os.path.exists(path)

# COMMAND ----------

# DBTITLE 1,Download dataset over HTTPS
import requests
import urllib.parse
import xml.etree.ElementTree as ET

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

def list_object_keys(base, prefix):
    """List all object keys under `prefix` in the public bucket at `base`
    (S3 ListObjectsV2 over HTTPS, with pagination)."""
    keys, token = [], None
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        root = ET.fromstring(requests.get(base, params=params, timeout=30).text)
        keys += [c.find("s3:Key", _S3_NS).text
                 for c in root.findall("s3:Contents", _S3_NS)]
        if root.find("s3:IsTruncated", _S3_NS).text != "true":
            return keys
        token = root.find("s3:NextContinuationToken", _S3_NS).text

def download_dataset(source, target):
    """Copy every object under the S3 prefix in `source` (a public HTTPS
    endpoint) into the UC volume path `target`. Skips files already copied,
    so re-running is safe."""
    parsed = urllib.parse.urlparse(source)
    base = f"{parsed.scheme}://{parsed.netloc}"
    prefix = parsed.path.lstrip("/")
    for key in list_object_keys(base, prefix):
        rel = key[len(prefix):]
        target_path = f"{target}/{rel}"
        if path_exists(target_path):
            continue
        print(f"Copying {rel} ...")
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(requests.get(f"{base}/{key}", timeout=60).content)

# COMMAND ----------

def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

# COMMAND ----------

# Structured Streaming
streaming_dir = f"{dataset_school}/enrollments-json-streaming"
raw_dir = f"{dataset_school}/enrollments-json-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} file to the school dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

    
def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# COMMAND ----------

# DLT
streaming_enrollments_dir = f"{dataset_school}/enrollments-dlt-streaming"
streaming_courses_dir = f"{dataset_school}/courses-streaming"

raw_enrollments_dir = f"{dataset_school}/enrollments-dlt-raw"
raw_courses_dir = f"{dataset_school}/courses-cdc"

def load_json_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.json"
    print(f"Loading {latest_file} enrollments file to the school dataset")
    dbutils.fs.cp(f"{streaming_enrollments_dir}/{latest_file}", f"{raw_enrollments_dir}/{latest_file}")
    print(f"Loading {latest_file} courses file to the school dataset")
    dbutils.fs.cp(f"{streaming_courses_dir}/{latest_file}", f"{raw_courses_dir}/{latest_file}")

    
def load_new_json_data(all=False):
    index = get_index(raw_enrollments_dir)
    if index >= 10:
        print("No more data to load\n")

    elif all == True:
        while index <= 10:
            load_json_file(index)
            index += 1
    else:
        load_json_file(index)
        index += 1

# COMMAND ----------

#clean_up()

# COMMAND ----------

# DBTITLE 1,Create UC schemas/volume, download, use schema
# The book created its schema in hive_metastore and the dataset under a
# DBFS mount. Both now live in UC: schemas in the main catalog, dataset
# files in the dea_book volume.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{db_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS main.{db_name}.dea_book")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{dlt_db_name}")

download_dataset(data_source_uri, dataset_school)

spark.sql(f"USE {db_name}")
print()
print(f"Schema name: main.{db_name}")