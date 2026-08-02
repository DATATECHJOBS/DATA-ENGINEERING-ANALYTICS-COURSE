# Databricks notebook source
# MAGIC %md
# MAGIC # PepsiCo PySpark Interview: Senior Data Engineer
# MAGIC
# MAGIC **Goal:** Join multiple input files using PySpark to produce an output DataFrame in the required format.
# MAGIC
# MAGIC The `data` directory contains:
# MAGIC ```
# MAGIC data
# MAGIC ├── england_councils
# MAGIC │   ├── district_councils.csv
# MAGIC │   ├── london_boroughs.csv
# MAGIC │   ├── metropolitan_districts.csv
# MAGIC │   └── unitary_authorities.csv
# MAGIC ├── property_avg_price.csv
# MAGIC └── property_sales_volume.csv
# MAGIC ```
# MAGIC - Council files: columns `council, county`
# MAGIC - `property_avg_price.csv`: `local_authority, avg_price_nov_2019, avg_price_nov_2018, difference`
# MAGIC - `property_sales_volume.csv`: `local_authority, sales_volume_sep_2019, sales_volume_sep_2018`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 1. In **Catalog**, create a Volume.
# MAGIC volume: gland_councils_raw catalog: england_councils schema:default 
# MAGIC
# MAGIC 2. Upload the `data` folder into it (keep the folder structure).
# MAGIC 3. Update the path below to match your Volume.

# COMMAND ----------

input_directory = "/Volumes/england_councils/default/england_councils_raw/data/"  # <-- change to your Volume path

# Quick sanity check: list the files
display(dbutils.fs.ls(f"{input_directory}/england_councils"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the raw data first
# MAGIC Before writing any transformation, look at what you're dealing with.
# MAGIC Read one council file and the two property files. Check schemas and a few rows.
# MAGIC
# MAGIC Questions to answer before moving on:
# MAGIC - What columns does each file have? Do the join keys have the same name?
# MAGIC - What data types did Spark infer? Are prices numeric or strings?
# MAGIC - Anything suspicious in the values? (Look closely at `difference` — and trust nothing.)

# COMMAND ----------

# TODO: read england_councils/district_councils.csv with header=True and display it
# TODO: read property_avg_price.csv, printSchema(), display a sample
# TODO: read property_sales_volume.csv, printSchema(), display a sample

# COMMAND ----------

#CHECK YOUR RAW DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 — `extract_councils()`
# MAGIC Combine the four council files into one DataFrame with columns:
# MAGIC
# MAGIC | column | rule |
# MAGIC |---|---|
# MAGIC | `council` | from the raw files |
# MAGIC | `county` | from the raw files |
# MAGIC | `council_type` | new column based on source file: `District Council`, `London Borough`, `Metropolitan District`, `Unitary Authority` |
# MAGIC
# MAGIC **Hint:** the result should contain **316 rows** — the sum of rows across the four files.
# MAGIC
# MAGIC Think about: `lit()` for the constant column, `union()` semantics (position-based, not name-based — why does that matter?).

# COMMAND ----------

from pyspark.sql.functions import lit, col, trim, regexp_replace

def extract_councils():
    # TODO: read each of the four CSVs (header=True)
    # TODO: add council_type via .withColumn("council_type", lit(...))
    # TODO: union all four and return
    pass

# COMMAND ----------

# Validation — do not modify
councils_df = extract_councils()
assert councils_df.count() == 316, f"Expected 316 rows, got {councils_df.count()}"
assert set(councils_df.columns) == {"council", "county", "council_type"}
display(councils_df.groupBy("council_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 — `extract_avg_price()`
# MAGIC From `property_avg_price.csv`, return a DataFrame with **only**:
# MAGIC - `council` — renamed from `local_authority`
# MAGIC - `avg_price_nov_2019`
# MAGIC
# MAGIC Drop the other columns. Make sure `avg_price_nov_2019` ends up numeric (double), not a string.

# COMMAND ----------

def extract_avg_price():
    # TODO: read, rename local_authority -> council, select two columns, cast price to double
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 — `extract_sales_volume()`
# MAGIC Same idea: from `property_sales_volume.csv`, return **only**:
# MAGIC - `council` — renamed from `local_authority`
# MAGIC - `sales_volume_sep_2019` (integer)

# COMMAND ----------

def extract_sales_volume():
    # TODO
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 — `transform()`
# MAGIC Join the three DataFrames into one with columns:
# MAGIC
# MAGIC `council, county, council_type, avg_price_nov_2019, sales_volume_sep_2019`
# MAGIC
# MAGIC **Requirement:** every council must remain in the output even if it has no price/volume data (which join type guarantees this?).
# MAGIC
# MAGIC **Hint:** the result should still contain **316 rows**.

# COMMAND ----------

def transform(councils_df, avg_price_df, sales_volume_df):
    # TODO: two left joins on "council"
    pass

# COMMAND ----------

# Validation — do not modify
result_df = transform(extract_councils(), extract_avg_price(), extract_sales_volume())
assert result_df.count() == 316, f"Expected 316 rows, got {result_df.count()}"
display(result_df)