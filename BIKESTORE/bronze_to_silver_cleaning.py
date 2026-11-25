# Databricks notebook: Bronze -> Silver cleaning
# Cleans: duplicates, nulls, extra whitespace, and converts column names to snake_case
# Output format: CSV (matches Bronze source format)

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

STORAGE_ACCOUNT = "gamingstorageset"
CONTAINER = "gamingdata"

BRONZE_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Bronze"
SILVER_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Silver"

# List every table you saw in your Bronze folders
TABLES = [
    "production/brands",
    "production/categories",
    "production/products",
    "production/stocks",
    "sales/customers",
    "sales/order_items",
    "sales/orders",
    "sales/staffs",
    "sales/stores",
]


def to_snake_case(name: str) -> str:
    """Convert any column name style to snake_case."""
    name = name.strip()
    name = re.sub(r"[\s\-]+", "_", name)                     # spaces/hyphens -> _
    name = re.sub(r"(?<!^)(?<![A-Z_])([A-Z])", r"_\1", name)  # camelCase -> camel_Case
    name = re.sub(r"__+", "_", name)                          # collapse multiple _
    return name.strip("_").lower()


def clean_dataframe(df: DataFrame) -> DataFrame:
    # 1. Rename all columns to snake_case
    for col in df.columns:
        df = df.withColumnRenamed(col, to_snake_case(col))

    # 2. Trim leading/trailing whitespace and collapse internal double spaces
    #    on every string column
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
        )
        # Treat empty strings left after trimming as nulls
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

    # 3. Drop exact duplicate rows
    df = df.dropDuplicates()

    # 4. Drop rows that are null across every column
    #    (use dropna(how="all") rather than "any" so you don't lose rows
    #    that just have one optional null field)
    df = df.dropna(how="all")

    return df


def run():
    for table_path in TABLES:
        bronze_path = f"{BRONZE_BASE}/{table_path}/"
        silver_path = f"{SILVER_BASE}/{table_path}/"
        table_name = table_path.split("/")[-1]

        print(f"Processing {table_name} ...")

        # Inspect the actual files in this folder to pick the right reader.
        files = [f.name for f in dbutils.fs.ls(bronze_path) if not f.isDir()]
        if any(f.endswith(".csv") for f in files):
            raw_df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(bronze_path)
            )
        elif any(f.endswith(".parquet") for f in files):
            raw_df = spark.read.format("parquet").load(bronze_path)
        elif any(f.endswith(".json") for f in files):
            raw_df = spark.read.format("json").load(bronze_path)
        else:
            print(f"  Skipping {table_name}: no recognized file type in {files}")
            continue

        before_count = raw_df.count()
        cleaned_df = clean_dataframe(raw_df)
        after_count = cleaned_df.count()

        (
            cleaned_df.write
            .format("csv")
            .mode("overwrite")
            .option("header", "true")
            .save(silver_path)
        )

        print(f"  {table_name}: {before_count} -> {after_count} rows written to Silver")
        print(f"  path: {silver_path}\n")


run()
