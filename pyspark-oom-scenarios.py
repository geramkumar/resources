from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, monotonically_increasing_id, expr
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd
import shutil
import os

# -----------------------------------------------
# Initialize SparkSession
# -----------------------------------------------
spark = SparkSession.builder \
    .appName("OOM_Scenarios_PySpark") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

# ==========================================================================
# 🚨 Scenario 1: Too Few Partitions → Large partitions → Executor OOM
# ==========================================================================

# ❌ PROBLEM: Very large partitions assigned to few tasks → easily OOM
# ✅ SOLUTION: Increase number of partitions using .repartition()

df_large = spark.range(10_000_000)  # ~10MB of numeric data

# Trigger OOM by creating very few large partitions
df_oombad = df_large.repartition(2)  # ❌ May cause high memory load per executor

# Investigation
print("Too few partitions:", df_oombad.rdd.getNumPartitions())

# Action (might trigger OOM)
# Uncomment this line to test under memory pressure (use with caution!)
# df_oombad.groupBy((col("id") % 1000).alias("bucket")).count().show()

# ✅ FIX: Repartition to smaller chunks (~100-200MB)
df_fixed = df_large.repartition(100)
print("Recommended partitions:", df_fixed.rdd.getNumPartitions())

df_fixed.groupBy((col("id") % 1000).alias("bucket")).count().show()

# ==========================================================================
# 🚨 Scenario 2: Skewed Data → One partition OOMs
# ==========================================================================

# ❌ PROBLEM: 90% of data has key = 0, so one task processes huge volume = OOM
# ✅ SOLUTION: Use AQE or manual salting to handle skew

# Skewed dataset
df_skewed = spark.createDataFrame(
    [(i, 0 if i < 9_000_000 else i) for i in range(10_000_000)],
    ["id", "key"]
)

# Investigation
df_skewed.groupBy("key").count().orderBy("count", ascending=False).show()

# Action
# Uncommenting may trigger OOM for groupBy on large skew (test with caution!)
# df_skewed.groupBy("key").count().show()

# ✅ FIX Option 1: Adaptive Query Execution (Spark 3+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ✅ FIX Option 2: Manual salting (add randomness to key using a "salt" column)
df_salted = df_skewed.withColumn("salt", monotonically_increasing_id() % 10)
df_salted.groupBy("key", "salt").count().show()

# ==========================================================================
# 🚨 Scenario 3: Large Table Broadcast Join → OOM in Executor or Driver
# ==========================================================================

# ❌ PROBLEM: Forcing broadcast join with big table (e.g. 100MB+) causes OOM
# ✅ SOLUTION: Don't broadcast large tables. Let Spark choose strategy or disable threshold.

# Big fact + big dimension
fact_table = spark.range(2_000_000).withColumnRenamed("id", "fact_id")
dim_table = spark.range(1_000_000).withColumnRenamed("id", "dim_id")

# ❌ FORCE broadcast on big table
from pyspark.sql.functions import broadcast
result_bad = fact_table.join(broadcast(dim_table), fact_table.fact_id == dim_table.dim_id)

# Investigation
result_bad.explain("formatted")

# Uncomment to test (use small scales or cluster will OOM)
# result_bad.count()

# ✅ FIX: Avoid forcing broadcast here — let Spark pick or set threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

result_fixed = fact_table.join(dim_table, fact_table.fact_id == dim_table.dim_id)
result_fixed.explain("formatted")
result_fixed.count()

# ==========================================================================
# 🚨 Scenario 4: Wide UDF (Python) — Serialization OOM
# ==========================================================================

# ❌ PROBLEM: Python UDFs serialize data row-by-row → slow & memory-heavy
# ✅ SOLUTION: Use Spark SQL functions or Pandas UDFs

# UDF
def dangerous_increment(x):
    return x + 1

slow_udf = udf(dangerous_increment, IntegerType())

df_udf = spark.range(5_000_000).withColumn("inc", slow_udf(col("id")))

# Uncomment to potentially trigger memory pressure
# df_udf.count()

# ✅ FIX Option 1: Native functions
df_fast = spark.range(5_000_000).withColumn("inc", col("id") + 1)
df_fast.count()

# ✅ FIX Option 2: Vectorized Pandas UDFs
@pandas_udf("int")
def pandas_add_one(x: pd.Series) -> pd.Series:
    return x + 1

df_pandas = spark.range(5_000_000).withColumn("inc", pandas_add_one(col("id")))
df_pandas.count()

# ==========================================================================
# 🚨 Scenario 5: Small Files or Large Shuffles → Memory Pressure / OOM
# ==========================================================================

# ❌ PROBLEM: Writing many partitions = many shuffle/write tasks → executor memory pressure
# ✅ SOLUTION: Control partitioning with coalesce(), optimize file size

output_path_bad = "/tmp/oom_many_files"
output_path_fixed = "/tmp/oom_few_files"

# Create and write many small files
df_many = spark.range(1_000_000).repartition(100)
df_many.write.mode("overwrite").parquet(output_path_bad)

# Investigation: list files (should be many .parquet files)
print("Files in bad output:", len(os.listdir(output_path_bad)))

# ✅ FIX: Reduce number of files
df_few = df_many.coalesce(5)
df_few.write.mode("overwrite").parquet(output_path_fixed)
print("Files in fixed output:", len(os.listdir(output_path_fixed)))

# ==========================================================================
# 📌 Summary: OOM Causes, Detection, Best Fixes
# ==========================================================================

"""
1. Large Partition OOM:
   • Cause: Few partitions overload memory
   • Investigate: df.rdd.getNumPartitions()
   • Fix: repartition() to ~100-200 for big data

2. Skewed Key:
   • Cause: One key = most records
   • Investigate: groupBy("key").count()
   • Fix: AQE skew hints, salting

3. Broadcast Join OOM:
   • Cause: Big table forced broadcast
   • Investigate: .explain(); memory logs
   • Fix: Let Spark auto-choose join; disable broadcast hint for big tables

4. Python UDF OOM:
   • Cause: Serial Python UDFs on huge sets
   • Fix: Built-in Spark SQL, or pandas_udf if custom logic needed

5. Shuffle/File Explosion:
   • Cause: Excessive partitions during write
   • Fix: coalesce() before write or increase write partition size

Tools:
  - `.explain("formatted")`
  - `Spark UI (Stages, Metrics)`
  - `.rdd.getNumPartitions()`
  - OS filesystem checks (number & size of files)
"""

# ==========================================================================
# ✅ Clean-up Output Paths (optional)
# ==========================================================================
def safe_rm(path):
    try:
        shutil.rmtree(path)
    except Exception:
        pass

safe_rm("/tmp/oom_many_files")
safe_rm("/tmp/oom_few_files")

# End of Script ✅
