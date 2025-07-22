from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, expr, avg, count, rand, monotonically_increasing_id, lit, pandas_udf
)
from delta.tables import DeltaTable  # For Delta features; requires delta-spark
import pandas as pd
import shutil

spark = SparkSession.builder \
    .appName("PerformanceInterviewPrep") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ------------------------------------------------------------
# 1. Join Without Broadcast — Shuffle Overhead
# ------------------------------------------------------------
# PROBLEM: Joining large fact + small dimension table triggers slow, expensive shuffle joins.
# INVESTIGATION: .explain("formatted") reveals 'Exchange' for both inputs.
# SOLUTION: Use broadcast() for small side to get BroadcastHashJoin.

fact = spark.createDataFrame([(i, i % 10) for i in range(100000)], ["id", "cat"])
dim = spark.createDataFrame([(i, chr(65 + i)) for i in range(10)], ["cat", "label"])
result = fact.join(dim, "cat")
result.explain("formatted")
# Solution:
result_bc = fact.join(broadcast(dim), "cat")
result_bc.explain("formatted")

# ------------------------------------------------------------
# 2. Data Skew — Unbalanced Join/Group Work
# ------------------------------------------------------------
# PROBLEM: Disproportionately large partitions for one/few keys (long tasks).
# INVESTIGATION: .groupBy().count().orderBy(desc("count")) to inspect key distribution.
skewed = spark.createDataFrame(
    [(i, 0 if i < 90000 else i) for i in range(100000)], ["id", "skew_key"])
skewed.groupBy("skew_key").count().orderBy(col("count").desc()).show()
# SOLUTION 1: Spark 3+ AQE skew join (auto): just enable conf.
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# SOLUTION 2: Manual salting (when AQE isn't enough)
skewed_salt = skewed.withColumn("salt", monotonically_increasing_id() % 8)

# ------------------------------------------------------------
# 3. Too Few Partitions — Large Partition / OOM Danger
# ------------------------------------------------------------
# PROBLEM: Default partition count produces huge partitions in big data jobs—risking OOM.
# INVESTIGATION: df.rdd.getNumPartitions()
big = spark.createDataFrame([(i, "x" * 200) for i in range(1000000)], ["id", "text"])
print("Partitions before:", big.rdd.getNumPartitions())
# SOLUTION: .repartition() for improved parallelism.
big = big.repartition(100)
print("Partitions after:", big.rdd.getNumPartitions())

# ------------------------------------------------------------
# 4. Too Many Partitions — Scheduler/File Overhead
# ------------------------------------------------------------
# PROBLEM: Excessive partitions for a small job causes slow startup, too many output files.
small = spark.range(20)
many_part = small.repartition(50)
print("Partitions after excessive repartition:", many_part.rdd.getNumPartitions())
# SOLUTION: .coalesce(num) for fewer, bigger partitions for efficient write.
few_part = many_part.coalesce(2)
print("Partitions after coalesce:", few_part.rdd.getNumPartitions())

# ------------------------------------------------------------
# 5. Predicate & Partition Pruning
# ------------------------------------------------------------
# PROBLEM: Spark reads all data from storage if partition filter is missing.
df = spark.createDataFrame([(i, "2025-01-01") for i in range(10000)], ["id", "date"])
df.write.partitionBy("date").parquet("/tmp/predicate_prune")
# INVESTIGATION: .explain() -- check if partition filter appears before scan.
df_full = spark.read.parquet("/tmp/predicate_prune")
df_full.filter(col("id") == 1).explain("formatted")  # All partitions scanned
# SOLUTION: Push filter into partitioned column
df_pruned = spark.read.parquet("/tmp/predicate_prune").filter(col("date") == "2025-01-01")
df_pruned.explain("formatted")

# ------------------------------------------------------------
# 6. Outputting Too Many Small Files
# ------------------------------------------------------------
# PROBLEM: High partition count on write creates many small files (file explosion).
big = spark.range(10000)
big.repartition(100).write.mode("overwrite").parquet("/tmp/too_many_files")
# SOLUTION: .coalesce(N) before write, or Delta OPTIMIZE after write (on Databricks/Delta)
big.coalesce(5).write.mode("overwrite").parquet("/tmp/few_files")

# Delta: compact with OPTIMIZE (Databricks, Delta Lake)
try:
    from delta.tables import DeltaTable
    big.repartition(100).write.format("delta").mode("overwrite").save("/tmp/delta_too_many")
    dt = DeltaTable.forPath(spark, "/tmp/delta_too_many")
    dt.optimize()
except Exception:
    pass  # Skip on non-Delta configs

# ------------------------------------------------------------
# 7. Filtering After Join — Causes Full Shuffle
# ------------------------------------------------------------
df1 = spark.range(10000).withColumn("k", (col("id") % 5))
df2 = spark.createDataFrame([(i, "X") for i in range(5)], ["k", "v"])
# Problem: Filter comes AFTER join: all rows are shuffled/joined.
df_join_then_filter = df1.join(df2, "k").filter(col("id") > 5000)
df_join_then_filter.explain("formatted")
# Solution: Filter BEFORE join—smaller input, less shuffle.
df_filter_then_join = df1.filter(col("id") > 5000).join(df2, "k")
df_filter_then_join.explain("formatted")

# ------------------------------------------------------------
# 8. Not Caching When DataFrame Is Reused
# ------------------------------------------------------------
df_cache = df.withColumn("rand", rand())
# PROBLEM: Multiple actions recompute same DataFrame each time.
df_cache.cache()
df_cache.count()
df_cache.count()  # Now uses cache!
df_cache.unpersist()

# ------------------------------------------------------------
# 9. Out of Memory — Not Tuning Spark Conf/Shuffle Partitions
# ------------------------------------------------------------
# PROBLEM: Defaults may not match cluster size or data scale.
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.cores", "2")
spark.conf.set("spark.sql.shuffle.partitions", "64")
# SOLUTION: Set based on workload and cluster, monitor with Spark UI.

# ------------------------------------------------------------
# 10. Using Python UDFs Instead of Built-ins or Pandas UDF
# ------------------------------------------------------------
from pyspark.sql.functions import udf
def rev(x): return x[::-1] if x else x
udf_rev = udf(rev, StringType())
df = spark.createDataFrame([("abc",), ("def",)], ["val"])
df.withColumn("rev_builtin", expr("reverse(val)")).show()
# When necessary, use pandas_udf for vectorization:
@pandas_udf("string")
def revvec(x: pd.Series) -> pd.Series:
    return x.str[::-1]
df.withColumn("rev_vec", revvec(col("val"))).show()

# ------------------------------------------------------------
# 11. Writing in Columnar Formats (Parquet/Delta)
# ------------------------------------------------------------
# PROBLEM: CSV/JSON slow, no predicate pushdown/compression.
df.write.mode("overwrite").csv("/tmp/csv_slow")
df.write.mode("overwrite").parquet("/tmp/parquet_fast")
try:
    df.write.format("delta").mode("overwrite").save("/tmp/delta_fast")
except Exception:
    pass  # Ignore Delta write on non-Delta envs

# ------------------------------------------------------------
# 12. Delta Table: OPTIMIZE, Z-Ordering, Vacuuming, History
# ------------------------------------------------------------
# Useful for Databricks/Delta Lake
try:
    DeltaTable.forPath(spark, "/tmp/delta_fast").optimize().executeZOrderBy("val")
    DeltaTable.forPath(spark, "/tmp/delta_fast").vacuum(0)
    DeltaTable.forPath(spark, "/tmp/delta_fast").history().show()
except Exception:
    pass

# ------------------------------------------------------------
# 13. Partition Evolution (Delta Lake)
# ------------------------------------------------------------
# Problem: Partitioned tables must handle midstream changes.
# Solution: Delta allows new partitions as data changes.
data1 = spark.createDataFrame([(1, "A", "2023-01-01")], ["id", "val", "date"])
data2 = spark.createDataFrame([(2, "B", "2023-02-01")], ["id", "val", "date"])
delta_path = "/tmp/delta_evolvable"
data1.write.format("delta").mode("overwrite").partitionBy("date").save(delta_path)
data2.write.format("delta").mode("append").partitionBy("date").save(delta_path)

# ------------------------------------------------------------
# 14. Dynamic Partition Pruning (Spark 3.x+)
# ------------------------------------------------------------
fact = spark.createDataFrame([(i, i % 10, f"2023-07-{i%3:02d}") for i in range(50000)], ["id", "cat", "pdate"])
dim = spark.createDataFrame([(i, f"2023-07-0{i}") for i in range(3)], ["cat", "pdate"])
fact.write.partitionBy("pdate").parquet("/tmp/dpp_fact")
dim.write.parquet("/tmp/dpp_dim")
joined = spark.read.parquet("/tmp/dpp_fact").join(spark.read.parquet("/tmp/dpp_dim"), ["cat", "pdate"])
joined.explain("formatted")  # See only relevant partitions scanned

# ------------------------------------------------------------
# 15. Too Many Output Files on Write
# ------------------------------------------------------------
big.repartition(50).write.mode("overwrite").parquet("/tmp/manyfiles")
# Solution: Use .coalesce() to reduce files
big.coalesce(1).write.mode("overwrite").parquet("/tmp/onefile")

# ------------------------------------------------------------
# 16. Out-of-Memory: Large collect/show
# ------------------------------------------------------------
df_big = spark.range(10000000)
# PROBLEM: .collect()/.show() w/o .limit() can OOM driver.
df_big.limit(10).show()  # Always use limit/sample for local operations

# ------------------------------------------------------------
# 17. Not Cleaning Up Temporary/Checkpoint Data
# ------------------------------------------------------------
shutil.rmtree("/tmp/predicate_prune", ignore_errors=True)
shutil.rmtree("/tmp/few_files", ignore_errors=True)
shutil.rmtree("/tmp/too_many_files", ignore_errors=True)
shutil.rmtree("/tmp/delta_too_many", ignore_errors=True)
shutil.rmtree("/tmp/delta_fast", ignore_errors=True)
shutil.rmtree("/tmp/csv_slow", ignore_errors=True)
shutil.rmtree("/tmp/parquet_fast", ignore_errors=True)
shutil.rmtree("/tmp/delta_evolvable", ignore_errors=True)
shutil.rmtree("/tmp/manyfiles", ignore_errors=True)
shutil.rmtree("/tmp/onefile", ignore_errors=True)

# ------------------------------------------------------------
# 18. Not Using Spark UI and .explain()
# ------------------------------------------------------------
# Always monitor with Spark UI and check .explain("formatted") for plan, shuffle, skew, broadcast, etc.
print("Spark UI:", spark.sparkContext.uiWebUrl)
df_big.explain("formatted")

# ------------------------------------------------------------
# 19. Not Using Adaptive Query Execution (Spark 3+)
# ------------------------------------------------------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE automatically tunes plan: skew join, shuffle, broadcast at runtime.

# ------------------------------------------------------------
# 20. Not Using .partitionBy() for Future Query Efficiency
# ------------------------------------------------------------
df.write.partitionBy("cat").parquet("/tmp/parquet_cat_partitioned")
df_part = spark.read.parquet("/tmp/parquet_cat_partitioned").filter(col("cat") == 1)
df_part.explain("formatted")

# ------------------------------------------------------------
# 21. Partition Overwrite Mode for Delta on Incremental Loads
# ------------------------------------------------------------
data3 = spark.createDataFrame([(3, "C", "2023-02-01")], ["id", "val", "date"])
try:
    data3.write.format("delta").mode("overwrite").option("replaceWhere", "date='2023-02-01'").save(delta_path)
except Exception:
    pass

# ------------------------------------------------------------
# 22. Cleaning Up for New Runs
# ------------------------------------------------------------
# Call these at the script end to avoid file bloat in your test/dev cluster.
shutil.rmtree("/tmp/predicate_prune", ignore_errors=True)
shutil.rmtree("/tmp/dpp_fact", ignore_errors=True)
shutil.rmtree("/tmp/dpp_dim", ignore_errors=True)
shutil.rmtree("/tmp/parquet_cat_partitioned", ignore_errors=True)
shutil.rmtree("/tmp/delta_evolvable", ignore_errors=True)
shutil.rmtree("/tmp/csv_slow", ignore_errors=True)
shutil.rmtree("/tmp/parquet_fast", ignore_errors=True)
shutil.rmtree("/tmp/too_many_files", ignore_errors=True)
shutil.rmtree("/tmp/few_files", ignore_errors=True)
shutil.rmtree("/tmp/onefile", ignore_errors=True)
shutil.rmtree("/tmp/manyfiles", ignore_errors=True)

# End of Master Script
