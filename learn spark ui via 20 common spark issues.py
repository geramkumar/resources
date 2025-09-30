"""
Top 15 Spark & Databricks Performance Anti-Patterns (Spark 3.x+)

Structure for each problem block:
1. Problem description (what the bad practice is)
2. BAD script (independent code using Databricks sample dataset)
3. Detailed comments: why this is bad, Spark UI navigation (Jobs, Stages, Executors, SQL/DataFrames, Storage)
4. GOOD script (independent solution using same dataset)
5. Best practice description

Each block is independent: no reuse of DataFrames across blocks.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.storagelevel import StorageLevel

# ============================================================================
# Problem 1: Single-writer bottleneck
# ============================================================================
# BAD PRACTICE: Using repartition(1) before writing forces Spark to collapse all data into one partition.
# This results in a single task doing all the work. It causes long-running jobs, executor OOM risk, and under-utilization.

# BAD SCRIPT
bad_orders = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
bad_orders.repartition(1).write.mode('overwrite').parquet('/tmp/bad_single_writer')

# SPARK UI NAVIGATION:
# - Jobs tab: Find the write job. Click -> see final stage.
# - Stages tab: The last stage shows only 1 task.
# - Executors tab: Only one executor does all work; others idle.
# - Task timeline: A single long task bar.

# GOOD SCRIPT
good_orders = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
good_orders.write.mode('overwrite').parquet('/tmp/good_parallel_writer')

# BEST PRACTICE:
# - Let Spark write with parallelism. Avoid repartition(1) on large datasets.
# - If a single file is strictly required, merge files after the parallel write using optimized utilities.

# ============================================================================
# Problem 2: Missing broadcast in join
# ============================================================================
# BAD PRACTICE: Joining a very large fact table with a small dimension table without broadcasting.
# This triggers a shuffle join, moving large data across the network unnecessarily.

# BAD SCRIPT
fact = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
small_dim = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
result_bad = fact.join(small_dim, on='ProductID').groupBy('ProductID').count()
result_bad.count()

# SPARK UI NAVIGATION:
# - Jobs tab: Open the join job.
# - Stages tab: Shuffle-heavy stage with large read/write bytes.
# - SQL/DataFrames tab: Physical plan shows SortMergeJoin and large Exchange operators.
# - Executors tab: High network and shuffle IO.

# GOOD SCRIPT
from pyspark.sql.functions import broadcast
fact2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
small_dim2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
result_good = fact2.join(broadcast(small_dim2), on='ProductID').groupBy('ProductID').count()
result_good.count()

# BEST PRACTICE:
# - Broadcast small dimension tables (<10MB default) to avoid shuffle.
# - Enable Adaptive Query Execution (AQE) for automatic join strategy selection.

# ============================================================================
# Problem 3: Data skew (hot key)
# ============================================================================
# BAD PRACTICE: Grouping on a skewed column where one value dominates (e.g., 'United States' in flights dataset).
# This creates straggler tasks, slowing the job.

# BAD SCRIPT
flights = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
skewed_result = flights.groupBy('DEST_COUNTRY_NAME').count()
skewed_result.collect()

# SPARK UI NAVIGATION:
# - Stages tab: Task duration histogram shows one very slow task (hot partition).
# - Executors tab: One executor handles a disproportionate workload.
# - SQL/DataFrames tab: Plan shows skewed aggregation.

# GOOD SCRIPT
flights2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
salted = flights2.withColumn('salt', (F.rand()*10).cast(IntegerType()))
salted_agg = salted.groupBy('DEST_COUNTRY_NAME','salt').count()
final = salted_agg.groupBy('DEST_COUNTRY_NAME').agg(F.sum('count'))
final.collect()

# BEST PRACTICE:
# - Use salting or Spark skew join handling with AQE.
# - Monitor Stages for stragglers.

# ============================================================================
# Problem 4: Excessive small files
# ============================================================================
# BAD PRACTICE: Writing thousands of small files by using very high partition counts.
# This leads to storage overhead, slow reads, and high metadata costs.

# BAD SCRIPT
small_df = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
small_df.repartition(1000).write.mode('overwrite').parquet('/tmp/bad_small_files')

# SPARK UI NAVIGATION:
# - Jobs -> final stage shows many tasks.
# - DBFS browser -> thousands of small files in directory.
# - Executors tab -> many very short write tasks.

# GOOD SCRIPT
good_small_df = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
good_small_df.coalesce(8).write.mode('overwrite').parquet('/tmp/good_compacted_files')

# BEST PRACTICE:
# - Avoid too many output files; use coalesce before write.
# - For Delta, use OPTIMIZE for file compaction.

# ============================================================================
# Problem 5: collect() large results to driver
# ============================================================================
# BAD PRACTICE: Using collect() or toPandas() on large DataFrames.
# This brings all data to driver memory and risks OOM.

# BAD SCRIPT
people = spark.read.json('/databricks-datasets/samples/people/people.json')
rows = people.limit(100000).collect()

# SPARK UI NAVIGATION:
# - Jobs tab: collect job appears small.
# - Driver logs: MemoryError or OOM in stderr.
# - Executors tab: tasks complete but driver crashes.

# GOOD SCRIPT
people2 = spark.read.json('/databricks-datasets/samples/people/people.json')
people2.limit(1000).toPandas()

# BEST PRACTICE:
# - Never collect large datasets.
# - Sample small subset or write to Parquet/Delta for distributed access.

# ============================================================================
# Problem 6: Wrong caching level (MEMORY_ONLY)
# ============================================================================
# BAD PRACTICE: Caching a large dataset in MEMORY_ONLY, leading to eviction and recomputation.

# BAD SCRIPT
retail = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
retail.persist(StorageLevel.MEMORY_ONLY)
retail.count()

# SPARK UI NAVIGATION:
# - Storage tab: check cached DataFrame; see evictions.
# - Executors: high GC time.
# - Jobs: repeated recomputation if blocks evicted.

# GOOD SCRIPT
retail2 = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
retail2.persist(StorageLevel.MEMORY_AND_DISK)
retail2.count()
retail2.unpersist()

# BEST PRACTICE:
# - Use MEMORY_AND_DISK for large DataFrames.
# - Always unpersist when finished.

# ============================================================================
# Problem 7: Too many tiny partitions
# ============================================================================
# BAD PRACTICE: Excessive repartition leading to thousands of tasks, scheduling overhead, and wasted resources.

# BAD SCRIPT
flights = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
flights.repartition(20000).count()

# SPARK UI NAVIGATION:
# - Stage -> shows 20k tasks.
# - Executors -> high scheduling overhead.
# - Jobs -> long runtime even with small input.

# GOOD SCRIPT
flights2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
flights2.repartition(200).count()

# BEST PRACTICE:
# - Partition count ~ executors * cores * 2.
# - Avoid extreme repartition counts.

# ============================================================================
# Problem 8: Repeated actions without caching
# ============================================================================
# BAD PRACTICE: Running multiple actions on same DataFrame without caching.

# BAD SCRIPT
trans = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
trans.filter(F.col('Quantity') > 5).count()
trans.filter(F.col('Quantity') > 5).select('ProductID').distinct().count()

# SPARK UI NAVIGATION:
# - Jobs: two similar jobs executed.
# - Stages: repeated file scans.

# GOOD SCRIPT
trans2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
trans2.persist(StorageLevel.MEMORY_AND_DISK)
trans2.count()
trans2.filter(F.col('Quantity') > 5).select('ProductID').distinct().count()
trans2.unpersist()

# BEST PRACTICE:
# - Persist reused DataFrames.
# - Release memory with unpersist.

# ============================================================================
# Problem 9: Wrong shuffle partitions
# ============================================================================
# BAD PRACTICE: Setting spark.sql.shuffle.partitions too high (20k) for moderate data.

# BAD SCRIPT
summary = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
spark.conf.set('spark.sql.shuffle.partitions','20000')
summary.groupBy('DEST_COUNTRY_NAME').count().count()

# SPARK UI NAVIGATION:
# - Stage -> #tasks = 20,000.
# - Executors -> many short tasks.

# GOOD SCRIPT
summary2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
spark.conf.set('spark.sql.shuffle.partitions','200')
summary2.groupBy('DEST_COUNTRY_NAME').count().count()

# BEST PRACTICE:
# - Tune shuffle partitions appropriately.
# - Enable AQE to adapt automatically.

# ============================================================================
# Problem 10: Cross join
# ============================================================================
# BAD PRACTICE: Accidental cross join with no join condition. Leads to dataset explosion.

# BAD SCRIPT
products = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
products.crossJoin(products.limit(100)).count()

# SPARK UI NAVIGATION:
# - SQL/DataFrames tab: shows CartesianProduct.
# - Stages: large shuffle sizes.

# GOOD SCRIPT
products2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
products2.alias('p1').join(products2.alias('p2'), 'ProductID').count()

# BEST PRACTICE:
# - Always specify join condition.
# - Use crossJoin only when logically required and inputs are tiny.

# ============================================================================
# Problem 11: Reading many small CSV files
# ============================================================================
# BAD PRACTICE: Reading many tiny CSV files causes many small tasks, high scheduling overhead.

# BAD SCRIPT
csv_df = spark.read.option('header', True).csv('/databricks-datasets/samples/population-vs-price/data_geo.csv')
csv_df.repartition(1000).write.mode('overwrite').csv('/tmp/bad_csvs', header=True)
spark.read.option('header',True).csv('/tmp/bad_csvs').count()

# SPARK UI NAVIGATION:
# - Jobs -> final stage has 1000 tasks.
# - DBFS -> directory contains many tiny CSV files.

# GOOD SCRIPT
csv_df2 = spark.read.option('header', True).csv('/databricks-datasets/samples/population-vs-price/data_geo.csv')
csv_df2.repartition(4).write.mode('overwrite').csv('/tmp/good_csvs', header=True)
spark.read.option('header',True).csv('/tmp/good_csvs').count()

# BEST PRACTICE:
# - Compact small files.
# - Prefer parquet/delta for efficient reads.

# ============================================================================
# Problem 12: No partition pruning
# ============================================================================
# BAD PRACTICE: Querying a partitioned dataset without filter, scanning all partitions.

# BAD SCRIPT
flights = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
flights.write.mode('overwrite').partitionBy('DEST_COUNTRY_NAME').parquet('/tmp/part_prune')
spark.read.parquet('/tmp/part_prune').count()

# SPARK UI NAVIGATION:
# - SQL tab: FileScan over all partitions.
# - Jobs -> large file read.

# GOOD SCRIPT
spark.read.parquet('/tmp/part_prune').filter(F.col('DEST_COUNTRY_NAME')=='United States').count()

# BEST PRACTICE:
# - Partition on high-cardinality filter columns.
# - Always use partition filters.

# ============================================================================
# Problem 13: Small Delta files, no compaction
# ============================================================================
# BAD PRACTICE: Writing many tiny Delta files without compaction.

# BAD SCRIPT
trans = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
trans.write.format('delta').mode('overwrite').partitionBy('StoreID').save('/tmp/delta_small')

# SPARK UI NAVIGATION:
# - DBFS: directory contains many small files.
# - Subsequent queries have many small read tasks.

# GOOD SCRIPT
# Run OPTIMIZE in Databricks SQL: OPTIMIZE delta.`/tmp/delta_small`
# (or compact via coalesce before write for parquet)

# BEST PRACTICE:
# - Periodically compact Delta tables with OPTIMIZE.

# ============================================================================
# Problem 14: Heavy aggregation memory pressure
# ============================================================================
# BAD PRACTICE: Using collect_list or collect_set on large groups creates large in-memory structures.

# BAD SCRIPT
summary = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
summary.groupBy('DEST_COUNTRY_NAME').agg(F.collect_list('count')).count()

# SPARK UI NAVIGATION:
# - Executors: high memory usage, GC, possible OOM.
# - Jobs: aggregation stage runs long.

# GOOD SCRIPT
summary2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
summary2.groupBy('DEST_COUNTRY_NAME').agg(F.sum('count')).count()

# BEST PRACTICE:
# - Avoid collect_list on large datasets.
# - Use scalable aggregations like sum, avg, approx functions.

# ============================================================================
# Problem 15: AQE disabled
# ============================================================================
# BAD PRACTICE: Running joins/aggregations without Adaptive Query Execution enabled.

# BAD SCRIPT
spark.conf.set('spark.sql.adaptive.enabled','false')
fact = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
products = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
fact.join(products, 'ProductID').groupBy('ProductID').count().count()

# SPARK UI NAVIGATION:
# - SQL tab: shows SortMergeJoin, no dynamic optimization.
# - Stages: large shuffle.

# GOOD SCRIPT
spark.conf.set('spark.sql.adaptive.enabled','true')
fact2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
products2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')
fact2.join(products2, 'ProductID').groupBy('ProductID').count().count()

# BEST PRACTICE:
# - Enable AQE in Spark 3.x+ to dynamically optimize shuffle partitions, skew, and join strategies.





##### Memory issues
# ============================================================================
# Problem 16: Driver OOM from collect() on large dataset
# ============================================================================
# BAD PRACTICE: Using collect() on a huge DataFrame overwhelms driver memory.

# BAD SCRIPT
people = spark.read.json('/databricks-datasets/samples/people/people.json')
rows = people.limit(500000).collect()   # ⚠️ risky if dataset is large

# SPARK UI NAVIGATION:
# - Jobs tab: shows a quick collect job (tasks finish normally).
# - Executors tab: tasks complete fine, no skew.
# - Driver logs: stderr shows OutOfMemoryError (driver crash) if dataset is too large.

# GOOD SCRIPT
people2 = spark.read.json('/databricks-datasets/samples/people/people.json')
sampled = people2.limit(1000).toPandas()
print("Safe sample rows:", sampled.shape)

# BEST PRACTICE:
# - Never collect large DataFrames to driver.
# - Always limit before collect or write to distributed storage (Parquet/Delta).

# ============================================================================
# Problem 17: Executor OOM from wide transformation (large shuffle)
# ============================================================================
# BAD PRACTICE: Performing a wide aggregation without enough partitions causes OOM in executors.

# BAD SCRIPT
flights = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
spark.conf.set("spark.sql.shuffle.partitions", "10")  # too few partitions
bad_agg = flights.groupBy("DEST_COUNTRY_NAME").agg(F.collect_list("count"))
bad_agg.count()

# SPARK UI NAVIGATION:
# - Stages tab: very few tasks (10), each handling huge shuffle blocks.
# - Executors tab: shows OOM or long GC times on a few executors.
# - Task failure logs: OutOfMemoryError in stderr.

# GOOD SCRIPT
flights2 = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
spark.conf.set("spark.sql.shuffle.partitions", "200")  # sensible partition count
good_agg = flights2.groupBy("DEST_COUNTRY_NAME").agg(F.sum("count"))
good_agg.count()

# BEST PRACTICE:
# - Tune shuffle partitions according to data size & cluster cores.
# - Avoid collect_list/collect_set on very large groups; prefer sum/avg/count.


# ============================================================================
# Problem 18: Caching large DataFrame without enough memory
# ============================================================================
# BAD PRACTICE: Persisting a multi-GB DataFrame in MEMORY_ONLY causes eviction, recomputation.

# BAD SCRIPT
retail = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
retail.persist(StorageLevel.MEMORY_ONLY)
retail.count()
# Running more queries on 'retail' will cause block eviction & recomputation.

# SPARK UI NAVIGATION:
# - Storage tab: cached RDD shows fraction stored < 100%, many evicted blocks.
# - Executors tab: shows high GC time, memory pressure.
# - Jobs tab: recomputation triggered for missing blocks.

# GOOD SCRIPT
retail2 = spark.read.parquet('/databricks-datasets/retail/online_retail.parquet')
retail2.persist(StorageLevel.MEMORY_AND_DISK)
retail2.count()
retail2.unpersist()

# BEST PRACTICE:
# - Use MEMORY_AND_DISK for large DataFrames.\n# - Always unpersist after use.\n# - Monitor Storage tab to confirm caching success.

# ============================================================================
# Problem 19: Skewed shuffle causing executor memory blowup
# ============================================================================
# BAD PRACTICE: Skewed keys cause one partition to hold majority of data, filling executor memory.

# BAD SCRIPT
flights = spark.read.parquet('/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')
skewed = flights.withColumn("skew_key", F.when(F.col("DEST_COUNTRY_NAME") == "United States", "US").otherwise("Other"))
skewed.groupBy("skew_key").agg(F.collect_list("count")).count()

# SPARK UI NAVIGATION:
# - Stages tab: task duration histogram shows one straggler task.
# - Executors tab: the executor handling skew partition shows OOM or high memory.\n# - Logs: OutOfMemoryError for that executor.

# GOOD SCRIPT
salted = skewed.withColumn("salt", (F.rand()*10).cast(IntegerType()))
salted_agg = salted.groupBy("skew_key", "salt").agg(F.sum("count").alias("sum_count"))
final = salted_agg.groupBy("skew_key").agg(F.sum("sum_count"))
final.count()

# BEST PRACTICE:\n# - Handle skew with salting or AQE skew join handling.\n# - Avoid aggregating collect_list on skewed keys.


# ============================================================================
# Problem 20: Large broadcast variable causing executor memory blowup
# ============================================================================
# BAD PRACTICE: Broadcasting very large DataFrame (not small enough) overwhelms executor memory.

# BAD SCRIPT
large_df = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')
# Forcing broadcast on a dataset too large\nforced = large_df.hint("broadcast").join(large_df, on="ProductID")\nforced.count()\n\n# SPARK UI NAVIGATION:\n# - SQL/DataFrames tab: shows BroadcastHashJoin with huge broadcast size.\n# - Executors tab: OOM errors when loading broadcast table into executor memory.\n# - Logs: Executor OOM / spill messages.\n\n# GOOD SCRIPT\nsmall_dim = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/products.parquet')\nlarge_fact = spark.read.parquet('/databricks-datasets/learning-spark-v2/retail/transactions/transactions.parquet')\ngood_join = large_fact.join(F.broadcast(small_dim), on=\"ProductID\")\ngood_join.count()\n\n# BEST PRACTICE:\n# - Only broadcast DataFrames comfortably fitting in executor memory (<10MB by default).\n# - For larger tables, rely on shuffle joins or let AQE decide.\n```

---

👉 These 5 new problems (16–20) demonstrate **driver OOM, executor OOM from wide transformations, caching pitfalls, skewed data memory issues, and bad broadcasting**.  

Would you like me to **merge these into the existing 15-problem script** (so you get a single 20-problem “master” script), or keep them as a **separate memory-focused supplement**?

