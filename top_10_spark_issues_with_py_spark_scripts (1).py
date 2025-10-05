"""
Top 20 Spark & Databricks Performance and Error Scenarios — Training-grade PySpark Notebook

Overview
--------
This notebook contains **20 high-value scenarios** encountered in Spark / Databricks production systems. Each scenario follows the same professional structure (matching Scenario 1 quality) so you can run them in a Databricks notebook and **study the Spark UI** to understand root causes and resolutions.

For every scenario you will find:
- INPUT BLOCK — realistic sample data (prefer `/databricks-datasets` where relevant)
- PROBLEM BLOCK — code that reproduces the symptom (safe defaults, small dataset limits included)
- OBSERVATION GUIDE — precise, actionable steps to inspect Spark UI tabs (Jobs, Stages, Tasks, SQL, Executors, Storage), and commands (dbutils/fs, spark.conf) to gather evidence
- SOLUTION BLOCK — concrete fixes (code snippets you can run immediately)
- COMMENTS & BEST PRACTICES — why the problem occurs, what to watch for, expected Spark UI differences after fix

Notes
-----
- Run each scenario in a **separate notebook cell**. Keep the Spark UI open in a second tab to follow stages/tasks as you execute each cell.
- The notebook is designed for Databricks Runtime (Spark 3.x+). Paths use `dbfs:/` and Databricks sample datasets.
- I intentionally avoid defining Python functions; everything is written with pure PySpark DataFrame / SQL / SparkContext APIs.

================================================================================
# IMPORTS & SPARK SESSION (Databricks already provides `spark`) 
================================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("Spark_UI_Performance_Study_20").getOrCreate()

################################################################################
# SCENARIO 1 — SMALL FILE PROBLEM (Reference-quality)
################################################################################
print('
### SCENARIO 1 — SMALL FILE PROBLEM ###')

# --- INPUT BLOCK ---
# Use Databricks sample Parquet to reflect real data (people dataset)
df_people = spark.read.parquet('/databricks-datasets/learning-spark-v2/people/people-10m.parquet')
sample = df_people.limit(20000)

# --- PROBLEM BLOCK ---
problem_path = 'dbfs:/tmp/spark_ui_scenario1_small_files'
sample.repartition(1000).write.mode('overwrite').parquet(problem_path)
print('Wrote with repartition(1000) -> many small files. Inspect DBFS and Spark UI.')

# --- OBSERVATION GUIDE ---
# Spark UI — Jobs -> select job for write -> Stage:
#  - Number of Tasks ≈ 1000 (each task handles tiny data)
#  - Task Duration: many tasks < 1s (setup + commit dominates)
#  - Task Input Size per Task: very small
# DBFS: dbutils.fs.ls(problem_path) -> confirms many files

# --- SOLUTION BLOCK ---
solution_path = 'dbfs:/tmp/spark_ui_scenario1_small_files_solution'
sample.coalesce(20).write.mode('overwrite').parquet(solution_path)
print('Wrote with coalesce(20) -> fewer larger files. Re-check Spark UI: fewer tasks, larger input per task.')

# --- COMMENTS & BEST PRACTICES ---
# - Aim for 64-256MB file size as a rule of thumb.
# - Use coalesce() for final writes to reduce files without shuffle.
# - On Delta: use OPTIMIZE and ZORDER for compaction and performance.
# - Spark UI change: reduced task count, longer per-task processing but far less scheduling overhead.

################################################################################
# SCENARIO 2 — EXECUTOR OOM FROM A VERY LARGE PARTITION
################################################################################
print('
### SCENARIO 2 — EXECUTOR OOM (Large Partition) ###')

# --- INPUT BLOCK ---
# Use retail customers data and inflate one column to make row large
df_customers = spark.read.option('header','true').csv('/databricks-datasets/retail-org/customers/customers.csv')
df_customers_big = df_customers.withColumn('payload', F.lit('X'*50000))

# --- PROBLEM BLOCK ---
# Force single partition to simulate scenario where one task holds too much data
df_customers_big.repartition(1).groupBy('Country').agg(F.count('*').alias('cnt')).show()
print('Single partition executed — watch executor memory and GC in Spark UI.')

# --- OBSERVATION GUIDE ---
# - Spark UI -> Stages: one task with huge Input Size
# - Executors tab: high memory usage, long GC time, possible OOM in logs
# - Driver/Executor logs: look for java.lang.OutOfMemoryError

# --- SOLUTION BLOCK ---
df_customers_big.repartition(100).groupBy('Country').agg(F.count('*').alias('cnt')).show()
print('Distributed partitions -> reduced per-task memory. Check Spark UI for balanced executors.')

# --- COMMENTS & BEST PRACTICES ---
# - Avoid small number of very large partitions; size partitions to ~128MB.
# - Use spark.sql.shuffle.partitions tuning and AQE.
# - Observe Task Time vs GC Time in Executors tab.

################################################################################
# SCENARIO 3 — JOIN SKEW (HOT KEY HOTSPOT)
################################################################################
print('
### SCENARIO 3 — JOIN SKEW ###')

# --- INPUT BLOCK ---
df_orders = spark.read.option('header','true').csv('/databricks-datasets/retail-org/orders/orders.csv')
df_customers = spark.read.option('header','true').csv('/databricks-datasets/retail-org/customers/customers.csv')
# create skew by replacing many ids with a single hot key
df_orders_skew = df_orders.withColumn('CustomerId', F.when(F.rand() > 0.8, 'HOT_KEY').otherwise(F.col('CustomerId')))

# --- PROBLEM BLOCK ---
joined = df_orders_skew.join(df_customers, on='CustomerId', how='left')
joined.count()
print('Join executed — expect skew; examine stage shuffle distribution in Spark UI.')

# --- OBSERVATION GUIDE ---
# - SQL tab -> find join stage: check Shuffle Read Size per task
# - Look for long-tail tasks (one/few tasks >> others)
# - Check task skew in Stage Tasks table: input metrics, durations

# --- SOLUTION BLOCK ---
# Enable AQE which can handle skew automatically
spark.conf.set('spark.sql.adaptive.enabled','true')
spark.conf.set('spark.sql.adaptive.skewJoin.enabled','true')
print('AQE + skewJoin enabled. Re-run the join to observe improved task balance in Spark UI.')

# --- COMMENTS & BEST PRACTICES ---
# - Salting is a manual option when AQE is not available.
# - Broadcast small dimension tables where possible to avoid shuffle.
# - Spark UI before fix: huge variance in task durations; after fix: more uniform durations.

################################################################################
# SCENARIO 4 — DRIVER OOM (collect/toPandas misuse)
################################################################################
print('
### SCENARIO 4 — DRIVER OOM (collect/toPandas) ###')

# --- INPUT BLOCK ---
df_people = spark.read.parquet('/databricks-datasets/learning-spark-v2/people/people-10m.parquet')

# --- PROBLEM BLOCK ---
print('Do NOT run df_people.collect() on this dataset — it can exhaust driver memory.')
# Uncomment only if you intentionally want to cause driver memory issues
# df_people.collect()

# --- OBSERVATION GUIDE ---
# - Local stage will appear for collect()/toPandas() as driver-bound action
# - Driver logs and cluster metrics will show memory pressure

# --- SOLUTION BLOCK ---
df_sample = df_people.limit(1000).toPandas()
print('Collected 1,000 rows safely to pandas. For larger required analysis, write to storage and download.')

# --- COMMENTS & BEST PRACTICES ---
# - Always use limit()/sample() to reduce data before collecting.
# - Use display() in Databricks for quick inspection; it streams results safely.

################################################################################
# SCENARIO 5 — EXCESSIVE SHUFFLES DUE TO DEFAULT spark.sql.shuffle.partitions
################################################################################
print('
### SCENARIO 5 — EXCESSIVE SHUFFLES ###')

# --- INPUT BLOCK ---
df_orders = spark.read.option('header','true').csv('/databricks-datasets/retail-org/orders/orders.csv')

# --- PROBLEM BLOCK ---
print('Current spark.sql.shuffle.partitions =', spark.conf.get('spark.sql.shuffle.partitions'))
df_orders.groupBy('Country').agg(F.count('*')).count()

# --- OBSERVATION GUIDE ---
# - SQL tab: number of tasks equals spark.sql.shuffle.partitions
# - If dataset small, many tasks process tiny amounts of data
# - Tasks show high scheduling overhead relative to processing time

# --- SOLUTION BLOCK ---
spark.conf.set('spark.sql.shuffle.partitions','20')
df_orders.groupBy('Country').agg(F.count('*')).count()
print('Reduced shuffle partitions -> fewer, more efficient tasks. Also consider enabling AQE.')

# --- COMMENTS & BEST PRACTICES ---
# - Tune shuffle partitions according to data size: partitions ~= totalSize/targetPartitionSize
# - AQE auto-coalesces partitions at runtime for better defaults

################################################################################
# SCENARIO 6 — IMPROPER CACHING / MEMORY PRESSURE
################################################################################
print('
### SCENARIO 6 — IMPROPER CACHING ###')

# --- INPUT BLOCK ---
df_orders = spark.read.option('header','true').csv('/databricks-datasets/retail-org/orders/orders.csv')

# --- PROBLEM BLOCK ---
df_us = df_orders.filter(F.col('Country') == 'United States').persist(StorageLevel.MEMORY_ONLY)
df_ca = df_orders.filter(F.col('Country') == 'Canada').persist(StorageLevel.MEMORY_ONLY)
# materialize
print('Materializing caches...')
df_us.count(); df_ca.count()
print('Caches materialized; monitor Storage and Executors tabs')

# --- OBSERVATION GUIDE ---
# - Storage tab: list cached RDDs/DataFrames, sizes in memory
# - Executors tab: memory usage, GC activity and potential eviction
# - If too many caches or MEMORY_ONLY and insufficient memory, eviction occurs leading to recomputation

# --- SOLUTION BLOCK ---
df_us.unpersist(); df_ca.unpersist()
print('Unpersisted cached datasets; memory freed. Consider MEMORY_AND_DISK for larger caches.')

# --- COMMENTS & BEST PRACTICES ---
# - Cache selectively: only when dataset reused multiple times.
# - Use MEMORY_AND_DISK or DISK_ONLY for very large caches.

################################################################################
# SCENARIO 7 — SLOW PYTHON UDFS (High Serialization & CPU Overhead)
################################################################################
print('
### SCENARIO 7 — SLOW PYTHON UDFS ###')

# --- INPUT BLOCK ---
data = [(i, 'text_' + str(i)) for i in range(50000)]
df_text = spark.createDataFrame(data, ['id', 'txt'])

# --- PROBLEM BLOCK ---
print('Avoid per-row Python UDFs; they run in Python worker processes with serialization cost.')
# Example of bad pattern (commented out):
# from pyspark.sql.functions import udf
# bad_udf = udf(lambda s: s[::-1])
# df_text.withColumn('rev', bad_udf(F.col('txt'))).count()

# --- OBSERVATION GUIDE ---
# - Spark UI -> SQL tab: PythonUDF stages show higher task CPU and serialization times
# - Executors: increased Python worker activity and longer GC times

# --- SOLUTION BLOCK ---
df_text.withColumn('rev', F.expr('reverse(txt)')).count()
print('Used built-in reverse() expression — executed in JVM efficiently.')

# --- COMMENTS & BEST PRACTICES ---
# - Prefer native SQL functions or vectorized pandas UDFs.
# - Monitor task CPU vs wall-clock time in Spark UI; Python UDFs show disproportionate CPU.

################################################################################
# SCENARIO 8 — BROADCAST JOIN MISUSE
################################################################################
print('
### SCENARIO 8 — BROADCAST JOIN MISUSE ###')

# --- INPUT BLOCK ---
df_people_small = df_people.limit(5000).select('name', 'gender')
df_people_large = df_people.limit(200000)

# --- PROBLEM BLOCK ---
print('Do not broadcast a large dataset — can cause executor OOM.')
# e.g., bad pattern:
# df_people_large.join(F.broadcast(df_people_large), 'name').count()

# --- OBSERVATION GUIDE ---
# - SQL Plan shows BroadcastHashJoin when broadcast applied
# - Executors tab may reveal memory spikes if broadcast oversized

# --- SOLUTION BLOCK ---
df_people_large.join(F.broadcast(df_people_small), 'name').count()
print('Broadcasted small table only — efficient join. Verify Query Plan and Executor memory.')

# --- COMMENTS & BEST PRACTICES ---
# - Only broadcast truly small tables (default threshold ~10MB)
# - Tune spark.sql.autoBroadcastJoinThreshold if needed

################################################################################
# SCENARIO 9 — LARGE CLOSURE SERIALIZATION (Driver Object Captured)
################################################################################
print('
### SCENARIO 9 — LARGE CLOSURE SERIALIZATION ###')

# --- INPUT BLOCK ---
large_list = list(range(1000000))
rdd = spark.sparkContext.parallelize(range(1000), 10)

# --- PROBLEM BLOCK ---
print('Capturing large_list in closure causes large serialized task size and slow task startup.')
rdd.map(lambda x: x + large_list[0]).count()

# --- OBSERVATION GUIDE ---
# - Stage details: Serialized Task Size (if visible) or long task startup times
# - Driver logs: large serialization payload may be printed

# --- SOLUTION BLOCK ---
big_bcast = spark.sparkContext.broadcast(large_list)
rdd.map(lambda x: x + big_bcast.value[0]).count()
print('Using broadcast variable reduces task serialization overhead.')

# --- COMMENTS & BEST PRACTICES ---
# - Use sc.broadcast for large, read-only objects.
# - Avoid capturing large data structures in closures.

################################################################################
# SCENARIO 10 — SCHEMA MISMATCH / UNION ERRORS
################################################################################
print('
### SCENARIO 10 — SCHEMA MISMATCH ###')

# --- INPUT BLOCK ---
df1 = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])
df2 = spark.createDataFrame([(3, 100), (4, 200)], ['id', 'name'])

# --- PROBLEM BLOCK ---
print('Union of df1 and df2 will fail due to schema mismatch (string vs int).')
# df1.union(df2).show()  # Uncomment to reproduce

# --- OBSERVATION GUIDE ---
# - Failed job shows AnalysisException in Spark UI job details
# - Query plan shows mismatch in column types

# --- SOLUTION BLOCK ---
df2_cast = df2.withColumn('name', F.col('name').cast('string'))
df_union = df1.union(df2_cast)
df_union.show()

# --- COMMENTS & BEST PRACTICES ---
# - Normalize schema types when unioning files/tables (use cast or select with explicit schema)
# - Delta supports mergeSchema but use with caution and test performance

################################################################################
# SCENARIO 11 — TOO MANY SMALL PARTITIONS FROM FILE FORMAT (e.g., many small CSVs)
################################################################################
print('
### SCENARIO 11 — MANY SMALL PARTITIONS FROM FILE FORMAT ###')

# --- INPUT BLOCK ---
# Create many tiny CSV files by writing small_splits DataFrame repeatedly
base = spark.range(0, 10000).withColumn('category', (F.rand()*10).cast('int'))
small_files_path = 'dbfs:/tmp/spark_ui_many_small_csvs'
base.repartition(1000).write.mode('overwrite').csv(small_files_path)
print('Wrote many tiny CSV files. Check number of files with dbutils.fs.ls().')

# --- PROBLEM BLOCK ---
# Reading many small files causes job to spawn many short tasks
spark.read.csv(small_files_path).count()

# --- OBSERVATION GUIDE ---
# - Jobs/Stages: many tasks, small Input Size per task
# - Storage: many small files increase metadata overhead on cloud storage (S3/ADLS)

# --- SOLUTION BLOCK ---
# Consolidate files by reading and rewriting in larger partitions
consolidated_path = 'dbfs:/tmp/spark_ui_many_small_csvs_consolidated'
spark.read.csv(small_files_path).coalesce(20).write.mode('overwrite').parquet(consolidated_path)
print('Consolidated into fewer Parquet files; re-run read and observe reduced tasks in Spark UI.')

# --- COMMENTS & BEST PRACTICES ---
# - Prefer Parquet over CSV for columnar storage and fewer files.
# - Use file compaction strategies and Delta OPTIMIZE where available.

################################################################################
# SCENARIO 12 — SLOW METASTORE / HIVE METASTORE LATENCY
################################################################################
print('
### SCENARIO 12 — SLOW METASTORE LATENCY ###')

# --- INPUT BLOCK ---
# Simulate many metadata calls: create many small tables or repeatedly list tables
for i in range(50):
    spark.sql(f"CREATE TABLE IF NOT EXISTS tmp_meta_test_{i} (id INT) USING parquet")

# --- PROBLEM BLOCK ---
# Listing databases/tables repeatedly or querying many small partitions causes many metastore calls
for i in range(50):
    _ = spark.catalog.listTables('default')
print('Executed many metastore operations; observe latency if metastore is slow.')

# --- OBSERVATION GUIDE ---
# - Look at driver logs and application timeline for bursts of metastore RPCs
# - Slow metastore increases overall query latency, affecting job submission times

# --- SOLUTION BLOCK ---
# Reduce metastore chatter: avoid creating excessive small tables; cache metadata where possible
print('Best practice: reduce frequent DDL/metadata operations in hot paths; use Glue/managed metastore tuned for scale.')

# --- COMMENTS & BEST PRACTICES ---
# - For high-scale environments, ensure metastore is provisioned for heavy loads.
# - Use caching of table metadata in the application where safe.

################################################################################
# SCENARIO 13 — INEFFICIENT PARTITION PRUNING (Too Many Partitions Scanned)
################################################################################
print('
### SCENARIO 13 — INEFFICIENT PARTITION PRUNING ###')

# --- INPUT BLOCK ---
# Create a partitioned Parquet (simulate date partitions)
parted = spark.range(0, 10000).withColumn('dt', F.concat(F.lit('2021-09-'), ((F.rand()*30).cast('int')+1).cast('string')))
path_pp = 'dbfs:/tmp/spark_ui_partitioned'
parted.write.mode('overwrite').partitionBy('dt').parquet(path_pp)

# --- PROBLEM BLOCK ---
# Query without filter -> scans ALL partitions
spark.read.parquet(path_pp).count()
print('Full scan executed; check Spark UI for large scan across many files/partitions.')

# --- OBSERVATION GUIDE ---
# - In Stage details: large Shuffle/Read sizes and many files read
# - Check physical plan (explain) to see lack of partition pruning

# --- SOLUTION BLOCK ---
spark.read.parquet(path_pp).filter(F.col('dt') == '2021-09-15').count()
print('Filtered read -> partition pruning limits files read. Spark UI: fewer input files and faster stage.')

# --- COMMENTS & BEST PRACTICES ---
# - Always push filters on partition columns to leverage pruning.
# - Check query explain() and Spark UI Input Size metrics to validate pruning.

################################################################################
# SCENARIO 14 — WIDE DEPENDENCIES CAUSING LARGE SHUFFLE (groupBy with many keys)
################################################################################
print('
### SCENARIO 14 — WIDE DEPENDENCIES & LARGE SHUFFLE ###')

# --- INPUT BLOCK ---
df = spark.range(0, 200000).withColumn('grp', (F.rand()*1000).cast('int'))

# --- PROBLEM BLOCK ---
# groupBy with high cardinality creates expensive shuffle
spark.conf.set('spark.sql.shuffle.partitions', '200')
df.groupBy('grp').agg(F.count('*')).count()
print('High-cardinality groupBy caused large shuffle. Observe shuffle read/write in Spark UI.')

# --- OBSERVATION GUIDE ---
# - Stage: large Shuffle Write Size and Shuffle Read Size
# - Tasks may be skewed if key distribution uneven

# --- SOLUTION BLOCK ---
# Consider approximate aggregation or pre-aggregation or increase partitions
spark.conf.set('spark.sql.shuffle.partitions', '400')
df.groupBy('grp').agg(F.count('*')).count()
print('Increased shuffle partitions to distribute load; consider approximate algorithms for heavy cardinality.')

# --- COMMENTS & BEST PRACTICES ---
# - For very high cardinality, pre-aggregate or use HyperLogLog for approximations.
# - Monitor Shuffle metrics in Spark UI to tune partitions and cluster size.

################################################################################
# SCENARIO 15 — HIGH GC TIME DUE TO MEMORY PRESSURE
################################################################################
print('
### SCENARIO 15 — HIGH GC TIME ###')

# --- INPUT BLOCK ---
# Create memory-intensive DataFrame
mem_df = spark.range(0, 200000).withColumn('payload', F.lit('X'*1000))

# --- PROBLEM BLOCK ---
mem_df.repartition(10).groupBy().agg(F.collect_list('payload')).count()
print('This aggregation collects large lists in memory -> heavy GC and possible OOM.')

# --- OBSERVATION GUIDE ---
# - Executors tab: high GC time percentage
# - Task timeline: long GC pauses and increased task durations

# --- SOLUTION BLOCK ---
# Avoid collect_list on massive payloads; use count or write intermediate results to disk
mem_df.repartition(100).groupBy().agg(F.count('*')).show()
print('Changed operation to be less memory-intensive and increased partitions to reduce per-task memory.')

# --- COMMENTS & BEST PRACTICES ---
# - Monitor GC metrics and tune heap size or reduce memory pressure via partitioning.
# - Use MEMORY_AND_DISK for caching to avoid excessive GC.

################################################################################
# SCENARIO 16 — FILE SYSTEM LISTING OVERHEAD (Too many objects in S3/ADLS)
################################################################################
print('
### SCENARIO 16 — FILE SYSTEM LISTING OVERHEAD ###')

# --- INPUT BLOCK ---
# Simulate many small files written to a prefix
many_files_path = 'dbfs:/tmp/spark_ui_many_objects'
base = spark.range(0, 100000).withColumn('cat', (F.rand()*1000).cast('int'))
base.repartition(2000).write.mode('overwrite').parquet(many_files_path)

# --- PROBLEM BLOCK ---
# Listing or opening directory with 1000s of files causes latency
_ = dbutils.fs.ls(many_files_path)
print('Listed many objects. Observe latency; metastore / filesystem calls may be slow.')

# --- OBSERVATION GUIDE ---
# - Slow job start times due to listing many objects
# - Check driver logs for list-object call durations

# --- SOLUTION BLOCK ---
# Compact files and use partitioning strategy to reduce number of objects per prefix
compacted_path = 'dbfs:/tmp/spark_ui_many_objects_compacted'
spark.read.parquet(many_files_path).coalesce(50).write.mode('overwrite').parquet(compacted_path)
print('Compacted files to fewer objects; re-check listing speed and job startup time.')

# --- COMMENTS & BEST PRACTICES ---
# - Avoid huge number of small files in a single S3/ADLS prefix.
# - Use hierarchical partitioning and compaction.

################################################################################
# SCENARIO 17 — TOO MANY STAGES (Complex Query Plan Causes Stage Explosion)
################################################################################
print('
### SCENARIO 17 — TOO MANY STAGES ###')

# --- INPUT BLOCK ---
df = spark.range(0, 100000).withColumn('c1', F.rand()).withColumn('c2', F.rand())

# --- PROBLEM BLOCK ---
# Construct a query with many narrow transformations chaining shuffles
q = df.repartition(100).groupBy('id').agg(F.count('*').alias('a'))
q = q.repartition(100).groupBy('a').agg(F.count('*').alias('b'))
q = q.repartition(100).groupBy('b').agg(F.count('*').alias('c'))
q.count()
print('Multiple repartition/groupBy chains produced many stages. Observe stage graph.')

# --- OBSERVATION GUIDE ---
# - Spark UI -> DAG Visualization: many stages and shuffle boundaries
# - Numerous stages increase coordination overhead and failure surface

# --- SOLUTION BLOCK ---
# Fuse transformations where possible; reduce unnecessary repartitions
spark.conf.set('spark.sql.shuffle.partitions', '200')
# Re-write logic to minimize shuffles; example: combine operations or use map-side combines
print('Refactor query to reduce shuffle boundaries. Observe simplified stage graph in Spark UI.')

# --- COMMENTS & BEST PRACTICES ---
# - Avoid repeated repartitioning unless necessary for data distribution.
# - Use explain() to inspect physical plan and reduce shuffle boundaries.

################################################################################
# SCENARIO 18 — SLOW BROADCAST JOIN DUE TO NETWORK/Serializer
################################################################################
print('
### SCENARIO 18 — SLOW BROADCAST JOIN DUE TO NETWORK/Serializer ###')

# --- INPUT BLOCK ---
df_dim = df_people.limit(5000).select('name','gender')
df_fact = df_people.limit(200000)

# --- PROBLEM BLOCK ---
# Broadcasting many columns or complex nested structs may bloat broadcast size
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', str(10*1024*1024))  # 10MB
joined = df_fact.join(F.broadcast(df_dim), 'name')
joined.count()
print('Broadcast join done; check serialized broadcast size in driver logs and executor memory.')

# --- OBSERVATION GUIDE ---
# - Check query plan for BroadcastHashJoin
# - Driver logs: size of serialized broadcast
# - Executors: memory spikes when receiving broadcast block

# --- SOLUTION BLOCK ---
# Reduce broadcast size: select only required columns or reduce rows
df_dim_small = df_dim.select('name')
df_fact.join(F.broadcast(df_dim_small), 'name').count()
print('Reduced broadcast payload; lower memory and network pressure.')

# --- COMMENTS & BEST PRACTICES ---
# - When broadcasting, project only necessary columns; avoid nested/exploded structs
# - Monitor broadcast size in driver logs

################################################################################
# SCENARIO 19 — TOO MANY SMALL TASKS DUE TO MAP SIDE SMALL PARTITIONING
################################################################################
print('
### SCENARIO 19 — TOO MANY SMALL TASKS DUE TO MAP-SIDE ###')

# --- INPUT BLOCK ---
# Small random dataset partitioned into many tiny partitions
tiny = spark.range(0, 10000).repartition(2000)

# --- PROBLEM BLOCK ---
# Any action spawns 2000 very small tasks -> scheduling overhead
tiny.count()
print('Many small tasks executed. Observe scheduling overhead in Spark UI Task timing.')

# --- OBSERVATION GUIDE ---
# - Tasks table: many tasks with <100ms durations
# - Overhead dominated by scheduling, task deserialization, and commit

# --- SOLUTION BLOCK ---
tiny.coalesce(100).count()
print('Coalesced to 100 partitions before action -> fewer tasks, reduced overhead.')

# --- COMMENTS & BEST PRACTICES ---
# - Avoid over-partitioning relative to data size and cluster cores
# - Rule: partitions ~= parallelism * (1-2) factor; aim for 2-4 tasks per core

################################################################################
# SCENARIO 20 — STUCK TASKS DUE TO EXTERNAL SERVICE LATENCY (e.g., calling external API inside map)
################################################################################
print('
### SCENARIO 20 — STUCK TASKS DUE TO EXTERNAL SERVICE LATENCY ###')

# --- INPUT BLOCK ---
# Simulate external call latency by sleeping inside mapPartitions (use small dataset)
rdd = spark.sparkContext.parallelize(range(100), 10)

# --- PROBLEM BLOCK ---
# WARNING: This is a synthetic simulation. Keep small to avoid cluster issues.
print('Simulating external call with time.sleep inside mapPartitions (do NOT call on prod clusters)')
# Example (commented out to avoid accidental run):
# import time
# def slow_partition(it):
#     for x in it:
#         time.sleep(0.1)  # simulate 100ms external call per record
#         yield x
# rdd.mapPartitions(slow_partition).count()

# --- OBSERVATION GUIDE ---
# - Spark UI -> Tasks: long-running tasks stuck in RUNNING state
# - Executors: threads blocked, low CPU but high wall-clock time
# - Job progress stalls and may time out

# --- SOLUTION BLOCK ---
# Best practice: use async bulk calls, batching, or write to queue and process externally
print('Avoid per-record external calls. Use batching, caching external results, or pre-fetch via broadcast.')

# --- COMMENTS & BEST PRACTICES ---
# - External calls inside tasks increase tail latency and reduce throughput.
# - Use asynchronous/batched calls or offload to streaming / micro-batch systems when necessary.

print('
✅ Completed 20 scenarios.
')
print('Next steps: I can export this notebook as a .dbc archive for direct import into Databricks workspace.')
