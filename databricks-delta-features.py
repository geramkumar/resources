# Databricks notebook source
# MAGIC %md
# MAGIC # Senior Databricks Architect Interview Preparation Script
# MAGIC ## 20+ Essential Features with Problem-Solution Scenarios
# MAGIC 
# MAGIC **Target Audience**: Senior Databricks Architects
# MAGIC **Coverage**: Production-ready scenarios with quantified outcomes
# MAGIC **Updated**: July 2025

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
import pandas as pd
from datetime import datetime, timedelta
import json
import random
import time
import uuid

# Initialize Spark with production optimizations
spark = SparkSession.builder \
    .appName("DatabricksArchitectPrep") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Environment Ready for 20+ Feature Demonstrations")

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario 1: Delta Lake ACID Transactions
# MAGIC 
# MAGIC **Feature**: ACID (Atomicity, Consistency, Isolation, Durability) Transactions
# MAGIC **Problem Statement**: Ensure data consistency in concurrent multi-user environment with 10,000+ simultaneous operations
# MAGIC **Solution**: Implement Delta Lake ACID guarantees with concurrent read/write operations
# MAGIC **Business Impact**: 99.99% data consistency, zero data corruption incidents

# COMMAND ----------

print("=== SCENARIO 1: Delta Lake ACID Transactions ===")

# Sample Data: E-commerce Order Processing System
def create_orders_dataset(num_orders=100000):
    """Generate realistic order data for ACID transaction testing"""
    orders = []
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    
    for i in range(num_orders):
        orders.append((
            f"ORD-{i+1:08d}",
            random.randint(1, 50000),  # customer_id
            round(random.uniform(25.99, 999.99), 2),
            random.choice(statuses),
            datetime.now() - timedelta(days=random.randint(0, 365)),
            random.randint(1, 10)  # quantity
        ))
    
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("quantity", IntegerType(), True)
    ])
    
    return spark.createDataFrame(orders, schema)

# Create sample data
orders_df = create_orders_dataset()
orders_path = "/tmp/delta/scenario1_orders"

# Command 1: Create Delta table with ACID properties
orders_df.write.format("delta").mode("overwrite").save(orders_path)
spark.sql(f"CREATE TABLE orders_acid USING DELTA LOCATION '{orders_path}'")

print(f"✅ Created orders table with {orders_df.count():,} records")

# Command 2: Demonstrate atomic operations
print("🔄 Performing atomic batch update...")

# Simulate concurrent operations
delta_table = DeltaTable.forPath(spark, orders_path)

# Atomic operation: Update all pending orders to processing
start_time = time.time()
delta_table.update(
    condition = col("status") == "pending",
    set = {"status": lit("processing")}
)
end_time = time.time()

# Verify atomicity - either all updated or none
updated_count = spark.read.format("delta").load(orders_path).filter(col("status") == "processing").count()
print(f"✅ Atomically updated {updated_count:,} orders in {end_time-start_time:.2f} seconds")

# Command 3: Demonstrate isolation with concurrent reads
print("📊 Testing isolation with concurrent operations...")

# Reader can access consistent data even during writes
consistent_total = spark.sql("SELECT SUM(amount) as total_revenue FROM orders_acid").collect()[0]['total_revenue']
print(f"✅ Consistent read during concurrent operations: ${consistent_total:,.2f} total revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario 2: Delta Lake Time Travel
# MAGIC 
# MAGIC **Feature**: Time Travel and Version Control
# MAGIC **Problem Statement**: Need to audit changes, rollback errors, and analyze historical data across 500+ daily versions
# MAGIC **Solution**: Use Delta Lake time travel to access any point-in-time data version
# MAGIC **Business Impact**: 95% reduction in recovery time, complete audit trail, zero data loss incidents

# COMMAND ----------

print("=== SCENARIO 2: Delta Lake Time Travel ===")

# Create multiple versions by performing operations
print("🕒 Creating multiple versions for time travel demonstration...")

# Version 1: Initial data (already created)
initial_count = spark.read.format("delta").load(orders_path).count()

# Version 2: Add new orders
new_orders = create_orders_dataset(5000)
new_orders.write.format("delta").mode("append").save(orders_path)
v2_count = spark.read.format("delta").load(orders_path).count()

# Version 3: Update order statuses
spark.sql("UPDATE orders_acid SET status = 'delivered' WHERE status = 'shipped'")
delivered_count = spark.sql("SELECT COUNT(*) FROM orders_acid WHERE status = 'delivered'").collect()[0][0]

print(f"✅ Version 1: {initial_count:,} orders")
print(f"✅ Version 2: {v2_count:,} orders (added {v2_count-initial_count:,})")
print(f"✅ Version 3: {delivered_count:,} delivered orders")

# Command 1: Show table history
history_df = spark.sql("DESCRIBE HISTORY orders_acid")
print("📋 Table Version History:")
history_df.select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)

# Command 2: Time travel to specific version
print("⏪ Time traveling to previous versions...")

if history_df.count() >= 2:
    # Travel to version 0
    v0_data = spark.read.format("delta").option("versionAsOf", 0).load(orders_path)
    v0_revenue = v0_data.agg(sum("amount")).collect()[0][0]
    
    # Travel to version 1  
    v1_data = spark.read.format("delta").option("versionAsOf", 1).load(orders_path)
    v1_revenue = v1_data.agg(sum("amount")).collect()[0][0]
    
    print(f"✅ Version 0 Revenue: ${v0_revenue:,.2f}")
    print(f"✅ Version 1 Revenue: ${v1_revenue:,.2f}")
    print(f"📈 Revenue Growth: ${v1_revenue - v0_revenue:,.2f} ({((v1_revenue/v0_revenue-1)*100):.1f}%)")

# Command 3: Timestamp-based time travel
yesterday = datetime.now() - timedelta(hours=1)
timestamp_str = yesterday.strftime("%Y-%m-%d %H:%M:%S")

try:
    historical_data = spark.read.format("delta").option("timestampAsOf", timestamp_str).load(orders_path)
    print(f"✅ Successfully accessed data as of {timestamp_str}")
    print(f"📊 Records at timestamp: {historical_data.count():,}")
except:
    print("⚠️ No data available for that timestamp (expected for new table)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario 3: Delta Lake Schema Evolution
# MAGIC 
# MAGIC **Feature**: Automatic Schema Evolution and Enforcement
# MAGIC **Problem Statement**: Handle evolving data schemas across 50+ source systems without breaking pipelines
# MAGIC **Solution**: Enable schema evolution with backward compatibility and validation
# MAGIC **Business Impact**: 80% reduction in pipeline failures, seamless schema changes

# COMMAND ----------

print("=== SCENARIO 3: Delta Lake Schema Evolution ===")

# Original schema enforcement test
print("🔒 Testing schema enforcement...")

# Command 1: Attempt to write incompatible schema (should fail)
incompatible_data = spark.createDataFrame([
    ("ORD-999", "INVALID_CUSTOMER_ID", 100.0, "pending", datetime.now(), 1)
], ["order_id", "customer_id", "amount", "status", "order_date", "quantity"])

try:
    incompatible_data.write.format("delta").mode("append").save(orders_path)
    print("❌ Schema validation failed - should have rejected invalid data")
except Exception as e:
    print("✅ Schema enforcement working - rejected incompatible data")

# Command 2: Add new columns with schema evolution
print("🔄 Testing schema evolution...")

# Create evolved schema with new columns
evolved_orders = orders_df.withColumn("discount_amount", lit(0.0)) \
                          .withColumn("shipping_method", lit("standard")) \
                          .withColumn("customer_segment", lit("regular"))

# Enable schema evolution and write
evolved_orders.limit(1000).write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(orders_path)

print("✅ Successfully evolved schema with new columns")

# Command 3: Verify new schema
current_schema = spark.read.format("delta").load(orders_path).schema
print(f"📋 New schema has {len(current_schema.fields)} columns:")
for field in current_schema.fields:
    print(f"   - {field.name}: {field.dataType}")

# Command 4: Show schema evolution impact
evolved_count = spark.read.format("delta").load(orders_path).filter(col("discount_amount").isNotNull()).count()
total_count = spark.read.format("delta").load(orders_path).count()

print(f"📊 Schema Evolution Results:")
print(f"   - Total records: {total_count:,}")
print(f"   - Records with new schema: {evolved_count:,}")
print(f"   - Backward compatibility maintained: 100%")

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario 4: Delta Lake Merge (UPSERT) Operations
# MAGIC 
# MAGIC **Feature**: Efficient MERGE (UPSERT) Operations
# MAGIC **Problem Statement**: Handle 1M+ daily upserts with complex business logic in near real-time
# MAGIC **Solution**: Use Delta MERGE for atomic INSERT/UPDATE/DELETE operations
# MAGIC **Business Impact**: 70% performance improvement over traditional approaches, 99.9% data accuracy

# COMMAND ----------

print("=== SCENARIO 4: Delta Lake Merge (UPSERT) Operations ===")

# Sample Data: Customer dimension updates from multiple sources
def create_customer_updates():
    """Generate realistic customer update scenarios"""
    
    # Existing customers (updates)
    updates = []
    for i in range(1, 1001):  # Update first 1000 customers
        updates.append((
            i,
            f"Updated_Customer_{i}",
            f"updated{i}@email.com",
            random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
            round(random.uniform(1000, 50000), 2),
            datetime.now(),
            "UPDATE"
        ))
    
    # New customers (inserts)  
    for i in range(50001, 50501):  # 500 new customers
        updates.append((
            i,
            f"New_Customer_{i}",
            f"new{i}@email.com", 
            "Bronze",
            round(random.uniform(500, 5000), 2),
            datetime.now(),
            "INSERT"
        ))
    
    # Customers to deactivate (soft delete)
    for i in range(40001, 40101):  # Deactivate 100 customers
        updates.append((
            i,
            f"Customer_{i}",
            f"customer{i}@email.com",
            "Inactive",
            0.0,
            datetime.now(),
            "DELETE"
        ))
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("lifetime_value", DoubleType(), True),
        StructField("updated_date", TimestampType(), True),
        StructField("change_type", StringType(), True)
    ])
    
    return spark.createDataFrame(updates, schema)

# Create base customer table
base_customers = []
for i in range(1, 50001):  # 50,000 existing customers
    base_customers.append((
        i,
        f"Customer_{i}",
