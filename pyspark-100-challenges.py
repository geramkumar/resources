"""
PySpark Script: Complete 100 Senior Databricks Engineer Interview Challenges
Author: Interview Preparation Guide
Year: 2024
Description: Comprehensive collection of scenario-based challenges for senior roles
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS
from pyspark.streaming import StreamingContext
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Complete 100 Senior Databricks Engineer Challenges") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("="*80)
print("COMPLETE 100 SENIOR DATABRICKS ENGINEER INTERVIEW CHALLENGES")
print("="*80)

# =============================================================================
# SIMPLE LEVEL CHALLENGES (1-30)
# =============================================================================

print("\n" + "="*50)
print("SIMPLE LEVEL CHALLENGES (1-30)")
print("="*50)

# Challenge 1: Basic DataFrame Operations
print("\n1. BASIC DATAFRAME OPERATIONS")
print("Problem: Create a DataFrame from sample employee data and perform basic operations")
print("Expected: Filter, select, and count operations")

employee_data = [
    (1, "John", "Engineering", 75000, 28),
    (2, "Sarah", "Marketing", 65000, 32),
    (3, "Mike", "Engineering", 80000, 35),
    (4, "Lisa", "HR", 55000, 29),
    (5, "David", "Engineering", 90000, 40),
    (6, "Emma", "Marketing", 70000, 26),
    (7, "James", "Finance", 72000, 31)
]

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

df_employees = spark.createDataFrame(employee_data, employee_schema)

# Solution
print("Solution:")
engineers = df_employees.filter(col("department") == "Engineering")
high_earners = df_employees.filter(col("salary") > 70000)
avg_salary = df_employees.agg(avg("salary").alias("avg_salary")).collect()[0]["avg_salary"]
print(f"Engineers count: {engineers.count()}")
print(f"High earners count: {high_earners.count()}")
print(f"Average salary: ${avg_salary:,.2f}")

# Challenge 2: Data Type Conversions
print("\n2. DATA TYPE CONVERSIONS")
print("Problem: Convert string dates to timestamp and handle null values")

date_data = [
    (1, "2024-01-15", "100.50"),
    (2, "2024-02-20", "200.75"),
    (3, "invalid_date", "150.25"),
    (4, "2024-03-10", None),
    (5, "2024-04-05", "300.00"),
    (6, None, "250.80"),
    (7, "2024-05-12", "invalid_amount")
]

df_dates = spark.createDataFrame(date_data, ["id", "date_str", "amount_str"])

# Solution
print("Solution:")
df_converted = df_dates.withColumn("date_converted", 
                                 to_timestamp(col("date_str"), "yyyy-MM-dd")) \
                     .withColumn("amount_converted", 
                               col("amount_str").cast(DoubleType())) \
                     .withColumn("is_valid_date", 
                               col("date_converted").isNotNull()) \
                     .withColumn("is_valid_amount", 
                               col("amount_converted").isNotNull())

df_converted.show()

# Challenge 3: String Operations
print("\n3. STRING OPERATIONS")
print("Problem: Clean and standardize customer names and extract domain from email")

customer_data = [
    (1, "  john DOE  ", "john.doe@gmail.com", "New York"),
    (2, "sarah_smith", "sarah@company.co.uk", "London"),
    (3, "Mike-Johnson", "mike.j@yahoo.com", "Chicago"),
    (4, "lisa.brown", "lisa@hotmail.com", "Boston"),
    (5, "DAVID_WILSON", "d.wilson@outlook.com", "Seattle"),
    (6, "emma@test", "emma@test.org", "Miami"),
    (7, "james123", "james@domain.net", "Denver")
]

df_customers = spark.createDataFrame(customer_data, ["id", "name", "email", "city"])

# Solution
print("Solution:")
df_clean_customers = df_customers.withColumn("clean_name", 
                                           regexp_replace(
                                               trim(upper(col("name"))), 
                                               "[^A-Z]", " ")) \
                                .withColumn("email_domain", 
                                          regexp_extract(col("email"), "@(.+)", 1)) \
                                .withColumn("name_length", length(col("name"))) \
                                .withColumn("is_valid_email", 
                                          col("email").rlike("^[^@]+@[^@]+\\.[^@]+$"))

df_clean_customers.show(truncate=False)

# Challenge 4: Aggregations with GroupBy
print("\n4. AGGREGATIONS WITH GROUPBY")
print("Problem: Calculate department-wise statistics from sales data")

sales_data = [
    (1, "Electronics", "TV", 1200, "2024-01-15"),
    (2, "Electronics", "Phone", 800, "2024-01-16"),
    (3, "Clothing", "Shirt", 50, "2024-01-15"),
    (4, "Electronics", "Laptop", 1500, "2024-01-17"),
    (5, "Clothing", "Jeans", 80, "2024-01-16"),
    (6, "Home", "Chair", 200, "2024-01-15"),
    (7, "Home", "Table", 300, "2024-01-17"),
    (8, "Electronics", "Tablet", 600, "2024-01-18"),
    (9, "Clothing", "Dress", 120, "2024-01-16")
]

df_sales = spark.createDataFrame(sales_data, ["id", "category", "product", "amount", "date"])

# Solution
print("Solution:")
dept_stats = df_sales.groupBy("category") \
                   .agg(count("*").alias("total_sales"),
                        sum("amount").alias("total_revenue"),
                        avg("amount").alias("avg_sale"),
                        max("amount").alias("max_sale"),
                        min("amount").alias("min_sale"))

dept_stats.show()

# Challenge 5: Window Functions - Basic Ranking
print("\n5. WINDOW FUNCTIONS - BASIC RANKING")
print("Problem: Rank employees by salary within each department")

window_spec = Window.partitionBy("department").orderBy(desc("salary"))

# Solution
print("Solution:")
df_ranked = df_employees.withColumn("salary_rank", 
                                  rank().over(window_spec)) \
                       .withColumn("salary_dense_rank", 
                                 dense_rank().over(window_spec)) \
                       .withColumn("salary_row_number", 
                                 row_number().over(window_spec))

df_ranked.show()

# Challenge 6: Handling Missing Values
print("\n6. HANDLING MISSING VALUES")
print("Problem: Clean dataset with various types of missing values")

missing_data = [
    (1, "Alice", 25, 50000.0, "Engineering"),
    (2, "Bob", None, 60000.0, "Marketing"),
    (3, "Charlie", 30, None, "Engineering"),
    (4, None, 35, 70000.0, "HR"),
    (5, "Diana", 28, 55000.0, None),
    (6, "Eve", 0, 48000.0, "Finance"),
    (7, "Frank", 45, -1, "Engineering"),
    (8, "Grace", 32, 65000.0, "Marketing")
]

df_missing = spark.createDataFrame(missing_data, ["id", "name", "age", "salary", "dept"])

# Solution
print("Solution:")
median_age = df_missing.filter(col("age") > 0).approxQuantile("age", [0.5], 0.01)[0]
median_salary = df_missing.filter(col("salary") > 0).approxQuantile("salary", [0.5], 0.01)[0]

df_cleaned = df_missing.withColumn("age_clean", 
                                 when((col("age").isNull()) | (col("age") <= 0), median_age)
                                 .otherwise(col("age"))) \
                     .withColumn("salary_clean", 
                               when((col("salary").isNull()) | (col("salary") <= 0), median_salary)
                               .otherwise(col("salary"))) \
                     .withColumn("name_clean", 
                               when(col("name").isNull(), "Unknown")
                               .otherwise(col("name"))) \
                     .withColumn("dept_clean", 
                               when(col("dept").isNull(), "Unassigned")
                               .otherwise(col("dept")))

df_cleaned.show()

# Challenge 7: Date Operations
print("\n7. DATE OPERATIONS")
print("Problem: Extract date components and calculate date differences")

event_data = [
    (1, "2024-01-15 10:30:00", "2024-01-20 14:45:00", "Conference"),
    (2, "2024-02-10 09:00:00", "2024-02-12 17:00:00", "Workshop"),
    (3, "2024-03-05 13:15:00", "2024-03-05 16:30:00", "Meeting"),
    (4, "2024-04-20 08:00:00", "2024-04-22 20:00:00", "Training"),
    (5, "2024-05-15 11:00:00", "2024-05-15 12:00:00", "Webinar"),
    (6, "2024-06-10 14:00:00", "2024-06-11 10:00:00", "Seminar"),
    (7, "2024-07-25 16:30:00", "2024-07-26 09:00:00", "Forum")
]

df_events = spark.createDataFrame(event_data, ["id", "start_time", "end_time", "event_type"])
df_events = df_events.withColumn("start_time", to_timestamp(col("start_time"))) \
                   .withColumn("end_time", to_timestamp(col("end_time")))

# Solution
print("Solution:")
df_date_analysis = df_events.withColumn("duration_hours", 
                                      (unix_timestamp("end_time") - unix_timestamp("start_time")) / 3600) \
                          .withColumn("start_year", year("start_time")) \
                          .withColumn("start_month", month("start_time")) \
                          .withColumn("start_day", dayofmonth("start_time")) \
                          .withColumn("start_weekday", dayofweek("start_time")) \
                          .withColumn("is_weekend", 
                                    when(dayofweek("start_time").isin([1, 7]), True).otherwise(False))

df_date_analysis.show()

# Challenge 8: JSON Data Processing
print("\n8. JSON DATA PROCESSING")
print("Problem: Parse and extract data from JSON strings")

json_data = [
    (1, '{"name": "John", "skills": ["Python", "Spark", "SQL"], "experience": 5}'),
    (2, '{"name": "Sarah", "skills": ["Java", "Kafka", "AWS"], "experience": 3}'),
    (3, '{"name": "Mike", "skills": ["Scala", "Hadoop", "Hive"], "experience": 7}'),
    (4, '{"name": "Lisa", "skills": ["Python", "Docker", "Kubernetes"], "experience": 4}'),
    (5, '{"name": "David", "skills": ["R", "Tableau", "Power BI"], "experience": 6}'),
    (6, '{"name": "Emma", "skills": ["Python", "TensorFlow", "PyTorch"], "experience": 2}'),
    (7, '{"name": "James", "skills": ["Go", "MongoDB", "Redis"], "experience": 8}')
]

df_json = spark.createDataFrame(json_data, ["id", "profile_json"])

# Solution
print("Solution:")
schema = StructType([
    StructField("name", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("experience", IntegerType(), True)
])

df_parsed = df_json.withColumn("profile", from_json(col("profile_json"), schema)) \
                 .select("id", "profile.*") \
                 .withColumn("skill_count", size("skills")) \
                 .withColumn("has_python", array_contains("skills", "Python")) \
                 .withColumn("primary_skill", col("skills")[0])

df_parsed.show(truncate=False)

# Challenge 9: Union and Deduplication
print("\n9. UNION AND DEDUPLICATION")
print("Problem: Combine datasets and remove duplicates")

customers_2023 = [
    (1, "John Doe", "john@email.com", "2023-01-15"),
    (2, "Sarah Smith", "sarah@email.com", "2023-02-20"),
    (3, "Mike Johnson", "mike@email.com", "2023-03-10")
]

customers_2024 = [
    (2, "Sarah Smith", "sarah@email.com", "2024-01-05"),
    (4, "Lisa Brown", "lisa@email.com", "2024-01-20"),
    (5, "David Wilson", "david@email.com", "2024-02-15"),
    (1, "John Doe", "john.doe@email.com", "2024-03-01")
]

df_customers_2023 = spark.createDataFrame(customers_2023, ["id", "name", "email", "date"])
df_customers_2024 = spark.createDataFrame(customers_2024, ["id", "name", "email", "date"])

# Solution
print("Solution:")
df_all_customers = df_customers_2023.union(df_customers_2024)
window_latest = Window.partitionBy("id").orderBy(desc("date"))
df_deduplicated = df_all_customers.withColumn("rn", row_number().over(window_latest)) \
                                .filter(col("rn") == 1) \
                                .drop("rn")

print("Before deduplication:")
df_all_customers.show()
print("After deduplication:")
df_deduplicated.show()

# Challenge 10: Conditional Logic
print("\n10. CONDITIONAL LOGIC")
print("Problem: Categorize customers based on multiple conditions")

transaction_data = [
    (1, "Premium", 150000, 50, 5),
    (2, "Standard", 75000, 25, 2),
    (3, "Premium", 200000, 80, 8),
    (4, "Basic", 30000, 10, 1),
    (5, "Standard", 120000, 40, 3),
    (6, "Premium", 300000, 100, 10),
    (7, "Basic", 45000, 15, 1),
    (8, "Standard", 90000, 30, 4)
]

df_transactions = spark.createDataFrame(transaction_data, 
                                      ["customer_id", "tier", "annual_spend", "transactions", "years"])

# Solution
print("Solution:")
df_categorized = df_transactions.withColumn("customer_segment",
    when((col("annual_spend") >= 200000) & (col("transactions") >= 50), "VIP")
    .when((col("annual_spend") >= 100000) & (col("transactions") >= 30), "Gold")
    .when((col("annual_spend") >= 50000) & (col("transactions") >= 20), "Silver")
    .otherwise("Bronze")
).withColumn("loyalty_status",
    when(col("years") >= 5, "Loyal")
    .when(col("years") >= 2, "Regular")
    .otherwise("New")
).withColumn("avg_transaction_value",
    col("annual_spend") / col("transactions")
)

df_categorized.show()

# Challenge 11: Pivot Operations
print("\n11. PIVOT OPERATIONS")
print("Problem: Transform sales data from long to wide format")

monthly_sales_data = [
    ("Electronics", "2024-01", 50000),
    ("Electronics", "2024-02", 55000),
    ("Electronics", "2024-03", 60000),
    ("Clothing", "2024-01", 30000),
    ("Clothing", "2024-02", 35000),
    ("Clothing", "2024-03", 40000),
    ("Home", "2024-01", 25000),
    ("Home", "2024-02", 28000),
    ("Home", "2024-03", 32000)
]

df_monthly_sales = spark.createDataFrame(monthly_sales_data, ["category", "month", "sales"])

# Solution
print("Solution:")
df_pivoted = df_monthly_sales.groupBy("category") \
                           .pivot("month") \
                           .sum("sales") \
                           .fillna(0)

df_pivoted.show()

# Challenge 12: Array Operations
print("\n12. ARRAY OPERATIONS")
print("Problem: Work with array columns - split, explode, and aggregate")

product_tags_data = [
    (1, "Laptop", "electronics,computers,portable,gaming"),
    (2, "T-Shirt", "clothing,casual,cotton,summer"),
    (3, "Coffee Maker", "appliances,kitchen,brewing,automatic"),
    (4, "Smartphone", "electronics,mobile,communication,smart"),
    (5, "Jeans", "clothing,denim,casual,pants"),
    (6, "Headphones", "electronics,audio,wireless,music"),
    (7, "Sofa", "furniture,living-room,comfort,seating")
]

df_products = spark.createDataFrame(product_tags_data, ["id", "product_name", "tags_str"])

# Solution
print("Solution:")
df_array_ops = df_products.withColumn("tags_array", split(col("tags_str"), ",")) \
                        .withColumn("tag_count", size(col("tags_array"))) \
                        .withColumn("has_electronics", array_contains(col("tags_array"), "electronics"))

df_exploded = df_array_ops.select("id", "product_name", explode("tags_array").alias("tag"))
tag_frequency = df_exploded.groupBy("tag").count().orderBy(desc("count"))

print("Products with array operations:")
df_array_ops.show(truncate=False)
print("Tag frequencies:")
tag_frequency.show()

# Challenge 13: Column Operations and Renaming
print("\n13. COLUMN OPERATIONS AND RENAMING")
print("Problem: Perform complex column operations and restructuring")

financial_data = [
    (1, 100000, 15000, 5000, 2000, 8000),
    (2, 150000, 22000, 7500, 3000, 12000),
    (3, 80000, 12000, 4000, 1500, 6000),
    (4, 200000, 30000, 10000, 4000, 16000),
    (5, 120000, 18000, 6000, 2500, 9500),
    (6, 90000, 13500, 4500, 1800, 7000),
    (7, 180000, 27000, 9000, 3500, 14000)
]

df_financial = spark.createDataFrame(financial_data, 
    ["company_id", "revenue", "marketing", "operations", "rd", "profit"])

# Solution
print("Solution:")
df_financial_analysis = df_financial \
    .withColumnRenamed("rd", "research_development") \
    .withColumn("total_expenses", col("marketing") + col("operations") + col("research_development")) \
    .withColumn("profit_margin", (col("profit") / col("revenue")) * 100) \
    .withColumn("expense_ratio", (col("total_expenses") / col("revenue")) * 100) \
    .withColumn("efficiency_score", 
               when(col("profit_margin") > 15, "High")
               .when(col("profit_margin") > 10, "Medium")
               .otherwise("Low")) \
    .select("company_id", "revenue", "total_expenses", "profit", 
           "profit_margin", "expense_ratio", "efficiency_score")

df_financial_analysis.show()

# Challenge 14: Regular Expressions
print("\n14. REGULAR EXPRESSIONS")
print("Problem: Extract patterns from text data using regex")

text_data = [
    (1, "Contact John at +1-555-123-4567 or email john@company.com"),
    (2, "Call Sarah on (555) 987-6543 for more info at sarah.smith@email.org"),
    (3, "Mike's number is 555.456.7890 and email is mike123@domain.net"),
    (4, "Reach Lisa at 555-321-0987 or lisa_brown@test.co.uk"),
    (5, "David: phone 555 555 5555, email d.wilson@firm.biz"),
    (6, "Emma can be reached at +1 555 111 2222 or emma@startup.io"),
    (7, "James contact: tel:555-333-4444 email:james.miller@corp.com")
]

df_text = spark.createDataFrame(text_data, ["id", "text"])

# Solution
print("Solution:")
df_regex = df_text.withColumn("phone_number", 
                            regexp_extract(col("text"), r"(\+?1?\s*\(?555\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})", 1)) \
                .withColumn("email", 
                          regexp_extract(col("text"), r"([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", 1)) \
                .withColumn("has_phone", col("phone_number") != "") \
                .withColumn("has_email", col("email") != "") \
                .withColumn("contact_methods_count", 
                          col("has_phone").cast("int") + col("has_email").cast("int"))

df_regex.show(truncate=False)

# Challenge 15: Null Value Analysis
print("\n15. NULL VALUE ANALYSIS")
print("Problem: Analyze null patterns and implement imputation strategies")

null_analysis_data = [
    (1, "Alice", 25, 50000, "Engineering", "New York"),
    (2, "Bob", None, 60000, "Marketing", None),
    (3, "Charlie", 30, None, "Engineering", "Chicago"),
    (4, None, 35, 70000, None, "Boston"),
    (5, "Diana", 28, 55000, "HR", "Seattle"),
    (6, "Eve", None, None, "Finance", "Miami"),
    (7, "Frank", 45, 85000, "Engineering", None),
    (8, None, None, None, None, None)
]

df_null_analysis = spark.createDataFrame(null_analysis_data, 
    ["id", "name", "age", "salary", "department", "city"])

# Solution
print("Solution:")
# Null count per column
null_counts = df_null_analysis.select([
    count(when(col(c).isNull(), c)).alias(c + "_nulls") 
    for c in df_null_analysis.columns
])

# Null percentage per column
total_rows = df_null_analysis.count()
null_percentages = df_null_analysis.select([
    (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c + "_null_pct") 
    for c in df_null_analysis.columns
])

# Records with multiple nulls
df_null_count = df_null_analysis.withColumn("null_count", 
    sum([when(col(c).isNull(), 1).otherwise(0) for c in df_null_analysis.columns]))

print("Null counts per column:")
null_counts.show()
print("Null percentages:")
null_percentages.show()
print("Records with null count:")
df_null_count.show()

# Challenge 16: Data Sampling
print("\n16. DATA SAMPLING")
print("Problem: Implement various sampling techniques for large datasets")

sampling_data = []
for i in range(1, 51):  # Create 50 records
    sampling_data.append((i, f"user_{i}", i % 5, i * 100, f"region_{i%3}"))

df_sampling = spark.createDataFrame(sampling_data, ["id", "username", "category", "value", "region"])

# Solution
print("Solution:")
# Simple random sampling
sample_10pct = df_sampling.sample(False, 0.1, seed=42)

# Stratified sampling by category
stratified_sample = df_sampling.sampleBy("category", {0: 0.2, 1: 0.3, 2: 0.4, 3: 0.2, 4: 0.1}, seed=42)

# Systematic sampling (every nth record)
df_with_row_num = df_sampling.withColumn("row_num", monotonically_increasing_id())
systematic_sample = df_with_row_num.filter(col("row_num") % 5 == 0)

print("Original data count:", df_sampling.count())
print("10% random sample count:", sample_10pct.count())
print("Stratified sample count:", stratified_sample.count())
print("Systematic sample (every 5th):", systematic_sample.count())

sample_10pct.show(5)

# Challenge 17: Cross Tabulation
print("\n17. CROSS TABULATION")
print("Problem: Create cross-tabulation analysis for categorical variables")

crosstab_data = [
    (1, "Male", "Engineer", "Yes", "High"),
    (2, "Female", "Manager", "No", "Medium"),
    (3, "Male", "Analyst", "Yes", "Low"),
    (4, "Female", "Engineer", "Yes", "High"),
    (5, "Male", "Manager", "No", "Medium"),
    (6, "Female", "Analyst", "Yes", "High"),
    (7, "Male", "Engineer", "Yes", "Medium"),
    (8, "Female", "Manager", "No", "Low")
]

df_crosstab = spark.createDataFrame(crosstab_data, 
    ["id", "gender", "role", "remote_work", "performance"])

# Solution
print("Solution:")
# Cross-tabulation between gender and role
gender_role_crosstab = df_crosstab.crosstab("gender", "role")

# Cross-tabulation with proportions
total_count = df_crosstab.count()
performance_remote_crosstab = df_crosstab.groupBy("performance") \
    .pivot("remote_work") \
    .count() \
    .fillna(0)

print("Gender vs Role cross-tabulation:")
gender_role_crosstab.show()
print("Performance vs Remote Work cross-tabulation:")
performance_remote_crosstab.show()

# Challenge 18: Quantile Analysis
print("\n18. QUANTILE ANALYSIS")
print("Problem: Calculate quantiles and percentiles for numerical data")

quantile_data = [
    (1, 25000), (2, 35000), (3, 45000), (4, 55000), (5, 65000),
    (6, 75000), (7, 85000), (8, 95000), (9, 105000), (10, 115000),
    (11, 125000), (12, 135000), (13, 145000), (14, 155000), (15, 165000)
]

df_quantile = spark.createDataFrame(quantile_data, ["id", "salary"])

# Solution
print("Solution:")
# Calculate quantiles
quantiles = df_quantile.approxQuantile("salary", [0.25, 0.5, 0.75, 0.9, 0.95], 0.01)

# Create salary bands based on quantiles
df_quantile_bands = df_quantile.withColumn("salary_band",
    when(col("salary") <= quantiles[0], "Q1")
    .when(col("salary") <= quantiles[1], "Q2")
    .when(col("salary") <= quantiles[2], "Q3")
    .otherwise("Q4"))

# Count by bands
band_counts = df_quantile_bands.groupBy("salary_band").count().orderBy("salary_band")

print("Quantiles (25th, 50th, 75th, 90th, 95th):", quantiles)
print("Salary distribution by quantile bands:")
band_counts.show()

# Challenge 19: Time Series Analysis
print("\n19. TIME SERIES ANALYSIS")
print("Problem: Analyze time series data with trends and seasonality")

timeseries_data = [
    ("2024-01-01", 100), ("2024-01-02", 110), ("2024-01-03", 105),
    ("2024-01-04", 120), ("2024-01-05", 115), ("2024-01-06", 130),
    ("2024-01-07", 125), ("2024-01-08", 135), ("2024-01-09", 140),
    ("2024-01-10", 145), ("2024-01-11", 150), ("2024-01-12", 155)
]

df_timeseries = spark.createDataFrame(timeseries_data, ["date", "value"])
df_timeseries = df_timeseries.withColumn("date", to_date(col("date")))

# Solution
print("Solution:")
window_ts = Window.orderBy("date")

df_ts_analysis = df_timeseries.withColumn("previous_value", lag("value", 1).over(window_ts)) \
                            .withColumn("next_value", lead("value", 1).over(window_ts)) \
                            .withColumn("moving_avg_3", 
                                      avg("value").over(window_ts.rowsBetween(-1, 1))) \
                            .withColumn("daily_change", 
                                      col("value") - col("previous_value")) \
                            .withColumn("pct_change", 
                                      ((col("value") - col("previous_value")) / col("previous_value") * 100))

df_ts_analysis.show()

# Challenge 20: Data Profiling
print("\n20. DATA PROFILING")
print("Problem: Generate comprehensive data profile report")

profile_data = [
    (1, "Alice", 25, 50000, "2024-01-15", True),
    (2, "Bob", 35, 75000, "2024-01-16", False),
    (3, "Charlie", 45, 100000, "2024-01-17", True),
    (4, "Diana", 28, 60000, "2024-01-18", True),
    (5, "Eve", 32, 80000, "2024-01-19", False),
    (6, "Frank", 38, 90000, "2024-01-20", True),
    (7, "Grace", 29, 65000, "2024-01-21", True)
]

df_profile = spark.createDataFrame(profile_data, 
    ["id", "name", "age", "salary", "join_date", "active"])

# Solution
print("Solution:")
# Basic statistics
numeric_stats = df_profile.select("age", "salary").describe()

# Data types and null counts
schema_info = [(field.name, str(field.dataType), 
               df_profile.filter(col(field.name).isNull()).count()) 
               for field in df_profile.schema.fields]

df_schema_info = spark.createDataFrame(schema_info, ["column", "data_type", "null_count"])

# Unique value counts
uniqueness_stats = df_profile.agg(
    countDistinct("id").alias("unique_ids"),
    countDistinct("name").alias("unique_names"),
    countDistinct("age").alias("unique_ages"),
    count("*").alias("total_records")
)

print("Numeric statistics:")
numeric_stats.show()
print("Schema information:")
df_schema_info.show()
print("Uniqueness statistics:")
uniqueness_stats.show()

# Challenge 21: Set Operations
print("\n21. SET OPERATIONS")
print("Problem: Perform set operations on DataFrames")

set_data_a = [
    (1, "Alice", "Engineering"),
    (2, "Bob", "Marketing"),
    (3, "Charlie", "Engineering"),
    (4, "Diana", "HR")
]

set_data_b = [
    (3, "Charlie", "Engineering"),
    (4, "Diana", "HR"),
    (5, "Eve", "Finance"),
    (6, "Frank", "Marketing")
]

df_set_a = spark.createDataFrame(set_data_a, ["id", "name", "department"])
df_set_b = spark.createDataFrame(set_data_b, ["id", "name", "department"])

# Solution
print("Solution:")
# Union (all records)
union_df = df_set_a.union(df_set_b)

# Union distinct (remove duplicates)
union_distinct_df = df_set_a.union(df_set_b).distinct()

# Intersection (common records)
intersection_df = df_set_a.intersect(df_set_b)

# Except/Subtract (records in A but not in B)
except_df = df_set_a.except(df_set_b)

print("Dataset A:")
df_set_a.show()
print("Dataset B:")
df_set_b.show()
print("Union (with duplicates):", union_df.count())
print("Union distinct:", union_distinct_df.count())
print("Intersection:")
intersection_df.show()
print("A except B:")
except_df.show()

# Challenge 22: Hierarchical Data
print("\n22. HIERARCHICAL DATA")
print("Problem: Process hierarchical/tree-like data structures")

hierarchy_data = [
    (1, "CEO", None, 1),
    (2, "CTO", 1, 2),
    (3, "CFO", 1, 2),
    (4, "Engineering Manager", 2, 3),
    (5, "Finance Manager", 3, 3),
    (6, "Senior Engineer", 4, 4),
    (7, "Junior Engineer", 4, 4),
    (8, "Accountant", 5, 4)
]

df_hierarchy = spark.createDataFrame(hierarchy_data, ["id", "title", "manager_id", "level"])

# Solution
print("Solution:")
# Find direct reports for each manager
direct_reports = df_hierarchy.alias("emp") \
    .join(df_hierarchy.alias("mgr"), col("emp.manager_id") == col("mgr.id"), "left") \
    .select(col("emp.id"), col("emp.title"), col("mgr.title").alias("manager_title"), col("emp.level"))

# Count subordinates by level
level_counts = df_hierarchy.groupBy("level").count().orderBy("level")

# Find leaf nodes (employees with no subordinates)
leaf_nodes = df_hierarchy.alias("emp") \
    .join(df_hierarchy.alias("sub"), col("emp.id") == col("sub.manager_id"), "left_anti")

print("Employee hierarchy with managers:")
direct_reports.show()
print("Count by level:")
level_counts.show()
print("Leaf nodes (no subordinates):")
leaf_nodes.show()

# Challenge 23: Data Validation Rules
print("\n23. DATA VALIDATION RULES")
print("Problem: Implement business rule validations")

validation_data = [
    (1, "john@email.com", 25, 50000, "2024-01-15", "Engineering"),
    (2, "invalid-email", 17, 30000, "2024-02-30", "Marketing"),  # Multiple violations
    (3, "sarah@test.com", 35, -5000, "2024-01-16", ""),  # Negative salary, empty dept
    (4, "mike@company.com", 150, 80000, "1990-01-01", "Finance"),  # Age too high, old date
    (5, "lisa@email.com", 28, 75000, "2024-01-17", "HR"),  # Valid
    (6, "", 30, 60000, "2024-01-18", "Engineering"),  # Empty email
    (7, "david@test.com", 32, 85000, "2024-01-19", "IT")  # Valid
]

df_validation = spark.createDataFrame(validation_data, 
    ["id", "email", "age", "salary", "hire_date", "department"])

# Solution
print("Solution:")
# Define validation rules
df_validated = df_validation \
    .withColumn("email_valid", col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")) \
    .withColumn("age_valid", (col("age") >= 18) & (col("age") <= 65)) \
    .withColumn("salary_valid", col("salary") > 0) \
    .withColumn("date_valid", 
               (to_date(col("hire_date")).isNotNull()) & 
               (to_date(col("hire_date")) >= to_date(lit("2020-01-01")))) \
    .withColumn("department_valid", 
               (col("department").isNotNull()) & (col("department") != "")) \
    .withColumn("validation_errors", 
               concat_ws(",",
                       when(~col("email_valid"), "INVALID_EMAIL"),
                       when(~col("age_valid"), "INVALID_AGE"),
                       when(~col("salary_valid"), "INVALID_SALARY"),
                       when(~col("date_valid"), "INVALID_DATE"),
                       when(~col("department_valid"), "INVALID_DEPARTMENT"))) \
    .withColumn("is_valid", col("validation_errors") == "")

# Summary of validation results
validation_summary = df_validated.agg(
    count("*").alias("total_records"),
    sum(col("is_valid").cast("int")).alias("valid_records"),
    sum((~col("is_valid")).cast("int")).alias("invalid_records")
)

print("Validation results:")
df_validated.select("id", "email", "age", "salary", "validation_errors", "is_valid").show(truncate=False)
print("Validation summary:")
validation_summary.show()

# Challenge 24: Geographic Data Processing
print("\n24. GEOGRAPHIC DATA PROCESSING")
print("Problem: Process geographic coordinates and calculate distances")

geo_data = [
    (1, "New York", 40.7128, -74.0060),
    (2, "Los Angeles", 34.0522, -118.2437),
    (3, "Chicago", 41.8781, -87.6298),
    (4, "Houston", 29.7604, -95.3698),
    (5, "Phoenix", 33.4484, -112.0740),
    (6, "Philadelphia", 39.9526, -75.1652),
    (7, "San Antonio", 29.4241, -98.4936)
]

df_geo = spark.createDataFrame(geo_data, ["id", "city", "latitude", "longitude"])

# Solution
print("Solution:")
# Calculate distance from New York (using Haversine approximation)
ny_lat, ny_lon = 40.7128, -74.0060

df_geo_analysis = df_geo.withColumn("distance_from_ny_miles",
    expr(f"""
    3959 * acos(
        cos(radians({ny_lat})) * cos(radians(latitude)) *
        cos(radians(longitude) - radians({ny_lon})) +
        sin(radians({ny_lat})) * sin(radians(latitude))
    )
    """)) \
    .withColumn("hemisphere", 
               when(col("latitude") > 0, "Northern").otherwise("Southern")) \
    .withColumn("time_zone_estimate", 
               when(col("longitude") > -75, "Eastern")
               .when(col("longitude") > -105, "Central")
               .when(col("longitude") > -120, "Mountain")
               .otherwise("Pacific"))

df_geo_analysis.show()

# Challenge 25: Text Analytics
print("\n25. TEXT ANALYTICS")
print("Problem: Perform basic text analytics and sentiment analysis")

text_analytics_data = [
    (1, "I love this amazing product! It's fantastic and works perfectly."),
    (2, "This is terrible. Worst purchase ever. Complete waste of money."),
    (3, "Average product. Nothing special but does the job okay."),
    (4, "Excellent quality! Highly recommend to everyone. Great value."),
    (5, "Poor quality. Broke after one day. Very disappointed."),
    (6, "Good product overall. Some minor issues but generally satisfied."),
    (7, "Outstanding service and product. Exceeded my expectations completely.")
]

df_text = spark.createDataFrame(text_analytics_data, ["id", "review"])

# Solution
print("Solution:")
# Basic text analytics
df_text_analysis = df_text.withColumn("word_count", 
                                    size(split(col("review"), " "))) \
                         .withColumn("char_count", length(col("review"))) \
                         .withColumn("avg_word_length", 
                                   col("char_count") / col("word_count")) \
                         .withColumn("has_exclamation", 
                                   col("review").contains("!")) \
                         .withColumn("has_negative_words",
                                   col("review").rlike("(?i)(terrible|worst|poor|disappointed|broke)")) \
                         .withColumn("has_positive_words",
                                   col("review").rlike("(?i)(love|amazing|fantastic|excellent|outstanding)")) \
                         .withColumn("sentiment",
                                   when(col("has_positive_words") & ~col("has_negative_words"), "Positive")
                                   .when(col("has_negative_words") & ~col("has_positive_words"), "Negative")
                                   .otherwise("Neutral"))

df_text_analysis.show(truncate=False)

# Challenge 26: Data Comparison
print("\n26. DATA COMPARISON")
print("Problem: Compare two datasets and identify differences")

current_data = [
    (1, "Alice", "Engineering", 75000),
    (2, "Bob", "Marketing", 65000),
    (3, "Charlie", "Engineering", 80000),
    (4, "Diana", "HR", 70000)
]

updated_data = [
    (1, "Alice", "Engineering", 78000),  # Salary updated
    (2, "Bob", "Sales", 65000),  # Department changed
    (3, "Charlie", "Engineering", 80000),  # No change
    (5, "Eve", "Finance", 72000)  # New record
]

df_current = spark.createDataFrame(current_data, ["id", "name", "department", "salary"])
df_updated = spark.createDataFrame(updated_data, ["id", "name", "department", "salary"])

# Solution
print("Solution:")
# Find new records
new_records = df_updated.join(df_current, "id", "left_anti")

# Find deleted records
deleted_records = df_current.join(df_updated, "id", "left_anti")

# Find modified records
df_comparison = df_current.alias("curr").join(df_updated.alias("upd"), "id", "inner") \
    .select(
        col("curr.id"),
        col("curr.name"),
        col("curr.department").alias("old_department"),
        col("upd.department").alias("new_department"),
        col("curr.salary").alias("old_salary"),
        col("upd.salary").alias("new_salary")
    ) \
    .withColumn("department_changed", 
               col("old_department") != col("new_department")) \
    .withColumn("salary_changed", 
               col("old_salary") != col("new_salary")) \
    .filter(col("department_changed") | col("salary_changed"))

print("New records:")
new_records.show()
print("Deleted records:")
deleted_records.show()
print("Modified records:")
df_comparison.show()

# Challenge 27: Data Masking
print("\n27. DATA MASKING")
print("Problem: Implement data masking for sensitive information")

sensitive_data = [
    (1, "John Doe", "123-45-6789", "john.doe@email.com", "555-123-4567"),
    (2, "Sarah Smith", "987-65-4321", "sarah.smith@company.com", "555-987-6543"),
    (3, "Mike Johnson", "456-78-9012", "mike.j@domain.org", "555-456-7890"),
    (4, "Lisa Brown", "789-01-2345", "lisa.brown@test.net", "555-321-0987"),
    (5, "David Wilson", "345-67-8901", "d.wilson@firm.biz", "555-555-5555")
]

df_sensitive = spark.createDataFrame(sensitive_data, 
    ["id", "name", "ssn", "email", "phone"])

# Solution
print("Solution:")
df_masked = df_sensitive \
    .withColumn("name_masked", 
               concat(substring(col("name"), 1, 1), lit("*** "), 
                     substring(split(col("name"), " ")[1], 1, 1), lit("***"))) \
    .withColumn("ssn_masked", 
               concat(lit("XXX-XX-"), substring(col("ssn"), -4, 4))) \
    .withColumn("email_masked", 
               concat(substring(col("email"), 1, 2), lit("***@"), 
                     regexp_extract(col("email"), "@(.+)", 1))) \
    .withColumn("phone_masked", 
               regexp_replace(col("phone"), r"(\d{3})-(\d{3})-(\d{4})", "XXX-XXX-$3")) \
    .select("id", "name_masked", "ssn_masked", "email_masked", "phone_masked")

print("Original data:")
df_sensitive.show()
print("Masked data:")
df_masked.show()

# Challenge 28: Recursive Operations
print("\n28. RECURSIVE OPERATIONS")
print("Problem: Calculate cumulative and recursive values")

recursive_data = [
    (1, "2024-01-01", 100),
    (2, "2024-01-02", 150),
    (3, "2024-01-03", 200),
    (4, "2024-01-04", 120),
    (5, "2024-01-05", 180),
    (6, "2024-01-06", 160),
    (7, "2024-01-07", 220)
]

df_recursive = spark.createDataFrame(recursive_data, ["id", "date", "amount"])
df_recursive = df_recursive.withColumn("date", to_date(col("date")))

# Solution
print("Solution:")
window_cumulative = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_recursive_calc = df_recursive \
    .withColumn("cumulative_sum", sum("amount").over(window_cumulative)) \
    .withColumn("running_avg", avg("amount").over(window_cumulative)) \
    .withColumn("growth_rate", 
               (col("amount") - lag("amount", 1).over(Window.orderBy("date"))) / 
               lag("amount", 1).over(Window.orderBy("date")) * 100) \
    .withColumn("cumulative_growth", 
               (col("cumulative_sum") - first("amount").over(window_cumulative)) / 
               first("amount").over(window_cumulative) * 100)

df_recursive_calc.show()

# Challenge 29: Data Lineage Tracking
print("\n29. DATA LINEAGE TRACKING")
print("Problem: Track data transformations and lineage")

lineage_data = [
    (1, "raw_data", "customer_table", "2024-01-15 10:00:00", "ingestion"),
    (2, "clean_data", "customer_clean", "2024-01-15 10:30:00", "cleaning"),
    (3, "enriched_data", "customer_enriched", "2024-01-15 11:00:00", "enrichment"),
    (4, "aggregated_data", "customer_summary", "2024-01-15 11:30:00", "aggregation"),
    (5, "final_data", "customer_mart", "2024-01-15 12:00:00", "publication")
]

df_lineage = spark.createDataFrame(lineage_data, 
    ["step_id", "dataset_name", "table_name", "timestamp", "operation"])
df_lineage = df_lineage.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
df_lineage_analysis = df_lineage \
    .withColumn("processing_order", row_number().over(Window.orderBy("timestamp"))) \
    .withColumn("time_taken_minutes", 
               (unix_timestamp(lead("timestamp").over(Window.orderBy("timestamp"))) - 
                unix_timestamp("timestamp")) / 60) \
    .withColumn("is_final_step", 
               col("step_id") == max("step_id").over(Window.partitionBy(lit(1))))

# Lineage chain
lineage_chain = df_lineage_analysis.select("processing_order", "dataset_name", "operation", 
                                          "time_taken_minutes").orderBy("processing_order")

print("Data lineage chain:")
lineage_chain.show()

# Challenge 30: Performance Metrics
print("\n30. PERFORMANCE METRICS")
print("Problem: Calculate system performance metrics")

performance_data = [
    ("2024-01-15 10:00:00", "system_A", 95.5, 150, 2.3),
    ("2024-01-15 10:00:00", "system_B", 88.2, 200, 1.8),
    ("2024-01-15 10:15:00", "system_A", 92.1, 180, 2.8),
    ("2024-01-15 10:15:00", "system_B", 91.5, 175, 2.1),
    ("2024-01-15 10:30:00", "system_A", 97.8, 120, 1.9),
    ("2024-01-15 10:30:00", "system_B", 89.3, 210, 2.5),
    ("2024-01-15 10:45:00", "system_A", 94.2, 160, 2.2)
]

df_performance = spark.createDataFrame(performance_data, 
    ["timestamp", "system", "cpu_usage", "memory_mb", "response_time"])
df_performance = df_performance.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
system_window = Window.partitionBy("system").orderBy("timestamp")

df_perf_metrics = df_performance \
    .withColumn("cpu_trend", 
               col("cpu_usage") - lag("cpu_usage").over(system_window)) \
    .withColumn("memory_trend", 
               col("memory_mb") - lag("memory_mb").over(system_window)) \
    .withColumn("response_trend", 
               col("response_time") - lag("response_time").over(system_window)) \
    .withColumn("performance_score", 
               (100 - col("cpu_usage")) * 0.4 + 
               (1000 - col("memory_mb")) / 10 * 0.3 + 
               (5 - col("response_time")) * 20 * 0.3)

# System comparison
system_comparison = df_perf_metrics.groupBy("system") \
    .agg(avg("cpu_usage").alias("avg_cpu"),
         avg("memory_mb").alias("avg_memory"),
         avg("response_time").alias("avg_response"),
         avg("performance_score").alias("avg_performance"))

print("Performance metrics with trends:")
df_perf_metrics.show()
print("System comparison:")
system_comparison.show()

# =============================================================================
# INTERMEDIATE LEVEL CHALLENGES (31-65)
# =============================================================================

print("\n" + "="*60)
print("INTERMEDIATE LEVEL CHALLENGES (31-65)")
print("="*60)

# Challenge 31: Complex Joins
print("\n31. COMPLEX JOINS")
print("Problem: Perform multiple joins with different conditions")

orders_data = [
    (1, 101, "2024-01-15", 250.00, "completed"),
    (2, 102, "2024-01-16", 150.00, "pending"),
    (3, 103, "2024-01-17", 300.00, "completed"),
    (4, 101, "2024-01-18", 200.00, "cancelled"),
    (5, 104, "2024-01-19", 400.00, "completed"),
    (6, 105, "2024-01-20", 175.00, "pending")
]

customers_data = [
    (101, "Alice Johnson", "Premium", "alice@email.com"),
    (102, "Bob Smith", "Standard", "bob@email.com"),
    (103, "Charlie Brown", "Premium", "charlie@email.com"),
    (104, "Diana Prince", "Gold", "diana@email.com"),
    (106, "Eve Wilson", "Standard", "eve@email.com")
]

products_data = [
    (1, "Laptop", "Electronics", 1200.00),
    (2, "Smartphone", "Electronics", 800.00),
    (3, "Tablet", "Electronics", 400.00)
]

df_orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_date", "amount", "status"])
df_customers = spark.createDataFrame(customers_data, ["customer_id", "name", "tier", "email"])
df_products = spark.createDataFrame(products_data, ["product_id", "product_name", "category", "price"])

# Solution
print("Solution:")
completed_orders = df_orders.filter(col("status") == "completed") \
                          .join(df_customers, "customer_id", "inner")

all_customers_orders = df_customers.join(df_orders, "customer_id", "left")
customers_no_orders = df_customers.join(df_orders, "customer_id", "left_anti")

print("Completed orders with customer details:")
completed_orders.show()
print("Customers with no orders:")
customers_no_orders.show()

# Challenge 32: Window Functions - Advanced
print("\n32. WINDOW FUNCTIONS - ADVANCED")
print("Problem: Calculate running totals, moving averages, and lag/lead operations")

sales_time_data = [
    ("2024-01-01", "Electronics", 5000),
    ("2024-01-02", "Electronics", 5500),
    ("2024-01-03", "Electronics", 4800),
    ("2024-01-04", "Electronics", 6200),
    ("2024-01-05", "Electronics", 5800),
    ("2024-01-01", "Clothing", 3000),
    ("2024-01-02", "Clothing", 3200),
    ("2024-01-03", "Clothing", 2800),
    ("2024-01-04", "Clothing", 3500),
    ("2024-01-05", "Clothing", 3100)
]

df_time_sales = spark.createDataFrame(sales_time_data, ["date", "category", "sales"])
df_time_sales = df_time_sales.withColumn("date", to_date(col("date")))

# Solution
print("Solution:")
window_category_date = Window.partitionBy("category").orderBy("date")
window_category_date_range = Window.partitionBy("category").orderBy("date") \
                                 .rowsBetween(-2, 0)

df_advanced_window = df_time_sales.withColumn("running_total", 
                                            sum("sales").over(window_category_date)) \
                                .withColumn("previous_day_sales", 
                                          lag("sales", 1).over(window_category_date)) \
                                .withColumn("next_day_sales", 
                                          lead("sales", 1).over(window_category_date)) \
                                .withColumn("3day_moving_avg", 
                                          avg("sales").over(window_category_date_range)) \
                                .withColumn("sales_growth", 
                                          ((col("sales") - col("previous_day_sales")) / 
                                           col("previous_day_sales") * 100))

df_advanced_window.show()

# Challenge 33: Data Skew Handling
print("\n33. DATA SKEW HANDLING")
print("Problem: Handle data skew in joins and aggregations")

skewed_data = [
    (1, "action_1", "2024-01-01"),
    (1, "action_2", "2024-01-02"),
    (1, "action_3", "2024-01-03"),
    (1, "action_4", "2024-01-04"),
    (2, "action_5", "2024-01-05"),
    (3, "action_6", "2024-01-06"),
    (4, "action_7", "2024-01-07"),
    (5, "action_8", "2024-01-08")
]

df_skewed = spark.createDataFrame(skewed_data, ["user_id", "action", "date"])

# Solution
print("Solution:")
df_salted = df_skewed.withColumn("salt", (rand() * 10).cast(IntegerType())) \
                   .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))

user_profiles = [(1, "Premium"), (2, "Standard"), (3, "Basic")]
df_user_profiles = spark.createDataFrame(user_profiles, ["user_id", "tier"])

from pyspark.sql.functions import broadcast
df_joined = df_skewed.join(broadcast(df_user_profiles), "user_id", "left")

print("Salted data for skew handling:")
df_salted.show()
print("Broadcast join result:")
df_joined.show()

# Challenge 34: UDF Creation and Usage
print("\n34. UDF CREATION AND USAGE")
print("Problem: Create custom functions for complex business logic")

from pyspark.sql.functions import udf

udf_data = [
    (1, "john.doe@company.com", "555-123-4567", "Software Engineer"),
    (2, "sarah.smith@startup.io", "555-987-6543", "Data Scientist"),
    (3, "mike.johnson@corp.net", "555-456-7890", "Product Manager"),
    (4, "lisa.brown@tech.org", "555-321-0987", "UX Designer"),
    (5, "david.wilson@firm.biz", "invalid-phone", "Business Analyst"),
    (6, "emma.davis@company.com", "555-555-5555", "DevOps Engineer"),
    (7, "james.miller@startup.co", "555-111-2222", "Marketing Manager")
]

df_udf_data = spark.createDataFrame(udf_data, ["id", "email", "phone", "role"])

# Solution
print("Solution:")
def extract_domain(email):
    if email and "@" in email:
        return email.split("@")[1]
    return "unknown"

def validate_phone(phone):
    import re
    if phone and re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
        return True
    return False

def categorize_role(role):
    if role:
        role_lower = role.lower()
        if any(keyword in role_lower for keyword in ["engineer", "developer", "devops"]):
            return "Technical"
        elif any(keyword in role_lower for keyword in ["manager", "analyst", "scientist"]):
            return "Management"
        elif any(keyword in role_lower for keyword in ["designer", "marketing"]):
            return "Creative"
    return "Other"

extract_domain_udf = udf(extract_domain, StringType())
validate_phone_udf = udf(validate_phone, BooleanType())
categorize_role_udf = udf(categorize_role, StringType())

df_with_udfs = df_udf_data.withColumn("domain", extract_domain_udf(col("email"))) \
                        .withColumn("valid_phone", validate_phone_udf(col("phone"))) \
                        .withColumn("role_category", categorize_role_udf(col("role")))

df_with_udfs.show(truncate=False)

# Challenge 35: Performance Optimization
print("\n35. PERFORMANCE OPTIMIZATION")
print("Problem: Optimize Spark job performance using various techniques")

perf_data = []
for i in range(1, 21):
    perf_data.append((i, f"product_{i%10}", i%5, i*1.5, f"2024-{i%12+1:02d}-01"))

df_perf = spark.createDataFrame(perf_data, ["id", "product", "category", "price", "date"])

# Solution
print("Solution:")
df_perf.cache()
df_optimized = df_perf.coalesce(2)

expensive_products = df_optimized.filter(col("price") > 10) \
                                .select("id", "product", "price")

summary_stats = df_optimized.groupBy("category") \
                          .agg(count("*").alias("count"),
                               avg("price").alias("avg_price"))

print("Optimization applied - expensive products:")
expensive_products.show()
print("Summary statistics:")
summary_stats.show()

# Challenge 36: Streaming Data Processing
print("\n36. STREAMING DATA PROCESSING")
print("Problem: Process streaming data with windowing and watermarking")

streaming_data = [
    ("user1", "click", "2024-01-15 10:00:00", "page1"),
    ("user2", "view", "2024-01-15 10:01:00", "page2"),
    ("user1", "click", "2024-01-15 10:02:00", "page3"),
    ("user3", "purchase", "2024-01-15 10:03:00", "page1"),
    ("user2", "click", "2024-01-15 10:04:00", "page1"),
    ("user1", "view", "2024-01-15 10:05:00", "page2"),
    ("user4", "click", "2024-01-15 10:06:00", "page3"),
    ("user3", "view", "2024-01-15 10:07:00", "page2")
]

df_streaming = spark.createDataFrame(streaming_data, ["user_id", "event_type", "timestamp", "page"])
df_streaming = df_streaming.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
windowed_events = df_streaming.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("event_type")
).agg(
    count("*").alias("event_count"),
    countDistinct("user_id").alias("unique_users")
)

user_sessions = df_streaming.groupBy("user_id") \
                          .agg(count("*").alias("total_events"),
                               min("timestamp").alias("session_start"),
                               max("timestamp").alias("session_end"))

print("Windowed events:")
windowed_events.show(truncate=False)
print("User sessions:")
user_sessions.show()

# Challenge 37: Data Quality Checks
print("\n37. DATA QUALITY CHECKS")
print("Problem: Implement comprehensive data quality validations")

quality_data = [
    (1, "John Doe", 25, "john@email.com", 50000, "2024-01-15"),
    (2, "Sarah Smith", -5, "invalid-email", 75000, "2024-02-30"),
    (3, None, 30, "mike@email.com", None, "2024-03-15"),
    (4, "Lisa Brown", 150, "lisa@email.com", -10000, "invalid-date"),
    (5, "David Wilson", 35, "david@email.com", 80000, "2024-04-15"),
    (6, "", 28, "", 60000, "2024-05-15"),
    (7, "Emma Davis", 32, "emma@email.com", 70000, "2024-06-15")
]

df_quality = spark.createDataFrame(quality_data, ["id", "name", "age", "email", "salary", "hire_date"])

# Solution
print("Solution:")
df_quality_checked = df_quality.withColumn("name_valid", 
    when((col("name").isNull()) | (col("name") == ""), False).otherwise(True)) \
.withColumn("age_valid", 
    when((col("age").isNull()) | (col("age") < 0) | (col("age") > 120), False).otherwise(True)) \
.withColumn("email_valid", 
    when(col("email").rlike("^[^@]+@[^@]+\\.[^@]+$"), True).otherwise(False)) \
.withColumn("salary_valid", 
    when((col("salary").isNull()) | (col("salary") <= 0), False).otherwise(True)) \
.withColumn("hire_date_valid", 
    when(to_date(col("hire_date")).isNull(), False).otherwise(True)) \
.withColumn("record_quality_score", 
    (col("name_valid").cast("int") + 
     col("age_valid").cast("int") + 
     col("email_valid").cast("int") + 
     col("salary_valid").cast("int") + 
     col("hire_date_valid").cast("int")) / 5.0)

quality_summary = df_quality_checked.agg(
    avg("record_quality_score").alias("avg_quality_score"),
    sum(when(col("record_quality_score") == 1.0, 1).otherwise(0)).alias("perfect_records"),
    count("*").alias("total_records")
)

print("Data quality assessment:")
df_quality_checked.show()
print("Quality summary:")
quality_summary.show()

# [Continue with remaining challenges 38-65...]

# Challenge 38: Dynamic Schema Evolution
print("\n38. DYNAMIC SCHEMA EVOLUTION")
print("Problem: Handle schema changes and evolution dynamically")

schema_v1_data = [
    (1, "Alice", 25, "Engineering"),
    (2, "Bob", 30, "Marketing"),
    (3, "Charlie", 35, "Finance")
]

schema_v2_data = [
    (4, "Diana", 28, "HR", 75000, "2024-01-15"),  # Added salary and join_date
    (5, "Eve", 32, "Engineering", 85000, "2024-01-16"),
    (6, "Frank", 40, "Marketing", 70000, "2024-01-17")
]

df_v1 = spark.createDataFrame(schema_v1_data, ["id", "name", "age", "department"])
df_v2 = spark.createDataFrame(schema_v2_data, ["id", "name", "age", "department", "salary", "join_date"])

# Solution
print("Solution:")
# Add missing columns to v1 with default values
df_v1_evolved = df_v1.withColumn("salary", lit(None).cast(IntegerType())) \
                   .withColumn("join_date", lit(None).cast(StringType()))

# Union the evolved schemas
df_combined = df_v1_evolved.union(df_v2)

# Schema comparison
print("Schema V1 columns:", df_v1.columns)
print("Schema V2 columns:", df_v2.columns)
print("Combined schema:")
df_combined.show()

# Challenge 39: Incremental Data Processing
print("\n39. INCREMENTAL DATA PROCESSING")
print("Problem: Process only new/changed records efficiently")

existing_data = [
    (1, "Alice", "Engineering", "2024-01-01", "2024-01-01"),
    (2, "Bob", "Marketing", "2024-01-01", "2024-01-01"),
    (3, "Charlie", "Finance", "2024-01-01", "2024-01-01")
]

new_data = [
    (2, "Bob", "Sales", "2024-01-01", "2024-01-15"),  # Department changed
    (3, "Charlie", "Finance", "2024-01-01", "2024-01-01"),  # No change
    (4, "Diana", "HR", "2024-01-15", "2024-01-15")  # New record
]

df_existing = spark.createDataFrame(existing_data, ["id", "name", "department", "created", "updated"])
df_new = spark.createDataFrame(new_data, ["id", "name", "department", "created", "updated"])

# Solution
print("Solution:")
# Identify truly new records
df_new_records = df_new.join(df_existing, "id", "left_anti")

# Identify changed records
df_changed = df_new.alias("new").join(df_existing.alias("old"), "id", "inner") \
    .filter((col("new.department") != col("old.department")) | 
            (col("new.updated") != col("old.updated"))) \
    .select(col("new.*"))

# Combine for incremental processing
df_incremental = df_new_records.union(df_changed)

print("New records:")
df_new_records.show()
print("Changed records:")
df_changed.show()
print("Incremental batch:")
df_incremental.show()

# Challenge 40: Multi-level Aggregations
print("\n40. MULTI-LEVEL AGGREGATIONS")
print("Problem: Perform aggregations at multiple hierarchical levels")

multilevel_data = [
    ("North", "NY", "NYC", "Electronics", 1000),
    ("North", "NY", "NYC", "Clothing", 800),
    ("North", "NY", "Buffalo", "Electronics", 600),
    ("South", "FL", "Miami", "Electronics", 1200),
    ("South", "FL", "Miami", "Clothing", 900),
    ("South", "TX", "Houston", "Electronics", 1100),
    ("West", "CA", "LA", "Electronics", 1500),
    ("West", "CA", "SF", "Clothing", 700)
]

df_multilevel = spark.createDataFrame(multilevel_data, 
    ["region", "state", "city", "category", "sales"])

# Solution
print("Solution:")
# Region level
region_agg = df_multilevel.groupBy("region") \
    .agg(sum("sales").alias("total_sales")) \
    .withColumn("level", lit("region"))

# State level
state_agg = df_multilevel.groupBy("region", "state") \
    .agg(sum("sales").alias("total_sales")) \
    .withColumn("level", lit("state"))

# City level
city_agg = df_multilevel.groupBy("region", "state", "city") \
    .agg(sum("sales").alias("total_sales")) \
    .withColumn("level", lit("city"))

# Rollup aggregation
rollup_agg = df_multilevel.rollup("region", "state", "city") \
    .agg(sum("sales").alias("total_sales"))

print("Region level aggregation:")
region_agg.show()
print("Rollup aggregation:")
rollup_agg.show()

# Challenge 41: Event Sequence Analysis
print("\n41. EVENT SEQUENCE ANALYSIS")
print("Problem: Analyze user event sequences and patterns")

event_sequence_data = [
    ("user1", "login", "2024-01-15 10:00:00", 1),
    ("user1", "browse", "2024-01-15 10:05:00", 2),
    ("user1", "add_to_cart", "2024-01-15 10:10:00", 3),
    ("user1", "checkout", "2024-01-15 10:15:00", 4),
    ("user2", "login", "2024-01-15 10:02:00", 1),
    ("user2", "browse", "2024-01-15 10:07:00", 2),
    ("user2", "logout", "2024-01-15 10:12:00", 3),
    ("user3", "login", "2024-01-15 10:01:00", 1),
    ("user3", "browse", "2024-01-15 10:03:00", 2),
    ("user3", "add_to_cart", "2024-01-15 10:08:00", 3),
    ("user3", "remove_from_cart", "2024-01-15 10:09:00", 4),
    ("user3", "logout", "2024-01-15 10:10:00", 5)
]

df_events = spark.createDataFrame(event_sequence_data, 
    ["user_id", "event_type", "timestamp", "sequence_number"])
df_events = df_events.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
user_window = Window.partitionBy("user_id").orderBy("sequence_number")

df_sequence_analysis = df_events \
    .withColumn("next_event", lead("event_type").over(user_window)) \
    .withColumn("time_to_next", 
               (unix_timestamp(lead("timestamp").over(user_window)) - 
                unix_timestamp("timestamp")) / 60) \
    .withColumn("session_duration", 
               (unix_timestamp(last("timestamp").over(Window.partitionBy("user_id"))) - 
                unix_timestamp(first("timestamp").over(Window.partitionBy("user_id")))) / 60)

# Common event transitions
event_transitions = df_sequence_analysis.filter(col("next_event").isNotNull()) \
    .groupBy("event_type", "next_event") \
    .count() \
    .orderBy(desc("count"))

print("Event sequence analysis:")
df_sequence_analysis.show()
print("Common event transitions:")
event_transitions.show()

# Challenge 42: Time-based Partitioning
print("\n42. TIME-BASED PARTITIONING")
print("Problem: Partition data by time periods for efficient querying")

time_partition_data = [
    ("2024-01-15 08:30:00", "morning", "user1", "login"),
    ("2024-01-15 14:15:00", "afternoon", "user2", "purchase"),
    ("2024-01-15 20:45:00", "evening", "user3", "browse"),
    ("2024-01-16 09:00:00", "morning", "user1", "browse"),
    ("2024-01-16 13:30:00", "afternoon", "user2", "logout"),
    ("2024-01-16 19:20:00", "evening", "user3", "purchase"),
    ("2024-01-17 07:45:00", "morning", "user1", "login"),
    ("2024-01-17 16:10:00", "afternoon", "user2", "browse")
]

df_time_partition = spark.createDataFrame(time_partition_data, 
    ["timestamp", "time_of_day", "user_id", "action"])
df_time_partition = df_time_partition.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
df_partitioned = df_time_partition \
    .withColumn("year", year("timestamp")) \
    .withColumn("month", month("timestamp")) \
    .withColumn("day", dayofmonth("timestamp")) \
    .withColumn("hour", hour("timestamp")) \
    .withColumn("partition_key", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")))

# Activity by time partitions
partition_summary = df_partitioned.groupBy("partition_key", "time_of_day") \
    .agg(count("*").alias("activity_count"),
         countDistinct("user_id").alias("unique_users"))

print("Partitioned data:")
df_partitioned.show()
print("Activity by time partitions:")
partition_summary.show()

# Challenge 43: Data Deduplication Strategies
print("\n43. DATA DEDUPLICATION STRATEGIES")
print("Problem: Implement various deduplication strategies")

duplicate_data = [
    (1, "Alice", "alice@email.com", "2024-01-15", "insert"),
    (1, "Alice Johnson", "alice@email.com", "2024-01-16", "update"),  # Name updated
    (2, "Bob", "bob@email.com", "2024-01-15", "insert"),
    (2, "Bob", "bob@email.com", "2024-01-15", "insert"),  # Exact duplicate
    (3, "Charlie", "charlie@email.com", "2024-01-17", "insert"),
    (1, "Alice Johnson", "alice.johnson@email.com", "2024-01-18", "update"),  # Email updated
    (4, "Diana", "diana@email.com", "2024-01-19", "insert")
]

df_duplicates = spark.createDataFrame(duplicate_data, 
    ["id", "name", "email", "date", "operation"])

# Solution
print("Solution:")
# Strategy 1: Remove exact duplicates
df_exact_dedup = df_duplicates.distinct()

# Strategy 2: Keep latest record per ID
window_latest = Window.partitionBy("id").orderBy(desc("date"))
df_latest_dedup = df_duplicates.withColumn("rn", row_number().over(window_latest)) \
    .filter(col("rn") == 1).drop("rn")

# Strategy 3: Custom deduplication logic
df_custom_dedup = df_duplicates \
    .withColumn("priority", 
               when(col("operation") == "update", 2)
               .when(col("operation") == "insert", 1)
               .otherwise(0)) \
    .withColumn("rn", row_number().over(Window.partitionBy("id").orderBy(desc("priority"), desc("date")))) \
    .filter(col("rn") == 1).drop("rn", "priority")

print("Original data:", df_duplicates.count())
print("After exact dedup:", df_exact_dedup.count())
print("After latest per ID:", df_latest_dedup.count())
print("After custom dedup:", df_custom_dedup.count())

# Challenge 44: Complex Data Types
print("\n44. COMPLEX DATA TYPES")
print("Problem: Work with complex nested data structures")

complex_data = [
    (1, {"first": "John", "last": "Doe"}, ["Python", "Spark", "SQL"], 
     {"address": {"street": "123 Main St", "city": "NYC"}, "phone": "555-1234"}),
    (2, {"first": "Sarah", "last": "Smith"}, ["Java", "Kafka"], 
     {"address": {"street": "456 Oak Ave", "city": "LA"}, "phone": "555-5678"}),
    (3, {"first": "Mike", "last": "Johnson"}, ["Scala", "Hadoop"], 
     {"address": {"street": "789 Pine Rd", "city": "Chicago"}, "phone": "555-9012"})
]

# Convert to JSON for DataFrame creation
complex_json_data = [(row[0], json.dumps(row[1]), json.dumps(row[2]), json.dumps(row[3])) 
                     for row in complex_data]
df_complex = spark.createDataFrame(complex_json_data, ["id", "name_json", "skills_json", "contact_json"])

# Solution
print("Solution:")
# Define schemas
name_schema = StructType([
    StructField("first", StringType(), True),
    StructField("last", StringType(), True)
])

contact_schema = StructType([
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("phone", StringType(), True)
])

df_complex_parsed = df_complex \
    .withColumn("name", from_json(col("name_json"), name_schema)) \
    .withColumn("skills", from_json(col("skills_json"), ArrayType(StringType()))) \
    .withColumn("contact", from_json(col("contact_json"), contact_schema)) \
    .select("id", "name.*", "skills", "contact.address.*", "contact.phone") \
    .withColumn("full_name", concat(col("first"), lit(" "), col("last"))) \
    .withColumn("skill_count", size("skills")) \
    .withColumn("primary_skill", col("skills")[0])

df_complex_parsed.show(truncate=False)

# Challenge 45: Memory Management
print("\n45. MEMORY MANAGEMENT")
print("Problem: Optimize memory usage and prevent OOM errors")

memory_data = []
for i in range(1, 101):  # Simulate larger dataset
    memory_data.append((i, f"data_{i}", i * 100, f"category_{i%5}"))

# Use sample for demonstration
memory_sample = memory_data[:20]
df_memory = spark.createDataFrame(memory_sample, ["id", "data", "value", "category"])

# Solution
print("Solution:")
# Memory optimization techniques
df_memory.cache()  # Cache frequently accessed data
df_memory_optimized = df_memory.coalesce(1)  # Reduce partitions

# Use efficient aggregations
memory_stats = df_memory_optimized.groupBy("category") \
    .agg(count("*").alias("count"),
         sum("value").alias("total_value")) \
    .persist()  # Persist intermediate results

# Clear cache when done
df_memory.unpersist()

print("Memory optimized aggregations:")
memory_stats.show()

# Continue with remaining intermediate challenges (46-65)...

# Challenge 46: Data Bucketing
print("\n46. DATA BUCKETING")
print("Problem: Implement data bucketing for join optimization")

bucket_data_1 = [(i, f"user_{i}", i % 10) for i in range(1, 21)]
bucket_data_2 = [(i, f"user_{i}", i * 100) for i in range(1, 16)]

df_bucket_1 = spark.createDataFrame(bucket_data_1, ["id", "username", "category"])
df_bucket_2 = spark.createDataFrame(bucket_data_2, ["id", "username", "score"])

# Solution
print("Solution:")
# Simulate bucketing effect with hash partitioning
df_bucket_1_partitioned = df_bucket_1.repartition(4, col("id"))
df_bucket_2_partitioned = df_bucket_2.repartition(4, col("id"))

# Join will be more efficient with co-located data
df_joined = df_bucket_1_partitioned.join(df_bucket_2_partitioned, "id", "inner")

print("Bucketed join result:")
df_joined.show()

# Challenge 47: Custom Partitioning
print("\n47. CUSTOM PARTITIONING")
print("Problem: Implement custom partitioning strategies")

partition_data = []
for i in range(1, 31):
    region = "North" if i % 3 == 0 else "South" if i % 3 == 1 else "West"
    partition_data.append((i, f"customer_{i}", region, i * 50))

df_partition = spark.createDataFrame(partition_data, ["id", "customer", "region", "sales"])

# Solution
print("Solution:")
# Partition by region for efficient region-based queries
df_partitioned_by_region = df_partition.repartition(col("region"))

# Show partition distribution
region_counts = df_partitioned_by_region.groupBy("region").count()

print("Data partitioned by region:")
region_counts.show()

# Challenge 48: Dynamic Filtering
print("\n48. DYNAMIC FILTERING")
print("Problem: Apply filters dynamically based on conditions")

dynamic_filter_data = [
    (1, "Alice", "Engineering", 75000, "Premium"),
    (2, "Bob", "Marketing", 65000, "Standard"),
    (3, "Charlie", "Engineering", 85000, "Premium"),
    (4, "Diana", "HR", 70000, "Standard"),
    (5, "Eve", "Finance", 80000, "Premium"),
    (6, "Frank", "Marketing", 60000, "Basic"),
    (7, "Grace", "Engineering", 90000, "Premium")
]

df_dynamic = spark.createDataFrame(dynamic_filter_data, 
    ["id", "name", "department", "salary", "tier"])

# Solution
print("Solution:")
# Dynamic filter conditions
filter_conditions = {
    "high_salary": col("salary") > 70000,
    "engineering": col("department") == "Engineering",
    "premium": col("tier") == "Premium"
}

# Apply multiple dynamic filters
active_filters = ["high_salary", "engineering"]  # Configurable
combined_filter = None

for filter_name in active_filters:
    if filter_name in filter_conditions:
        if combined_filter is None:
            combined_filter = filter_conditions[filter_name]
        else:
            combined_filter = combined_filter & filter_conditions[filter_name]

df_filtered = df_dynamic.filter(combined_filter) if combined_filter is not None else df_dynamic

print("Dynamically filtered data:")
df_filtered.show()

# Challenge 49: Data Catalog Integration
print("\n49. DATA CATALOG INTEGRATION")
print("Problem: Create metadata for data catalog")

catalog_data = [
    ("customers", "customer_id,name,email,signup_date", 1000000, "2024-01-15"),
    ("orders", "order_id,customer_id,amount,order_date", 5000000, "2024-01-15"),
    ("products", "product_id,name,category,price", 50000, "2024-01-15"),
    ("reviews", "review_id,product_id,customer_id,rating,comment", 2000000, "2024-01-15")
]

df_catalog = spark.createDataFrame(catalog_data, 
    ["table_name", "columns", "row_count", "last_updated"])

# Solution
print("Solution:")
df_catalog_enriched = df_catalog \
    .withColumn("column_list", split(col("columns"), ",")) \
    .withColumn("column_count", size(split(col("columns"), ","))) \
    .withColumn("data_freshness_days", 
               datediff(current_date(), to_date(col("last_updated")))) \
    .withColumn("table_size_category",
               when(col("row_count") > 1000000, "Large")
               .when(col("row_count") > 100000, "Medium")
               .otherwise("Small"))

print("Data catalog metadata:")
df_catalog_enriched.show(truncate=False)

# Challenge 50: CDC Implementation
print("\n50. CDC (CHANGE DATA CAPTURE) IMPLEMENTATION")
print("Problem: Implement change data capture pattern")

# Current state
current_state_data = [
    (1, "Alice", "Engineering", 75000, "2024-01-15"),
    (2, "Bob", "Marketing", 65000, "2024-01-15"),
    (3, "Charlie", "Finance", 70000, "2024-01-15")
]

# Change log
change_log_data = [
    (1, "Alice", "Engineering", 78000, "2024-01-16", "UPDATE"),  # Salary change
    (2, "Bob", "Sales", 65000, "2024-01-16", "UPDATE"),  # Department change
    (4, "Diana", "HR", 72000, "2024-01-16", "INSERT"),  # New record
    (3, None, None, None, "2024-01-16", "DELETE")  # Deleted record
]

df_current = spark.createDataFrame(current_state_data, 
    ["id", "name", "department", "salary", "date"])
df_changes = spark.createDataFrame(change_log_data, 
    ["id", "name", "department", "salary", "date", "operation"])

df_inserts = df_changes.filter(col("operation") == "INSERT") \
    .select("id", "name", "department", "salary", "date")

df_updates = df_changes.filter(col("operation") == "UPDATE") \
    .select("id", "name", "department", "salary", "date")

df_deletes = df_changes.filter(col("operation") == "DELETE").select("id")

# Apply changes to current state
# Remove deleted records
df_after_deletes = df_current.join(df_deletes, "id", "left_anti")

# Update existing records
df_after_updates = df_after_deletes.alias("curr") \
    .join(df_updates.alias("upd"), "id", "left") \
    .select(
        col("curr.id"),
        coalesce(col("upd.name"), col("curr.name")).alias("name"),
        coalesce(col("upd.department"), col("curr.department")).alias("department"),
        coalesce(col("upd.salary"), col("curr.salary")).alias("salary"),
        coalesce(col("upd.date"), col("curr.date")).alias("date")
    )

# Add new records
df_final_state = df_after_updates.union(df_inserts)

print("Current state:")
df_current.show()
print("After CDC operations:")
df_final_state.show()

# Challenge 51: Advanced Aggregations
print("\n51. ADVANCED AGGREGATIONS")
print("Problem: Implement complex aggregation patterns")

advanced_agg_data = [
    ("2024-01-01", "Electronics", "Laptop", 1200, 2),
    ("2024-01-01", "Electronics", "Phone", 800, 5),
    ("2024-01-01", "Clothing", "Shirt", 50, 10),
    ("2024-01-02", "Electronics", "Laptop", 1200, 3),
    ("2024-01-02", "Clothing", "Jeans", 80, 8),
    ("2024-01-03", "Electronics", "Tablet", 600, 4),
    ("2024-01-03", "Home", "Chair", 200, 6),
    ("2024-01-04", "Home", "Table", 300, 2)
]

df_advanced_agg = spark.createDataFrame(advanced_agg_data, 
    ["date", "category", "product", "price", "quantity"])

# Solution
print("Solution:")
df_agg_analysis = df_advanced_agg \
    .withColumn("revenue", col("price") * col("quantity")) \
    .groupBy("category") \
    .agg(
        count("*").alias("product_count"),
        sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price"),
        collect_list("product").alias("products"),
        approx_count_distinct("product").alias("unique_products"),
        percentile_approx("price", 0.5).alias("median_price"),
        stddev("price").alias("price_stddev")
    )

df_agg_analysis.show(truncate=False)

# Challenge 52: Graph Analytics
print("\n52. GRAPH ANALYTICS")
print("Problem: Analyze graph data for network insights")

edges_data = [
    (1, 2, "follows", 0.8),
    (1, 3, "likes", 0.6),
    (2, 4, "follows", 0.9),
    (3, 4, "comments", 0.7),
    (4, 5, "shares", 0.5),
    (2, 5, "follows", 0.8),
    (5, 1, "mentions", 0.4),
    (3, 5, "likes", 0.6)
]

vertices_data = [
    (1, "Alice", "Influencer", 10000),
    (2, "Bob", "User", 500),
    (3, "Charlie", "Brand", 50000),
    (4, "Diana", "Creator", 5000),
    (5, "Eve", "User", 1200)
]

df_edges = spark.createDataFrame(edges_data, ["src", "dst", "relationship", "weight"])
df_vertices = spark.createDataFrame(vertices_data, ["id", "name", "type", "followers"])

# Solution
print("Solution:")
# Calculate node degrees
out_degrees = df_edges.groupBy("src").agg(
    count("*").alias("out_degree"),
    sum("weight").alias("out_weight")
)

in_degrees = df_edges.groupBy("dst").agg(
    count("*").alias("in_degree"),
    sum("weight").alias("in_weight")
)

# Network metrics
network_metrics = df_vertices.alias("v") \
    .join(out_degrees.alias("out"), col("v.id") == col("out.src"), "left") \
    .join(in_degrees.alias("in"), col("v.id") == col("in.dst"), "left") \
    .fillna(0) \
    .withColumn("total_degree", 
               coalesce(col("out_degree"), lit(0)) + coalesce(col("in_degree"), lit(0))) \
    .withColumn("influence_score", 
               col("followers") * coalesce(col("total_degree"), lit(1)))

print("Network analysis:")
network_metrics.show()

# Challenge 53: Real-time Recommendations
print("\n53. REAL-TIME RECOMMENDATIONS")
print("Problem: Build real-time recommendation system")

user_interactions = [
    (1, 101, "view", 4, "2024-01-15 10:00:00"),
    (1, 102, "purchase", 5, "2024-01-15 10:05:00"),
    (2, 101, "view", 3, "2024-01-15 10:02:00"),
    (2, 103, "purchase", 4, "2024-01-15 10:07:00"),
    (3, 102, "view", 5, "2024-01-15 10:03:00"),
    (3, 104, "purchase", 4, "2024-01-15 10:08:00"),
    (4, 101, "view", 2, "2024-01-15 10:04:00"),
    (4, 102, "view", 4, "2024-01-15 10:09:00")
]

df_interactions = spark.createDataFrame(user_interactions, 
    ["user_id", "item_id", "action", "rating", "timestamp"])

# Solution
print("Solution:")
# User-item interaction matrix
user_item_matrix = df_interactions.groupBy("user_id", "item_id") \
    .agg(sum(when(col("action") == "purchase", 5)
            .when(col("action") == "view", 1)
            .otherwise(0)).alias("interaction_score"))

# Item similarity (simplified)
item_stats = df_interactions.groupBy("item_id") \
    .agg(avg("rating").alias("avg_rating"),
         count("*").alias("interaction_count"),
         countDistinct("user_id").alias("unique_users"))

# Generate recommendations
recommendations = user_item_matrix.alias("ui1") \
    .join(user_item_matrix.alias("ui2"), 
          (col("ui1.user_id") != col("ui2.user_id")) & 
          (col("ui1.item_id") == col("ui2.item_id")), "inner") \
    .groupBy(col("ui1.user_id"), col("ui2.item_id")) \
    .agg(sum(col("ui1.interaction_score") * col("ui2.interaction_score")).alias("similarity_score")) \
    .filter(col("similarity_score") > 10)

print("User-item interactions:")
user_item_matrix.show()
print("Recommendations:")
recommendations.show()

# Challenge 54: Fraud Detection
print("\n54. FRAUD DETECTION")
print("Problem: Detect fraudulent transactions using patterns")

transaction_data = [
    (1, 1001, 250.00, "2024-01-15 10:00:00", "NYC", "approved"),
    (2, 1001, 50.00, "2024-01-15 10:05:00", "NYC", "approved"),
    (3, 1002, 1500.00, "2024-01-15 10:02:00", "LA", "approved"),
    (4, 1001, 2000.00, "2024-01-15 10:07:00", "Tokyo", "flagged"),  # Location anomaly
    (5, 1003, 100.00, "2024-01-15 10:03:00", "Chicago", "approved"),
    (6, 1001, 300.00, "2024-01-15 10:08:00", "NYC", "approved"),
    (7, 1002, 10000.00, "2024-01-15 10:04:00", "LA", "flagged"),  # Amount anomaly
    (8, 1003, 80.00, "2024-01-15 10:09:00", "Chicago", "approved")
]

df_transactions = spark.createDataFrame(transaction_data, 
    ["transaction_id", "user_id", "amount", "timestamp", "location", "status"])
df_transactions = df_transactions.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
user_window = Window.partitionBy("user_id").orderBy("timestamp")

df_fraud_detection = df_transactions \
    .withColumn("prev_location", lag("location").over(user_window)) \
    .withColumn("prev_amount", lag("amount").over(user_window)) \
    .withColumn("prev_timestamp", lag("timestamp").over(user_window)) \
    .withColumn("time_diff_minutes", 
               (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60) \
    .withColumn("location_change", 
               when(col("location") != col("prev_location"), True).otherwise(False)) \
    .withColumn("amount_spike", 
               when(col("amount") > col("prev_amount") * 3, True).otherwise(False)) \
    .withColumn("rapid_transaction", 
               when(col("time_diff_minutes") < 5, True).otherwise(False)) \
    .withColumn("fraud_score", 
               (col("location_change").cast("int") * 3 + 
                col("amount_spike").cast("int") * 4 + 
                col("rapid_transaction").cast("int") * 2)) \
    .withColumn("is_suspicious", 
               col("fraud_score") >= 5)

print("Fraud detection analysis:")
df_fraud_detection.select("transaction_id", "user_id", "amount", "location", 
                         "fraud_score", "is_suspicious").show()

# Challenge 55: Data Mesh Architecture
print("\n55. DATA MESH ARCHITECTURE")
print("Problem: Implement data mesh pattern with domain ownership")

domain_data = [
    ("customer", "customer_profile", "CRM_team", "customers", 1000000),
    ("customer", "customer_interactions", "CRM_team", "interactions", 5000000),
    ("product", "product_catalog", "Product_team", "products", 50000),
    ("product", "product_reviews", "Product_team", "reviews", 2000000),
    ("order", "order_management", "Order_team", "orders", 3000000),
    ("order", "order_fulfillment", "Order_team", "shipments", 3500000),
    ("finance", "billing", "Finance_team", "invoices", 4000000),
    ("finance", "payments", "Finance_team", "payments", 8000000)
]

df_data_mesh = spark.createDataFrame(domain_data, 
    ["domain", "data_product", "owner_team", "table_name", "record_count"])

# Solution
print("Solution:")
df_mesh_analysis = df_data_mesh \
    .withColumn("data_size_category",
               when(col("record_count") > 1000000, "Large")
               .when(col("record_count") > 100000, "Medium")
               .otherwise("Small")) \
    .withColumn("domain_priority",
               when(col("domain") == "customer", 1)
               .when(col("domain") == "order", 2)
               .when(col("domain") == "product", 3)
               .otherwise(4))

# Domain metrics
domain_metrics = df_mesh_analysis.groupBy("domain", "owner_team") \
    .agg(count("*").alias("data_products"),
         sum("record_count").alias("total_records"),
         collect_list("data_product").alias("products"))

print("Data mesh domain analysis:")
domain_metrics.show(truncate=False)

# Challenge 56: Multi-tenancy Data Processing
print("\n56. MULTI-TENANCY DATA PROCESSING")
print("Problem: Process data for multiple tenants securely")

tenant_data = [
    ("tenant_A", 1, "Alice", "Engineering", 75000),
    ("tenant_A", 2, "Bob", "Marketing", 65000),
    ("tenant_B", 1, "Charlie", "Finance", 70000),
    ("tenant_B", 2, "Diana", "HR", 72000),
    ("tenant_C", 1, "Eve", "Engineering", 80000),
    ("tenant_A", 3, "Frank", "Engineering", 78000),
    ("tenant_B", 3, "Grace", "Marketing", 68000),
    ("tenant_C", 2, "Henry", "Finance", 74000)
]

df_multitenancy = spark.createDataFrame(tenant_data, 
    ["tenant_id", "employee_id", "name", "department", "salary"])

# Solution
print("Solution:")
# Tenant-specific processing
def process_tenant_data(tenant_df, tenant_id):
    return tenant_df.filter(col("tenant_id") == tenant_id) \
                   .withColumn("processed_timestamp", current_timestamp()) \
                   .withColumn("data_classification", 
                             when(col("salary") > 75000, "Confidential")
                             .otherwise("Internal"))

# Process each tenant separately
tenant_ids = df_multitenancy.select("tenant_id").distinct().collect()

processed_results = []
for row in tenant_ids:
    tenant_id = row["tenant_id"]
    tenant_result = process_tenant_data(df_multitenancy, tenant_id)
    processed_results.append(tenant_result)

# Combine results (in practice, would be stored separately)
df_processed_multitenancy = processed_results[0]
for i in range(1, len(processed_results)):
    df_processed_multitenancy = df_processed_multitenancy.union(processed_results[i])

print("Multi-tenant processing results:")
df_processed_multitenancy.show()

# Challenge 57: Data Versioning
print("\n57. DATA VERSIONING")
print("Problem: Implement data versioning for reproducibility")

versioned_data = [
    (1, "Alice", "Engineering", 75000, "v1.0", "2024-01-15"),
    (2, "Bob", "Marketing", 65000, "v1.0", "2024-01-15"),
    (1, "Alice", "Senior Engineering", 78000, "v1.1", "2024-01-20"),  # Promotion
    (2, "Bob", "Marketing", 67000, "v1.1", "2024-01-20"),  # Salary adjustment
    (3, "Charlie", "Finance", 70000, "v1.1", "2024-01-20"),  # New hire
    (1, "Alice", "Senior Engineering", 80000, "v1.2", "2024-01-25")   # Another raise
]

df_versioned = spark.createDataFrame(versioned_data, 
    ["employee_id", "name", "position", "salary", "version", "effective_date"])

# Solution
print("Solution:")
# Latest version per employee
latest_version_window = Window.partitionBy("employee_id").orderBy(desc("version"))
df_latest = df_versioned.withColumn("rn", row_number().over(latest_version_window)) \
                       .filter(col("rn") == 1).drop("rn")

# Version comparison
version_comparison = df_versioned.alias("v1") \
    .join(df_versioned.alias("v2"), 
          (col("v1.employee_id") == col("v2.employee_id")) & 
          (col("v1.version") < col("v2.version")), "inner") \
    .select(col("v1.employee_id"), col("v1.version").alias("from_version"), 
           col("v2.version").alias("to_version"),
           col("v1.salary").alias("old_salary"), col("v2.salary").alias("new_salary")) \
    .withColumn("salary_change", col("new_salary") - col("old_salary"))

print("Latest version per employee:")
df_latest.show()
print("Salary changes between versions:")
version_comparison.show()

# Challenge 58: Cross-Cloud Data Processing
print("\n58. CROSS-CLOUD DATA PROCESSING")
print("Problem: Process data across multiple cloud providers")

cloud_data = [
    ("aws_s3", "customer_data", 1000000, "us-east-1", "parquet"),
    ("azure_blob", "product_data", 500000, "eastus", "delta"),
    ("gcp_storage", "transaction_data", 2000000, "us-central1", "parquet"),
    ("aws_s3", "log_data", 10000000, "us-west-2", "json"),
    ("azure_blob", "analytics_data", 800000, "westeurope", "delta"),
    ("gcp_storage", "ml_features", 300000, "europe-west1", "parquet")
]

df_cloud = spark.createDataFrame(cloud_data, 
    ["cloud_provider", "dataset_name", "record_count", "region", "format"])

# Solution
print("Solution:")
# Cross-cloud analytics
cloud_summary = df_cloud.groupBy("cloud_provider") \
    .agg(count("*").alias("dataset_count"),
         sum("record_count").alias("total_records"),
         collect_set("region").alias("regions"),
         collect_set("format").alias("formats"))

# Data distribution analysis
region_distribution = df_cloud.groupBy("region") \
    .agg(sum("record_count").alias("records_in_region"),
         countDistinct("cloud_provider").alias("cloud_providers"))

print("Cross-cloud data summary:")
cloud_summary.show(truncate=False)
print("Regional distribution:")
region_distribution.show()

# Challenge 59: Data Lineage Visualization
print("\n59. DATA LINEAGE VISUALIZATION")
print("Problem: Create data lineage mapping for governance")

lineage_mapping = [
    ("raw_customers", "bronze_customers", "ingestion", "2024-01-15 08:00:00"),
    ("bronze_customers", "silver_customers", "cleansing", "2024-01-15 09:00:00"),
    ("silver_customers", "gold_customer_metrics", "aggregation", "2024-01-15 10:00:00"),
    ("raw_orders", "bronze_orders", "ingestion", "2024-01-15 08:30:00"),
    ("bronze_orders", "silver_orders", "cleansing", "2024-01-15 09:30:00"),
    ("silver_orders", "gold_order_metrics", "aggregation", "2024-01-15 10:30:00"),
    ("gold_customer_metrics", "customer_dashboard", "visualization", "2024-01-15 11:00:00"),
    ("gold_order_metrics", "order_dashboard", "visualization", "2024-01-15 11:30:00")
]

df_lineage = spark.createDataFrame(lineage_mapping, 
    ["source_table", "target_table", "transformation", "timestamp"])

# Solution
print("Solution:")
# Lineage depth analysis
df_lineage_depth = df_lineage \
    .withColumn("level", 
               when(col("source_table").startswith("raw_"), 1)
               .when(col("source_table").startswith("bronze_"), 2)
               .when(col("source_table").startswith("silver_"), 3)
               .when(col("source_table").startswith("gold_"), 4)
               .otherwise(5)) \
    .withColumn("transformation_type",
               when(col("transformation") == "ingestion", "Data Ingestion")
               .when(col("transformation") == "cleansing", "Data Quality")
               .when(col("transformation") == "aggregation", "Business Logic")
               .otherwise("Presentation"))

# Lineage chain construction
lineage_chain = df_lineage_depth.orderBy("level", "timestamp")

print("Data lineage mapping:")
lineage_chain.show(truncate=False)

# Challenge 60: Feature Store Implementation
print("\n60. FEATURE STORE IMPLEMENTATION")
print("Problem: Build feature store for ML workflows")

feature_data = [
    ("customer_001", 35, 75000, 5, 0.95, 3, "2024-01-15"),
    ("customer_002", 28, 55000, 2, 0.87, 1, "2024-01-15"),
    ("customer_003", 42, 95000, 8, 0.92, 5, "2024-01-15"),
    ("customer_004", 31, 65000, 3, 0.89, 2, "2024-01-15"),
    ("customer_005", 38, 85000, 6, 0.94, 4, "2024-01-15"),
    ("customer_006", 29, 60000, 4, 0.88, 2, "2024-01-15"),
    ("customer_007", 45, 105000, 10, 0.96, 6, "2024-01-15")
]

df_features = spark.createDataFrame(feature_data, 
    ["customer_id", "age", "income", "tenure_years", "satisfaction_score", "product_count", "feature_date"])

# Solution
print("Solution:")
# Feature engineering and versioning
df_feature_store = df_features \
    .withColumn("age_group", 
               when(col("age") < 30, "Young")
               .when(col("age") < 40, "Middle")
               .otherwise("Senior")) \
    .withColumn("income_tier", 
               when(col("income") > 80000, "High")
               .when(col("income") > 60000, "Medium")
               .otherwise("Low")) \
    .withColumn("customer_value_score", 
               col("income") * col("satisfaction_score") * col("product_count") / 100000) \
    .withColumn("feature_version", lit("v1.0")) \
    .withColumn("created_timestamp", current_timestamp())

# Feature statistics for monitoring
feature_stats = df_feature_store.select(
    avg("age").alias("avg_age"),
    stddev("age").alias("stddev_age"),
    avg("income").alias("avg_income"),
    stddev("income").alias("stddev_income"),
    avg("customer_value_score").alias("avg_value_score"),
    min("satisfaction_score").alias("min_satisfaction"),
    max("satisfaction_score").alias("max_satisfaction")
)

print("Feature store data:")
df_feature_store.show()
print("Feature statistics:")
feature_stats.show()

# Challenge 61: Data Privacy Compliance
print("\n61. DATA PRIVACY COMPLIANCE")
print("Problem: Implement GDPR/privacy compliance measures")

privacy_data = [
    (1, "alice@email.com", "Alice Johnson", "555-1234", "US", True),
    (2, "bob@email.com", "Bob Smith", "555-5678", "EU", True),
    (3, "charlie@email.com", "Charlie Brown", "555-9012", "EU", False),  # Opt-out
    (4, "diana@email.com", "Diana Prince", "555-3456", "UK", True),
    (5, "eve@email.com", "Eve Wilson", "555-7890", "US", True),
    (6, "frank@email.com", "Frank Miller", "555-2345", "EU", True),
    (7, "grace@email.com", "Grace Lee", "555-6789", "CA", False)  # Opt-out
]

df_privacy = spark.createDataFrame(privacy_data, 
    ["customer_id", "email", "full_name", "phone", "region", "consent_given"])

# Solution
print("Solution:")
# Apply privacy rules based on region and consent
df_privacy_compliant = df_privacy \
    .withColumn("requires_gdpr", col("region").isin(["EU", "UK"])) \
    .withColumn("can_process", 
               (col("consent_given") == True) | (~col("requires_gdpr"))) \
    .withColumn("email_masked", 
               when(~col("can_process"), 
                   concat(lit("***@"), regexp_extract(col("email"), "@(.+)", 1)))
               .otherwise(col("email"))) \
    .withColumn("name_masked", 
               when(~col("can_process"), lit("*** ***"))
               .otherwise(col("full_name"))) \
    .withColumn("phone_masked", 
               when(~col("can_process"), lit("***-****"))
               .otherwise(col("phone"))) \
    .withColumn("data_retention_days", 
               when(col("requires_gdpr"), 730)  # 2 years for GDPR
               .otherwise(2555))  # 7 years for others

print("Privacy compliant data:")
df_privacy_compliant.select("customer_id", "email_masked", "name_masked", 
                           "can_process", "requires_gdpr").show()

# Challenge 62: Distributed Cache Management
print("\n62. DISTRIBUTED CACHE MANAGEMENT")
print("Problem: Implement efficient caching strategy")

cache_data = []
for i in range(1, 51):  # Larger dataset simulation
    cache_data.append((i, f"query_{i%10}", i % 5, f"result_{i}", i % 3))

# Sample for demonstration
cache_sample = cache_data[:15]
df_cache = spark.createDataFrame(cache_sample, 
    ["query_id", "query_pattern", "frequency", "result", "cache_tier"])

# Solution
print("Solution:")
# Cache strategy based on access patterns
df_cache_strategy = df_cache \
    .withColumn("cache_priority", 
               when(col("frequency") >= 4, "High")
               .when(col("frequency") >= 2, "Medium")
               .otherwise("Low")) \
    .withColumn("cache_ttl_hours", 
               when(col("cache_priority") == "High", 24)
               .when(col("cache_priority") == "Medium", 12)
               .otherwise(4)) \
    .withColumn("should_cache", col("frequency") >= 2)

# Cache hit analysis
cache_analysis = df_cache_strategy.groupBy("cache_priority") \
    .agg(count("*").alias("query_count"),
         avg("frequency").alias("avg_frequency"))

print("Cache strategy:")
df_cache_strategy.show()
print("Cache analysis:")
cache_analysis.show()

# Challenge 63: Data Quality Monitoring
print("\n63. DATA QUALITY MONITORING")
print("Problem: Implement automated data quality monitoring")

quality_metrics_data = [
    ("customers", "completeness", 0.95, 0.98, "2024-01-15", "PASS"),
    ("customers", "uniqueness", 0.99, 0.95, "2024-01-15", "PASS"),
    ("orders", "completeness", 0.87, 0.90, "2024-01-15", "FAIL"),
    ("orders", "validity", 0.94, 0.92, "2024-01-15", "PASS"),
    ("products", "completeness", 0.99, 0.95, "2024-01-15", "PASS"),
    ("products", "consistency", 0.88, 0.90, "2024-01-15", "FAIL"),
    ("reviews", "completeness", 0.92, 0.90, "2024-01-15", "PASS")
]

df_quality_metrics = spark.createDataFrame(quality_metrics_data, 
    ["table_name", "metric_type", "actual_score", "threshold", "date", "status"])

# Solution
print("Solution:")
# Quality monitoring dashboard
df_quality_monitoring = df_quality_metrics \
    .withColumn("score_difference", col("actual_score") - col("threshold")) \
    .withColumn("quality_trend", 
               when(col("score_difference") > 0.05, "Improving")
               .when(col("score_difference") < -0.05, "Degrading")
               .otherwise("Stable")) \
    .withColumn("alert_level", 
               when(col("status") == "FAIL", "Critical")
               .when(col("score_difference") < 0.02, "Warning")
               .otherwise("Good"))

# Summary by table
quality_summary = df_quality_monitoring.groupBy("table_name") \
    .agg(avg("actual_score").alias("avg_quality_score"),
         count(when(col("status") == "FAIL", 1)).alias("failed_checks"),
         count("*").alias("total_checks"))

print("Quality monitoring results:")
df_quality_monitoring.show()
print("Quality summary by table:")
quality_summary.show()

# Challenge 64: Automated Data Pipeline Testing
print("\n64. AUTOMATED DATA PIPELINE TESTING")
print("Problem: Implement automated testing for data pipelines")

pipeline_test_data = [
    ("test_001", "schema_validation", "customers", "PASS", "2024-01-15 10:00:00"),
    ("test_002", "data_freshness", "customers", "PASS", "2024-01-15 10:01:00"),
    ("test_003", "row_count_check", "orders", "FAIL", "2024-01-15 10:02:00"),
    ("test_004", "null_check", "orders", "PASS", "2024-01-15 10:03:00"),
    ("test_005", "duplicate_check", "products", "PASS", "2024-01-15 10:04:00"),
    ("test_006", "referential_integrity", "orders", "FAIL", "2024-01-15 10:05:00"),
    ("test_007", "business_rule_check", "customers", "PASS", "2024-01-15 10:06:00")
]

df_pipeline_tests = spark.createDataFrame(pipeline_test_data, 
    ["test_id", "test_type", "table_name", "result", "timestamp"])

# Solution
print("Solution:")
# Test result analysis
df_test_analysis = df_pipeline_tests \
    .withColumn("is_critical", 
               col("test_type").isin(["schema_validation", "referential_integrity"])) \
    .withColumn("test_category",
               when(col("test_type").contains("check"), "Data Quality")
               .when(col("test_type").contains("validation"), "Schema")
               .otherwise("Business Rules"))

# Test summary
test_summary = df_test_analysis.groupBy("table_name") \
    .agg(count("*").alias("total_tests"),
         count(when(col("result") == "PASS", 1)).alias("passed_tests"),
         count(when(col("result") == "FAIL", 1)).alias("failed_tests"),
         count(when((col("result") == "FAIL") & col("is_critical"), 1)).alias("critical_failures"))

print("Pipeline test results:")
df_test_analysis.show()
print("Test summary:")
test_summary.show()

# Challenge 65: Cost Optimization Analytics
print("\n65. COST OPTIMIZATION ANALYTICS")
print("Problem: Analyze and optimize data processing costs")

cost_data = [
    ("2024-01-15", "compute", "standard_cluster", 12.5, 8, 100.0),
    ("2024-01-15", "storage", "s3_standard", 0.023, 1000, 23.0),
    ("2024-01-15", "compute", "high_memory_cluster", 25.0, 4, 100.0),
    ("2024-01-16", "compute", "standard_cluster", 12.5, 10, 125.0),
    ("2024-01-16", "storage", "s3_standard", 0.023, 1200, 27.6),
    ("2024-01-16", "network", "data_transfer", 0.09, 500, 45.0),
    ("2024-01-17", "compute", "spot_instances", 6.0, 8, 48.0),
    ("2024-01-17", "storage", "s3_ia", 0.0125, 800, 10.0)
]

df_costs = spark.createDataFrame(cost_data, 
    ["date", "service_type", "resource_type", "unit_cost", "usage", "total_cost"])

# Solution
print("Solution:")
# Cost optimization analysis
df_cost_analysis = df_costs \
    .withColumn("cost_efficiency", col("usage") / col("total_cost")) \
    .withColumn("optimization_potential",
               when(col("resource_type").contains("standard"), 
                   col("total_cost") * 0.3)  # 30% savings potential
               .when(col("resource_type").contains("spot"), 
                   col("total_cost") * 0.1)  # Already optimized
               .otherwise(col("total_cost") * 0.2))

# Daily cost summary
daily_costs = df_cost_analysis.groupBy("date") \
    .agg(sum("total_cost").alias("daily_total"),
         sum("optimization_potential").alias("potential_savings"),
         count("*").alias("line_items"))

# Service type breakdown
service_breakdown = df_cost_analysis.groupBy("service_type") \
    .agg(sum("total_cost").alias("service_total"),
         avg("cost_efficiency").alias("avg_efficiency"))

print("Cost analysis:")
df_cost_analysis.show()
print("Daily cost summary:")
daily_costs.show()
print("Service breakdown:")
service_breakdown.show()

# =============================================================================
# ADVANCED LEVEL CHALLENGES (66-100)
# =============================================================================

print("\n" + "="*60)
print("ADVANCED LEVEL CHALLENGES (66-100)")
print("="*60)

# Challenge 66: Advanced ML Integration
print("\n66. ADVANCED ML INTEGRATION")
print("Problem: Build and deploy ML pipeline with feature engineering")

ml_data = [
    (1, 25, 50000, 3, 5, 600, 1),
    (2, 35, 75000, 5, 2, 750, 1),
    (3, 28, 60000, 2, 4, 580, 0),
    (4, 42, 90000, 8, 1, 800, 1),
    (5, 31, 65000, 4, 3, 620, 0),
    (6, 38, 80000, 6, 2, 720, 1),
    (7, 29, 55000, 3, 4, 590, 0),
    (8, 45, 95000, 10, 1, 820, 1)
]

df_ml = spark.createDataFrame(ml_data, 
    ["customer_id", "age", "income", "experience", "dependents", "credit_score", "approved"])

# Solution
print("Solution:")
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

feature_cols = ["age", "income", "experience", "dependents", "credit_score"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
lr = LogisticRegression(featuresCol="scaled_features", labelCol="approved")

pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(df_ml)
predictions = model.transform(df_ml)

print("ML Pipeline Results:")
predictions.select("customer_id", "approved", "prediction", "probability").show()

# Challenge 67: Complex Data Transformations
print("\n67. COMPLEX DATA TRANSFORMATIONS")
print("Problem: Handle nested data structures and complex business rules")

nested_data = [
    (1, {"name": "John", "address": {"street": "123 Main St", "city": "NYC", "zip": "10001"}, 
         "orders": [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]}),
    (2, {"name": "Sarah", "address": {"street": "456 Oak Ave", "city": "LA", "zip": "90210"}, 
         "orders": [{"id": 3, "amount": 150}]}),
    (3, {"name": "Mike", "address": {"street": "789 Pine Rd", "city": "Chicago", "zip": "60601"}, 
         "orders": []})
]

nested_json_data = [(row[0], json.dumps(row[1])) for row in nested_data]
df_nested = spark.createDataFrame(nested_json_data, ["id", "data_json"])

# Solution
print("Solution:")
nested_schema = StructType([
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True),
    StructField("orders", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("amount", IntegerType(), True)
    ])), True)
])

df_parsed_nested = df_nested.withColumn("parsed_data", from_json(col("data_json"), nested_schema)) \
                           .select("id", "parsed_data.*") \
                           .withColumn("full_address", 
                                     concat(col("address.street"), lit(", "), 
                                           col("address.city"), lit(" "), col("address.zip"))) \
                           .withColumn("total_orders", size(col("orders"))) \
                           .withColumn("total_order_amount", 
                                     expr("aggregate(orders, 0, (acc, x) -> acc + x.amount)"))

print("Parsed nested data:")
df_parsed_nested.show(truncate=False)

# Challenge 68: Real-time Analytics Dashboard
print("\n68. REAL-TIME ANALYTICS DASHBOARD")
print("Problem: Create real-time metrics for business dashboard")

realtime_events = [
    ("2024-01-15 10:00:00", "user1", "login", "web", "premium"),
    ("2024-01-15 10:01:00", "user2", "purchase", "mobile", "standard"),
    ("2024-01-15 10:02:00", "user3", "view", "web", "premium"),
    ("2024-01-15 10:03:00", "user1", "purchase", "web", "premium"),
    ("2024-01-15 10:04:00", "user4", "signup", "mobile", "free"),
    ("2024-01-15 10:05:00", "user2", "view", "mobile", "standard"),
    ("2024-01-15 10:06:00", "user5", "login", "web", "premium"),
    ("2024-01-15 10:07:00", "user3", "purchase", "tablet", "premium")
]

df_events = spark.createDataFrame(realtime_events, 
    ["timestamp", "user_id", "event_type", "platform", "subscription"])
df_events = df_events.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
current_time = "2024-01-15 10:07:00"
recent_events = df_events.filter(col("timestamp") >= 
    (to_timestamp(lit(current_time)) - expr("INTERVAL 5 MINUTES")))

dashboard_metrics = recent_events.agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("active_users"),
    sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
    sum(when(col("event_type") == "signup", 1).otherwise(0)).alias("signups")
)

platform_stats = recent_events.groupBy("platform").agg(
    count("*").alias("events"),
    countDistinct("user_id").alias("users")
)

subscription_stats = recent_events.groupBy("subscription").agg(
    count("*").alias("events"),
    countDistinct("user_id").alias("users")
)

print("Dashboard Metrics (Last 5 minutes):")
dashboard_metrics.show()
print("Platform Statistics:")
platform_stats.show()
print("Subscription Tier Analysis:")
subscription_stats.show()

# Challenge 69: Data Lake Architecture
print("\n69. DATA LAKE ARCHITECTURE")
print("Problem: Implement medallion architecture (Bronze, Silver, Gold)")

raw_customer_data = [
    ("1", "John Doe", "john@email.com", "2024-01-15", "active", "NYC"),
    ("2", "Sarah Smith", "sarah@email.com", "2024-01-16", "inactive", "LA"),
    ("3", "Mike Johnson", "mike@email.com", "2024-01-17", "active", "Chicago"),
    ("4", "Lisa Brown", "lisa@email.com", "2024-01-18", "pending", "Boston"),
    ("5", "David Wilson", "david@email.com", "2024-01-19", "active", "Seattle")
]

bronze_df = spark.createDataFrame(raw_customer_data, 
    ["customer_id", "name", "email", "created_date", "status", "city"])

# Solution
print("Solution:")
# Silver layer - cleaned and validated data
silver_df = bronze_df.withColumn("customer_id", col("customer_id").cast(IntegerType())) \
                    .withColumn("created_date", to_date(col("created_date"))) \
                    .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
                    .withColumn("name_clean", trim(upper(col("name")))) \
                    .withColumn("is_active", when(col("status") == "active", True).otherwise(False)) \
                    .withColumn("ingestion_date", current_date())

# Gold layer - business metrics and aggregations
gold_df = silver_df.groupBy("city", "is_active") \
                 .agg(count("*").alias("customer_count"),
                      min("created_date").alias("first_customer_date"),
                      max("created_date").alias("latest_customer_date")) \
                 .withColumn("analysis_date", current_date())

print("Bronze Layer (Raw):")
bronze_df.show()
print("Silver Layer (Cleaned):")
silver_df.show()
print("Gold Layer (Aggregated):")
gold_df.show()

# Challenge 70: Advanced Error Handling
print("\n70. ADVANCED ERROR HANDLING")
print("Problem: Implement robust error handling and data validation")

error_prone_data = [
    ("1", "valid_data", "100", "2024-01-15"),
    ("invalid_id", "test", "200", "2024-01-16"),
    ("3", "another_test", "invalid_number", "2024-01-17"),
    ("4", "good_data", "300", "invalid_date"),
    ("5", "perfect_data", "400", "2024-01-18"),
    (None, "null_id", "500", "2024-01-19"),
    ("7", "", "600", "2024-01-20")
]

df_error_prone = spark.createDataFrame(error_prone_data, ["id", "name", "value", "date"])

# Solution
print("Solution:")
def safe_cast_int(col_name):
    return when(col(col_name).rlike("^\\d+$"), col(col_name).cast(IntegerType())).otherwise(None)

def safe_cast_date(col_name):
    return when(to_date(col(col_name)).isNotNull(), to_date(col(col_name))).otherwise(None)

df_error_handled = df_error_prone.withColumn("id_clean", safe_cast_int("id")) \
                                .withColumn("value_clean", safe_cast_int("value")) \
                                .withColumn("date_clean", safe_cast_date("date")) \
                                .withColumn("name_clean", 
                                          when((col("name").isNull()) | (col("name") == ""), "MISSING")
                                          .otherwise(col("name"))) \
                                .withColumn("error_flags", 
                                          concat_ws(",",
                                                  when(col("id_clean").isNull(), "INVALID_ID"),
                                                  when(col("value_clean").isNull(), "INVALID_VALUE"),
                                                  when(col("date_clean").isNull(), "INVALID_DATE"),
                                                  when(col("name_clean") == "MISSING", "MISSING_NAME")))

clean_records = df_error_handled.filter(col("error_flags") == "")
error_records = df_error_handled.filter(col("error_flags") != "")

print("Error handling results:")
df_error_handled.show(truncate=False)
print(f"Clean records: {clean_records.count()}")
print(f"Error records: {error_records.count()}")

# Challenge 71: Advanced Streaming Analytics
print("\n71. ADVANCED STREAMING ANALYTICS")
print("Problem: Implement complex streaming analytics with state management")

streaming_state_data = [
    ("session_1", "user1", "start", "2024-01-15 10:00:00"),
    ("session_1", "user1", "click", "2024-01-15 10:01:00"),
    ("session_1", "user1", "purchase", "2024-01-15 10:05:00"),
    ("session_1", "user1", "end", "2024-01-15 10:10:00"),
    ("session_2", "user2", "start", "2024-01-15 10:02:00"),
    ("session_2", "user2", "view", "2024-01-15 10:03:00"),
    ("session_2", "user2", "exit", "2024-01-15 10:04:00"),
    ("session_3", "user1", "start", "2024-01-15 10:15:00")
]

df_streaming_state = spark.createDataFrame(streaming_state_data, 
    ["session_id", "user_id", "event_type", "timestamp"])
df_streaming_state = df_streaming_state.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
session_window = Window.partitionBy("session_id").orderBy("timestamp")

df_session_analysis = df_streaming_state \
    .withColumn("session_start", 
               first("timestamp").over(session_window)) \
    .withColumn("session_end", 
               last("timestamp").over(session_window)) \
    .withColumn("session_duration_minutes", 
               (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60) \
    .withColumn("event_sequence", 
               row_number().over(session_window)) \
    .withColumn("has_purchase", 
               max(when(col("event_type") == "purchase", 1).otherwise(0)).over(Window.partitionBy("session_id")))

session_summary = df_session_analysis.groupBy("session_id", "user_id") \
    .agg(first("session_duration_minutes").alias("duration"),
         count("*").alias("event_count"),
         first("has_purchase").alias("converted"))

print("Session analysis:")
session_summary.show()

# Challenge 72: Multi-dimensional Analytics
print("\n72. MULTI-DIMENSIONAL ANALYTICS")
print("Problem: Implement OLAP-style multi-dimensional analysis")

olap_data = [
    ("2024-01", "North", "Electronics", "Laptop", 1200, 10),
    ("2024-01", "North", "Electronics", "Phone", 800, 15),
    ("2024-01", "South", "Clothing", "Shirt", 50, 100),
    ("2024-01", "South", "Clothing", "Jeans", 80, 50),
    ("2024-02", "North", "Electronics", "Laptop", 1200, 12),
    ("2024-02", "North", "Electronics", "Tablet", 600, 8),
    ("2024-02", "South", "Home", "Chair", 200, 25),
    ("2024-02", "West", "Electronics", "Phone", 800, 20)
]

df_olap = spark.createDataFrame(olap_data, 
    ["month", "region", "category", "product", "price", "quantity"])

# Solution
print("Solution:")
df_olap_analysis = df_olap.withColumn("revenue", col("price") * col("quantity"))

# Multi-dimensional rollups
cube_analysis = df_olap_analysis.cube("month", "region", "category") \
    .agg(sum("revenue").alias("total_revenue"),
         sum("quantity").alias("total_quantity"),
         count("*").alias("product_lines"))

# Dimensional analysis
regional_trends = df_olap_analysis.groupBy("month", "region") \
    .agg(sum("revenue").alias("revenue")) \
    .withColumn("prev_month_revenue", 
               lag("revenue").over(Window.partitionBy("region").orderBy("month"))) \
    .withColumn("growth_rate", 
               ((col("revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100))

print("CUBE analysis:")
cube_analysis.show()
print("Regional trends:")
regional_trends.show()

# Challenge 73: Advanced Graph Processing
print("\n73. ADVANCED GRAPH PROCESSING")
print("Problem: Implement graph algorithms for network analysis")

graph_edges = [
    (1, 2, "follows", 0.8, 5),
    (1, 3, "likes", 0.6, 3),
    (2, 4, "follows", 0.9, 10),
    (3, 4, "comments", 0.7, 7),
    (4, 5, "shares", 0.5, 2),
    (2, 5, "follows", 0.8, 8),
    (5, 1, "mentions", 0.4, 1),
    (3, 5, "likes", 0.6, 4),
    (1, 4, "follows", 0.7, 6),
    (2, 3, "likes", 0.5, 2)
]

df_graph = spark.createDataFrame(graph_edges, 
    ["source", "target", "relationship", "weight", "frequency"])

# Solution
print("Solution:")
# Calculate centrality measures
degree_centrality = df_graph.groupBy("source") \
    .agg(count("*").alias("out_degree"),
         sum("weight").alias("out_weight")) \
    .union(
        df_graph.groupBy("target") \
               .agg(count("*").alias("in_degree"),
                    sum("weight").alias("in_weight"))
    ) \
    .groupBy("source") \
    .agg(sum("out_degree").alias("total_degree"),
         sum("out_weight").alias("total_weight"))

# Community detection (simplified)
strong_connections = df_graph.filter(col("weight") > 0.7)
communities = strong_connections.groupBy("source") \
    .agg(collect_set("target").alias("connected_nodes"),
         count("*").alias("strong_connections"))

print("Degree centrality:")
degree_centrality.show()
print("Strong connections:")
communities.show(truncate=False)

# Challenge 74: Time Travel Queries
print("\n74. TIME TRAVEL QUERIES")
print("Problem: Implement time travel functionality for data versioning")

time_travel_data = [
    (1, "Alice", "Engineer", 75000, "2024-01-15", "v1"),
    (2, "Bob", "Manager", 85000, "2024-01-15", "v1"),
    (1, "Alice", "Senior Engineer", 80000, "2024-02-01", "v2"),
    (2, "Bob", "Senior Manager", 90000, "2024-02-01", "v2"),
    (3, "Charlie", "Analyst", 65000, "2024-02-01", "v2"),
    (1, "Alice", "Lead Engineer", 85000, "2024-03-01", "v3"),
    (2, "Bob", "Director", 100000, "2024-03-01", "v3"),
    (3, "Charlie", "Senior Analyst", 70000, "2024-03-01", "v3")
]

df_time_travel = spark.createDataFrame(time_travel_data, 
    ["employee_id", "name", "position", "salary", "effective_date", "version"])

# Solution
print("Solution:")
# Query as of specific date/version
def query_as_of_version(df, target_version):
    return df.filter(col("version") <= target_version) \
             .withColumn("rn", row_number().over(
                 Window.partitionBy("employee_id").orderBy(desc("version")))) \
             .filter(col("rn") == 1) \
             .drop("rn")

# Query historical changes
def get_employee_history(df, employee_id):
    return df.filter(col("employee_id") == employee_id) \
             .orderBy("version") \
             .withColumn("prev_salary", lag("salary").over(
                 Window.orderBy("version"))) \
             .withColumn("salary_change", 
                        col("salary") - coalesce(col("prev_salary"), col("salary")))

# Demonstrate time travel
v1_snapshot = query_as_of_version(df_time_travel, "v1")
alice_history = get_employee_history(df_time_travel, 1)

print("Snapshot as of v1:")
v1_snapshot.show()
print("Alice's salary history:")
alice_history.show()

# Challenge 75: Advanced Performance Tuning
print("\n75. ADVANCED PERFORMANCE TUNING")
print("Problem: Implement advanced performance optimization techniques")

perf_tuning_data = []
for i in range(1, 101):  # Larger dataset for performance testing
    category = f"category_{i%5}"
    region = f"region_{i%3}"
    perf_tuning_data.append((i, f"product_{i}", category, region, i*10, i%10))

# Sample for demonstration
perf_sample = perf_tuning_data[:20]
df_perf_tuning = spark.createDataFrame(perf_sample, 
    ["id", "product", "category", "region", "sales", "priority"])

# Solution
print("Solution:")
# Performance optimization techniques
df_perf_tuning.cache()  # Cache for multiple operations
df_optimized = df_perf_tuning.repartition(col("category"))  # Partition by frequently joined column

# Broadcast small lookup tables
priority_mapping = spark.createDataFrame([(i, f"priority_{i}") for i in range(10)], 
                                        ["priority", "priority_name"])

from pyspark.sql.functions import broadcast
df_with_priority = df_optimized.join(broadcast(priority_mapping), "priority")

# Columnar operations instead of row-by-row
df_efficient = df_with_priority.select(
    "id", "product", "category", "region", "sales",
    (col("sales") * 1.1).alias("sales_with_tax"),
    when(col("sales") > 50, "High").otherwise("Low").alias("sales_tier")
)

# Use efficient aggregations
category_summary = df_efficient.groupBy("category") \
    .agg(sum("sales").alias("total_sales"),
         count("*").alias("product_count"),
         avg("sales").alias("avg_sales"))

print("Performance optimized results:")
category_summary.show()

# Challenge 76: Data Mesh Implementation
print("\n76. DATA MESH IMPLEMENTATION")
print("Problem: Implement data mesh architecture with domain ownership")

data_products = [
    ("customer_360", "customer_domain", "CRM_team", "customers,interactions,preferences", "operational"),
    ("product_insights", "product_domain", "Product_team", "catalog,reviews,recommendations", "analytical"),
    ("order_analytics", "order_domain", "Order_team", "orders,shipments,returns", "analytical"),
    ("financial_reporting", "finance_domain", "Finance_team", "transactions,billing,revenue", "reporting"),
    ("marketing_campaigns", "marketing_domain", "Marketing_team", "campaigns,leads,conversions", "operational"),
    ("supply_chain", "operations_domain", "Ops_team", "inventory,suppliers,logistics", "operational")
]

df_data_products = spark.createDataFrame(data_products, 
    ["product_name", "domain", "owner_team", "data_sources", "product_type"])

# Solution
print("Solution:")
df_mesh_architecture = df_data_products \
    .withColumn("source_count", size(split(col("data_sources"), ","))) \
    .withColumn("complexity_score", 
               when(col("source_count") > 3, "High")
               .when(col("source_count") > 1, "Medium")
               .otherwise("Low")) \
    .withColumn("governance_level",
               when(col("product_type") == "reporting", "Strict")
               .when(col("product_type") == "analytical", "Standard")
               .otherwise("Flexible"))

# Domain metrics
domain_summary = df_mesh_architecture.groupBy("domain", "owner_team") \
    .agg(count("*").alias("product_count"),
         avg("source_count").alias("avg_complexity"),
         collect_list("product_name").alias("products"))

print("Data mesh architecture:")
df_mesh_architecture.show(truncate=False)
print("Domain summary:")
domain_summary.show(truncate=False)

# Challenge 77: Advanced Security Implementation
print("\n77. ADVANCED SECURITY IMPLEMENTATION")
print("Problem: Implement row-level and column-level security")

sensitive_data = [
    (1, "Alice", "Engineering", 75000, "SSN123", "alice@company.com", "admin"),
    (2, "Bob", "Marketing", 65000, "SSN456", "bob@company.com", "user"),
    (3, "Charlie", "Finance", 85000, "SSN789", "charlie@company.com", "manager"),
    (4, "Diana", "HR", 70000, "SSN012", "diana@company.com", "hr"),
    (5, "Eve", "Engineering", 80000, "SSN345", "eve@company.com", "user"),
    (6, "Frank", "Marketing", 68000, "SSN678", "frank@company.com", "user"),
    (7, "Grace", "Finance", 82000, "SSN901", "grace@company.com", "manager")
]

df_sensitive = spark.createDataFrame(sensitive_data, 
    ["id", "name", "department", "salary", "ssn", "email", "access_level"])

# Solution
print("Solution:")
def apply_security_policy(df, user_role, user_department=None):
    """Apply row and column level security based on user context"""
    
    # Column-level security
    if user_role == "admin":
        # Admin sees everything
        secured_df = df
    elif user_role == "hr":
        # HR sees all employees but no SSN
        secured_df = df.drop("ssn")
    elif user_role == "manager":
        # Managers see their department + salary info
        secured_df = df.filter(col("department") == user_department) \
                      .drop("ssn")
    else:
        # Regular users see limited info, no salary/SSN
        secured_df = df.select("id", "name", "department", "email") \
                      .filter(col("department") == user_department)
    
    return secured_df.withColumn("accessed_by", lit(user_role)) \
                    .withColumn("access_timestamp", current_timestamp())

# Demonstrate different security levels
admin_view = apply_security_policy(df_sensitive, "admin")
hr_view = apply_security_policy(df_sensitive, "hr")
manager_view = apply_security_policy(df_sensitive, "manager", "Engineering")
user_view = apply_security_policy(df_sensitive, "user", "Engineering")

print("Admin view:")
admin_view.show()
print("User view (Engineering dept):")
user_view.show()

# Challenge 78: Automated Data Discovery
print("\n78. AUTOMATED DATA DISCOVERY")
print("Problem: Implement automated data discovery and cataloging")

discovery_data = [
    ("customer_table", "id,name,email,phone,address", "PII", 1000000, "daily"),
    ("order_table", "order_id,customer_id,amount,date", "Transactional", 5000000, "realtime"),
    ("product_table", "product_id,name,category,price", "Reference", 50000, "weekly"),
    ("logs_table", "timestamp,user_id,action,details", "Behavioral", 100000000, "streaming"),
    ("financial_table", "account_id,balance,transaction_type", "Financial", 2000000, "daily")
]

df_discovery = spark.createDataFrame(discovery_data, 
    ["table_name", "columns", "data_classification", "row_count", "update_frequency"])

# Solution
print("Solution:")
df_data_catalog = df_discovery \
    .withColumn("column_list", split(col("columns"), ",")) \
    .withColumn("column_count", size(col("column_list"))) \
    .withColumn("has_pii", 
               array_contains(col("column_list"), "email") | 
               array_contains(col("column_list"), "phone")) \
    .withColumn("data_sensitivity",
               when(col("data_classification") == "PII", "High")
               .when(col("data_classification") == "Financial", "High")
               .when(col("data_classification") == "Transactional", "Medium")
               .otherwise("Low")) \
    .withColumn("storage_tier",
               when(col("row_count") > 10000000, "Hot")
               .when(col("row_count") > 1000000, "Warm")
               .otherwise("Cold")) \
    .withColumn("discovery_timestamp", current_timestamp())

# Data lineage discovery
lineage_discovery = df_data_catalog.select("table_name", "has_pii", "data_sensitivity") \
    .withColumn("upstream_dependencies", 
               when(col("table_name").contains("order"), array(lit("customer_table"), lit("product_table")))
               .when(col("table_name").contains("logs"), array(lit("customer_table")))
               .otherwise(array()))

print("Automated data catalog:")
df_data_catalog.show(truncate=False)
print("Lineage discovery:")
lineage_discovery.show(truncate=False)

# Challenge 79: Event-Driven Architecture
print("\n79. EVENT-DRIVEN ARCHITECTURE")
print("Problem: Implement event-driven data processing pipeline")

event_stream_data = [
    ("event_001", "customer_created", "customer_service", "2024-01-15 10:00:00", '{"customer_id": 1, "name": "Alice"}'),
    ("event_002", "order_placed", "order_service", "2024-01-15 10:05:00", '{"order_id": 101, "customer_id": 1, "amount": 250}'),
    ("event_003", "payment_processed", "payment_service", "2024-01-15 10:06:00", '{"order_id": 101, "status": "success"}'),
    ("event_004", "shipment_created", "logistics_service", "2024-01-15 10:30:00", '{"order_id": 101, "tracking": "TRK123"}'),
    ("event_005", "customer_updated", "customer_service", "2024-01-15 11:00:00", '{"customer_id": 1, "email": "alice@new.com"}'),
    ("event_006", "order_cancelled", "order_service", "2024-01-15 11:30:00", '{"order_id": 102, "reason": "customer_request"}'),
    ("event_007", "refund_processed", "payment_service", "2024-01-15 12:00:00", '{"order_id": 102, "amount": 150}')
]

df_events = spark.createDataFrame(event_stream_data, 
    ["event_id", "event_type", "source_service", "timestamp", "payload"])
df_events = df_events.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
# Event processing pipeline
df_event_processing = df_events \
    .withColumn("event_category",
               when(col("event_type").contains("customer"), "Customer")
               .when(col("event_type").contains("order"), "Order")
               .when(col("event_type").contains("payment"), "Payment")
               .when(col("event_type").contains("shipment"), "Logistics")
               .otherwise("Other")) \
    .withColumn("payload_parsed", from_json(col("payload"), MapType(StringType(), StringType()))) \
    .withColumn("business_entity_id", 
               coalesce(col("payload_parsed.customer_id"), 
                       col("payload_parsed.order_id"))) \
    .withColumn("event_priority",
               when(col("event_type").contains("payment"), 1)
               .when(col("event_type").contains("order"), 2)
               .otherwise(3))

# Event correlation and saga pattern
order_saga = df_event_processing.filter(col("event_category").isin(["Order", "Payment", "Logistics"])) \
    .withColumn("order_id", col("payload_parsed.order_id")) \
    .filter(col("order_id").isNotNull()) \
    .groupBy("order_id") \
    .agg(collect_list(struct("event_type", "timestamp", "source_service")).alias("event_chain"),
         count("*").alias("event_count"),
         min("timestamp").alias("saga_start"),
         max("timestamp").alias("saga_end"))

print("Event processing:")
df_event_processing.select("event_id", "event_type", "event_category", "business_entity_id").show()
print("Order saga tracking:")
order_saga.show(truncate=False)

# Challenge 80: Advanced Data Modeling
print("\n80. ADVANCED DATA MODELING")
print("Problem: Implement dimensional modeling with slowly changing dimensions")

customer_history = [
    (1, "Alice", "Basic", "NYC", "2024-01-01", "2024-02-01", False),
    (1, "Alice", "Premium", "NYC", "2024-02-01", "9999-12-31", True),
    (2, "Bob", "Standard", "LA", "2024-01-15", "2024-03-01", False),
    (2, "Bob", "Standard", "SF", "2024-03-01", "9999-12-31", True),
    (3, "Charlie", "Premium", "Chicago", "2024-01-20", "9999-12-31", True)
]

df_scd = spark.createDataFrame(customer_history, 
    ["customer_id", "name", "tier", "city", "effective_start", "effective_end", "is_current"])

# Solution
print("Solution:")
# Type 2 SCD implementation
df_dimensional_model = df_scd \
    .withColumn("effective_start", to_date(col("effective_start"))) \
    .withColumn("effective_end", to_date(col("effective_end"))) \
    .withColumn("surrogate_key", 
               concat(col("customer_id"), lit("_"), 
                     date_format(col("effective_start"), "yyyyMMdd"))) \
    .withColumn("version_number", 
               row_number().over(Window.partitionBy("customer_id").orderBy("effective_start"))) \
    .withColumn("days_active", 
               when(col("is_current"), 
                   datediff(current_date(), col("effective_start")))
               .otherwise(datediff(col("effective_end"), col("effective_start"))))

# Current state view
current_customers = df_dimensional_model.filter(col("is_current") == True)

# Historical analysis
customer_evolution = df_dimensional_model.groupBy("customer_id") \
    .agg(count("*").alias("total_versions"),
         min("effective_start").alias("first_seen"),
         max("tier").alias("highest_tier"),
         sum("days_active").alias("total_active_days"))

print("Dimensional model:")
df_dimensional_model.show()
print("Customer evolution:")
customer_evolution.show()

# Continue with remaining challenges 81-100...

# Challenge 81: Metadata Management
print("\n81. METADATA MANAGEMENT")
print("Problem: Build comprehensive metadata management system")

metadata_catalog = [
    ("customers", "operational", "customer_service", "Bronze", "PII", "daily", 1000000),
    ("orders", "transactional", "order_service", "Silver", "Business", "realtime", 5000000),
    ("products", "reference", "product_service", "Gold", "Public", "weekly", 50000),
    ("analytics_summary", "analytical", "analytics_team", "Gold", "Internal", "daily", 10000),
    ("customer_360", "reporting", "business_intel", "Platinum", "Confidential", "daily", 500000)
]

df_metadata = spark.createDataFrame(metadata_catalog, 
    ["table_name", "data_type", "owner", "quality_tier", "classification", "refresh_freq", "row_count"])

# Solution
print("Solution:")
df_metadata_enriched = df_metadata \
    .withColumn("priority_score",
               when(col("classification") == "PII", 10)
               .when(col("classification") == "Confidential", 8)
               .when(col("classification") == "Business", 6)
               .when(col("classification") == "Internal", 4)
               .otherwise(2)) \
    .withColumn("storage_cost_estimate",
               col("row_count") * 0.0001) \
    .withColumn("compliance_required",
               col("classification").isin(["PII", "Confidential"])) \
    .withColumn("backup_frequency",
               when(col("priority_score") >= 8, "Hourly")
               .when(col("priority_score") >= 6, "Daily")
               .otherwise("Weekly"))

metadata_summary = df_metadata_enriched.groupBy("owner", "quality_tier") \
    .agg(count("*").alias("table_count"),
         sum("row_count").alias("total_rows"),
         sum("storage_cost_estimate").alias("total_cost"))

print("Metadata management:")
df_metadata_enriched.show()
print("Metadata summary by owner:")
metadata_summary.show()

# Challenge 82: Advanced Partitioning Strategies
print("\n82. ADVANCED PARTITIONING STRATEGIES")
print("Problem: Implement optimal partitioning for performance")

partition_data = []
for i in range(1, 61):  # 60 records
    date = f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}"
    region = ["North", "South", "East", "West"][i%4]
    category = ["Electronics", "Clothing", "Home", "Sports"][i%4]
    partition_data.append((i, date, region, category, i*100))

# Sample for demonstration
partition_sample = partition_data[:20]
df_partition = spark.createDataFrame(partition_sample, 
    ["id", "date", "region", "category", "sales"])

# Solution
print("Solution:")
df_partition = df_partition.withColumn("date", to_date(col("date")))

# Partitioning strategy analysis
df_partition_analysis = df_partition \
    .withColumn("year_month", date_format(col("date"), "yyyy-MM")) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("region_category", concat(col("region"), lit("_"), col("category")))

# Partition size analysis
partition_sizes = df_partition_analysis.groupBy("year_month", "region") \
    .agg(count("*").alias("partition_size"),
         sum("sales").alias("partition_sales"))

# Optimal partitioning recommendation
partition_recommendation = partition_sizes \
    .withColumn("partition_efficiency",
               when(col("partition_size") > 10, "Over-partitioned")
               .when(col("partition_size") < 3, "Under-partitioned")
               .otherwise("Optimal"))

print("Partition analysis:")
partition_recommendation.show()

# Challenge 83: Real-time Feature Engineering
print("\n83. REAL-TIME FEATURE ENGINEERING")
print("Problem: Build real-time feature engineering pipeline")

realtime_features = [
    ("user_001", "login", "2024-01-15 10:00:00", "mobile", "premium"),
    ("user_001", "view_product", "2024-01-15 10:02:00", "mobile", "premium"),
    ("user_001", "add_to_cart", "2024-01-15 10:05:00", "mobile", "premium"),
    ("user_002", "login", "2024-01-15 10:01:00", "web", "standard"),
    ("user_002", "search", "2024-01-15 10:03:00", "web", "standard"),
    ("user_001", "purchase", "2024-01-15 10:08:00", "mobile", "premium"),
    ("user_003", "login", "2024-01-15 10:04:00", "tablet", "free"),
    ("user_002", "view_product", "2024-01-15 10:06:00", "web", "standard")
]

df_realtime_features = spark.createDataFrame(realtime_features, 
    ["user_id", "action", "timestamp", "platform", "tier"])
df_realtime_features = df_realtime_features.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
user_window = Window.partitionBy("user_id").orderBy("timestamp")
time_window = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-4, 0)

df_feature_engineering = df_realtime_features \
    .withColumn("session_action_count", count("*").over(time_window)) \
    .withColumn("time_since_last_action", 
               (unix_timestamp(col("timestamp")) - 
                unix_timestamp(lag("timestamp").over(user_window))) / 60) \
    .withColumn("platform_changes", 
               count(when(col("platform") != lag("platform").over(user_window), 1)).over(time_window)) \
    .withColumn("purchase_propensity", 
               when(col("action") == "add_to_cart", 0.8)
               .when(col("action") == "view_product", 0.3)
               .when(col("action") == "search", 0.2)
               .otherwise(0.1)) \
    .withColumn("user_engagement_score", 
               col("session_action_count") * col("purchase_propensity"))

# Real-time aggregations
user_features = df_feature_engineering.groupBy("user_id") \
    .agg(count("*").alias("total_actions"),
         avg("user_engagement_score").alias("avg_engagement"),
         max("timestamp").alias("last_activity"),
         countDistinct("platform").alias("platform_diversity"))

print("Real-time feature engineering:")
df_feature_engineering.select("user_id", "action", "session_action_count", 
                              "user_engagement_score").show()
print("User features:")
user_features.show()

# Challenge 84: Compliance and Audit Trail
print("\n84. COMPLIANCE AND AUDIT TRAIL")
print("Problem: Implement comprehensive audit logging")

audit_events = [
    ("user_admin", "CREATE", "customer_table", "2024-01-15 09:00:00", "SUCCESS", "system"),
    ("user_analyst", "SELECT", "customer_table", "2024-01-15 10:00:00", "SUCCESS", "query_tool"),
    ("user_admin", "UPDATE", "customer_table", "2024-01-15 11:00:00", "SUCCESS", "manual"),
    ("user_guest", "SELECT", "sensitive_data", "2024-01-15 12:00:00", "DENIED", "web_app"),
    ("user_analyst", "DELETE", "old_records", "2024-01-15 13:00:00", "SUCCESS", "scheduled"),
    ("user_admin", "ALTER", "customer_table", "2024-01-15 14:00:00", "SUCCESS", "migration"),
    ("user_guest", "CREATE", "temp_table", "2024-01-15 15:00:00", "DENIED", "query_tool")
]

df_audit = spark.createDataFrame(audit_events, 
    ["user_id", "operation", "object_name", "timestamp", "status", "source"])
df_audit = df_audit.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
df_audit_analysis = df_audit \
    .withColumn("risk_level",
               when((col("operation") == "DELETE") | (col("operation") == "ALTER"), "HIGH")
               .when((col("operation") == "UPDATE") | (col("operation") == "CREATE"), "MEDIUM")
               .otherwise("LOW")) \
    .withColumn("compliance_flag",
               when(col("status") == "DENIED", "VIOLATION")
               .when((col("risk_level") == "HIGH") & (col("source") == "manual"), "REVIEW")
               .otherwise("COMPLIANT")) \
    .withColumn("hour", hour("timestamp")) \
    .withColumn("is_business_hours", 
               col("hour").between(9, 17))

# Compliance reporting
compliance_summary = df_audit_analysis.groupBy("user_id", "risk_level") \
    .agg(count("*").alias("operation_count"),
         count(when(col("status") == "DENIED", 1)).alias("denied_count"),
         count(when(~col("is_business_hours"), 1)).alias("after_hours_count"))

# Anomaly detection
user_behavior = df_audit_analysis.groupBy("user_id") \
    .agg(count("*").alias("total_operations"),
         countDistinct("object_name").alias("objects_accessed"),
         count(when(col("risk_level") == "HIGH", 1)).alias("high_risk_ops")) \
    .withColumn("anomaly_score",
               col("high_risk_ops") * 3 + col("objects_accessed") * 0.5)

print("Audit trail analysis:")
df_audit_analysis.select("user_id", "operation", "object_name", "risk_level", "compliance_flag").show()
print("User behavior analysis:")
user_behavior.show()

# Challenge 85: Disaster Recovery Implementation
print("\n85. DISASTER RECOVERY IMPLEMENTATION")
print("Problem: Implement disaster recovery and backup strategies")

backup_status = [
    ("customer_data", "2024-01-15 02:00:00", "COMPLETED", "primary", "secondary", 1000000),
    ("order_data", "2024-01-15 02:30:00", "COMPLETED", "primary", "secondary", 5000000),
    ("product_data", "2024-01-15 03:00:00", "FAILED", "primary", "secondary", 50000),
    ("analytics_data", "2024-01-15 03:30:00", "COMPLETED", "primary", "tertiary", 2000000),
    ("logs_data", "2024-01-15 04:00:00", "IN_PROGRESS", "primary", "secondary", 10000000),
    ("config_data", "2024-01-15 04:30:00", "COMPLETED", "primary", "secondary", 1000),
    ("archive_data", "2024-01-15 05:00:00", "COMPLETED", "primary", "cold_storage", 50000000)
]

df_dr = spark.createDataFrame(backup_status, 
    ["dataset_name", "backup_time", "status", "source_location", "target_location", "record_count"])
df_dr = df_dr.withColumn("backup_time", to_timestamp(col("backup_time")))

# Solution
print("Solution:")
df_dr_analysis = df_dr \
    .withColumn("backup_age_hours", 
               (unix_timestamp(current_timestamp()) - unix_timestamp("backup_time")) / 3600) \
    .withColumn("criticality_level",
               when(col("dataset_name").contains("customer"), "CRITICAL")
               .when(col("dataset_name").contains("order"), "CRITICAL")
               .when(col("dataset_name").contains("product"), "HIGH")
               .when(col("dataset_name").contains("analytics"), "MEDIUM")
               .otherwise("LOW")) \
    .withColumn("rto_target_hours",  # Recovery Time Objective
               when(col("criticality_level") == "CRITICAL", 1)
               .when(col("criticality_level") == "HIGH", 4)
               .when(col("criticality_level") == "MEDIUM", 12)
               .otherwise(24)) \
    .withColumn("rpo_target_hours",  # Recovery Point Objective
               when(col("criticality_level") == "CRITICAL", 0.5)
               .when(col("criticality_level") == "HIGH", 2)
               .when(col("criticality_level") == "MEDIUM", 6)
               .otherwise(12)) \
    .withColumn("backup_compliance",
               when(col("backup_age_hours") > col("rpo_target_hours"), "VIOLATION")
               .when(col("status") == "FAILED", "CRITICAL")
               .otherwise("COMPLIANT"))

# DR readiness assessment
dr_readiness = df_dr_analysis.groupBy("criticality_level") \
    .agg(count("*").alias("total_datasets"),
         count(when(col("status") == "COMPLETED", 1)).alias("successful_backups"),
         avg("backup_age_hours").alias("avg_backup_age"),
         count(when(col("backup_compliance") == "VIOLATION", 1)).alias("compliance_violations"))

print("Disaster recovery status:")
df_dr_analysis.select("dataset_name", "status", "criticality_level", "backup_age_hours", "backup_compliance").show()
print("DR readiness by criticality:")
dr_readiness.show()

# Challenge 86: Advanced Analytics Platform
print("\n86. ADVANCED ANALYTICS PLATFORM")
print("Problem: Build comprehensive analytics platform")

analytics_usage = [
    ("dashboard_sales", "executive_team", "2024-01-15 09:00:00", 45, "high"),
    ("report_customer_churn", "marketing_team", "2024-01-15 09:15:00", 120, "medium"),
    ("ml_model_training", "data_science_team", "2024-01-15 09:30:00", 300, "high"),
    ("adhoc_analysis", "business_analysts", "2024-01-15 10:00:00", 60, "low"),
    ("automated_report", "operations_team", "2024-01-15 10:30:00", 15, "medium"),
    ("real_time_monitoring", "engineering_team", "2024-01-15 11:00:00", 5, "critical"),
    ("data_exploration", "product_team", "2024-01-15 11:30:00", 90, "medium")
]

df_analytics_platform = spark.createDataFrame(analytics_usage, 
    ["workload_name", "team", "start_time", "duration_minutes", "priority"])
df_analytics_platform = df_analytics_platform.withColumn("start_time", to_timestamp(col("start_time")))

# Solution
print("Solution:")
df_platform_analysis = df_analytics_platform \
    .withColumn("resource_consumption",
               col("duration_minutes") * 
               when(col("priority") == "critical", 4)
               .when(col("priority") == "high", 3)
               .when(col("priority") == "medium", 2)
               .otherwise(1)) \
    .withColumn("cost_estimate", col("resource_consumption") * 0.10) \
    .withColumn("workload_category",
               when(col("workload_name").contains("dashboard"), "Reporting")
               .when(col("workload_name").contains("ml_"), "ML/AI")
               .when(col("workload_name").contains("report"), "Analytics")
               .when(col("workload_name").contains("monitoring"), "Operations")
               .otherwise("Ad-hoc")) \
    .withColumn("peak_hour", hour("start_time").between(9, 11))

# Platform utilization metrics
team_utilization = df_platform_analysis.groupBy("team") \
    .agg(count("*").alias("workload_count"),
         sum("duration_minutes").alias("total_duration"),
         sum("cost_estimate").alias("total_cost"),
         avg("duration_minutes").alias("avg_duration"))

# Resource optimization recommendations
workload_optimization = df_platform_analysis.groupBy("workload_category") \
    .agg(avg("duration_minutes").alias("avg_duration"),
         avg("resource_consumption").alias("avg_resource_usage"),
         count("*").alias("frequency")) \
    .withColumn("optimization_opportunity",
               when(col("avg_duration") > 100, "Consider optimization")
               .when(col("avg_resource_usage") > 200, "Resource intensive")
               .otherwise("Efficient"))

print("Analytics platform usage:")
df_platform_analysis.select("workload_name", "team", "duration_minutes", "cost_estimate", "workload_category").show()
print("Team utilization:")
team_utilization.show()
print("Optimization opportunities:")
workload_optimization.show()

# Challenge 87: Data Governance Framework
print("\n87. DATA GOVERNANCE FRAMEWORK")
print("Problem: Implement comprehensive data governance")

governance_data = [
    ("customer_pii", "customers", "John Smith", "Data Steward", "HIGH", "GDPR,CCPA", "APPROVED"),
    ("financial_records", "transactions", "Jane Doe", "Compliance Officer", "CRITICAL", "SOX,PCI", "APPROVED"),
    ("product_analytics", "products", "Mike Johnson", "Product Manager", "MEDIUM", "Internal", "PENDING"),
    ("marketing_data", "campaigns", "Sarah Wilson", "Marketing Director", "MEDIUM", "CCPA", "APPROVED"),
    ("employee_data", "hr_records", "Lisa Brown", "HR Director", "HIGH", "GDPR,Local", "REVIEW"),
    ("vendor_information", "suppliers", "David Lee", "Procurement Manager", "LOW", "Internal", "APPROVED"),
    ("research_data", "experiments", "Emma Davis", "Research Lead", "MEDIUM", "Proprietary", "PENDING")
]

df_governance = spark.createDataFrame(governance_data, 
    ["data_asset", "source_table", "data_owner", "role", "sensitivity", "regulations", "approval_status"])

# Solution
print("Solution:")
df_governance_analysis = df_governance \
    .withColumn("regulation_count", size(split(col("regulations"), ","))) \
    .withColumn("compliance_complexity",
               when(col("regulation_count") > 2, "High")
               .when(col("regulation_count") > 1, "Medium")
               .otherwise("Low")) \
    .withColumn("risk_score",
               when(col("sensitivity") == "CRITICAL", 10)
               .when(col("sensitivity") == "HIGH", 7)
               .when(col("sensitivity") == "MEDIUM", 4)
               .otherwise(2)) \
    .withColumn("governance_priority",
               col("risk_score") + col("regulation_count")) \
    .withColumn("requires_audit",
               col("sensitivity").isin(["CRITICAL", "HIGH"]) | 
               col("regulations").contains("SOX"))

# Governance metrics
governance_summary = df_governance_analysis.groupBy("approval_status", "sensitivity") \
    .agg(count("*").alias("asset_count"),
         avg("risk_score").alias("avg_risk_score"))

data_steward_workload = df_governance_analysis.groupBy("data_owner") \
    .agg(count("*").alias("managed_assets"),
         sum("governance_priority").alias("total_priority_score"),
         count(when(col("requires_audit"), 1)).alias("audit_required_assets"))

print("Data governance analysis:")
df_governance_analysis.select("data_asset", "data_owner", "sensitivity", "compliance_complexity", "governance_priority").show()
print("Governance summary:")
governance_summary.show()
print("Data steward workload:")
data_steward_workload.show()

# Challenge 88: Federated Query Processing
print("\n88. FEDERATED QUERY PROCESSING")
print("Problem: Query across multiple distributed data sources")

federated_sources = [
    ("source_A", "mysql", "customer_data", "customers", 1000000, "us-east-1"),
    ("source_B", "postgres", "order_data", "orders", 5000000, "us-west-2"),
    ("source_C", "mongodb", "product_catalog", "products", 50000, "eu-west-1"),
    ("source_D", "cassandra", "user_sessions", "sessions", 10000000, "ap-south-1"),
    ("source_E", "redis", "real_time_cache", "cache", 100000, "us-east-1"),
    ("source_F", "snowflake", "analytics_warehouse", "warehouse", 20000000, "us-central-1"),
    ("source_G", "s3", "data_lake", "raw_data", 100000000, "us-east-1")
]

df_federated = spark.createDataFrame(federated_sources, 
    ["source_id", "technology", "dataset_name", "table_name", "record_count", "region"])

# Solution
print("Solution:")
df_federation_analysis = df_federated \
    .withColumn("query_complexity",
               when(col("technology").isin(["mysql", "postgres"]), "SQL")
               .when(col("technology") == "mongodb", "NoSQL")
               .when(col("technology") == "cassandra", "Wide-Column")
               .when(col("technology") == "redis", "Key-Value")
               .otherwise("Cloud-Native")) \
    .withColumn("latency_estimate_ms",
               when(col("region") == "us-east-1", 10)
               .when(col("region").startswith("us-"), 50)
               .when(col("region").startswith("eu-"), 100)
               .otherwise(150)) \
    .withColumn("federation_cost",
               col("record_count") * col("latency_estimate_ms") * 0.000001) \
    .withColumn("join_compatibility",
               when(col("query_complexity") == "SQL", "High")
               .when(col("query_complexity") == "NoSQL", "Medium")
               .otherwise("Low"))

# Cross-source query planning
query_plan = df_federation_analysis.groupBy("region", "query_complexity") \
    .agg(count("*").alias("source_count"),
         sum("record_count").alias("total_records"),
         avg("latency_estimate_ms").alias("avg_latency"))

# Federation optimization
federation_recommendations = df_federation_analysis \
    .withColumn("optimization_strategy",
               when(col("federation_cost") > 100, "Cache locally")
               .when(col("latency_estimate_ms") > 100, "Use async processing")
               .when(col("join_compatibility") == "Low", "Pre-aggregate")
               .otherwise("Direct federation"))

print("Federated query analysis:")
federation_recommendations.select("source_id", "technology", "latency_estimate_ms", "optimization_strategy").show()
print("Query planning by region:")
query_plan.show()

# Challenge 89: Advanced Data Observability
print("\n89. ADVANCED DATA OBSERVABILITY")
print("Problem: Implement comprehensive data observability")

observability_metrics = [
    ("pipeline_customer_etl", "2024-01-15 08:00:00", 120, 1000000, 0, "SUCCESS", 95.5),
    ("pipeline_order_streaming", "2024-01-15 08:05:00", 300, 5000000, 2, "SUCCESS", 98.2),
    ("pipeline_product_batch", "2024-01-15 08:10:00", 180, 50000, 0, "SUCCESS", 99.1),
    ("pipeline_analytics_ml", "2024-01-15 08:15:00", 600, 2000000, 1, "WARNING", 87.3),
    ("pipeline_customer_etl", "2024-01-15 09:00:00", 125, 1000500, 0, "SUCCESS", 94.8),
    ("pipeline_order_streaming", "2024-01-15 09:05:00", 280, 4800000, 0, "SUCCESS", 98.7),
    ("pipeline_product_batch", "2024-01-15 09:10:00", 200, 49500, 1, "WARNING", 96.5),
    ("pipeline_analytics_ml", "2024-01-15 09:15:00", 650, 1950000, 3, "FAILED", 75.2)
]

df_observability = spark.createDataFrame(observability_metrics, 
    ["pipeline_name", "execution_time", "duration_seconds", "records_processed", "error_count", "status", "quality_score"])
df_observability = df_observability.withColumn("execution_time", to_timestamp(col("execution_time")))

# Solution
print("Solution:")
pipeline_window = Window.partitionBy("pipeline_name").orderBy("execution_time")

df_observability_analysis = df_observability \
    .withColumn("prev_duration", lag("duration_seconds").over(pipeline_window)) \
    .withColumn("duration_trend", 
               ((col("duration_seconds") - col("prev_duration")) / col("prev_duration") * 100)) \
    .withColumn("throughput_rps", col("records_processed") / col("duration_seconds")) \
    .withColumn("error_rate", col("error_count") / col("records_processed") * 100) \
    .withColumn("sla_violation", 
               col("duration_seconds") > 
               when(col("pipeline_name").contains("streaming"), 360)
               .when(col("pipeline_name").contains("batch"), 240)
               .otherwise(480)) \
    .withColumn("alert_level",
               when(col("status") == "FAILED", "CRITICAL")
               .when(col("quality_score") < 90, "HIGH")
               .when(col("sla_violation"), "MEDIUM")
               .otherwise("LOW"))

# Observability dashboard metrics
pipeline_health = df_observability_analysis.groupBy("pipeline_name") \
    .agg(count("*").alias("total_runs"),
         avg("duration_seconds").alias("avg_duration"),
         avg("quality_score").alias("avg_quality"),
         sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failure_count"),
         sum(when(col("sla_violation"), 1).otherwise(0)).alias("sla_violations"))

# Anomaly detection
anomaly_detection = df_observability_analysis \
    .withColumn("duration_anomaly", 
               abs(col("duration_trend")) > 20) \
    .withColumn("quality_anomaly", 
               col("quality_score") < 85) \
    .withColumn("throughput_anomaly", 
               col("throughput_rps") < 1000)

print("Data observability analysis:")
df_observability_analysis.select("pipeline_name", "duration_seconds", "quality_score", "alert_level", "sla_violation").show()
print("Pipeline health metrics:")
pipeline_health.show()

# Challenge 90: Multi-Cloud Data Strategy
print("\n90. MULTI-CLOUD DATA STRATEGY")
print("Problem: Implement multi-cloud data management strategy")

multicloud_deployment = [
    ("customer_data", "aws", "s3", "us-east-1", 10000, 500.0, "primary"),
    ("customer_data", "azure", "blob", "eastus", 10000, 450.0, "backup"),
    ("analytics_data", "gcp", "bigquery", "us-central1", 50000, 800.0, "primary"),
    ("analytics_data", "aws", "redshift", "us-west-2", 50000, 750.0, "secondary"),
    ("streaming_data", "aws", "kinesis", "us-east-1", 1000000, 1200.0, "primary"),
    ("ml_models", "azure", "ml_studio", "westus2", 100, 300.0, "primary"),
    ("backup_data", "gcp", "coldline", "us-west1", 100000, 50.0, "archive"),
    ("real_time_cache", "aws", "elasticache", "us-east-1", 10000, 200.0, "primary")
]

df_multicloud = spark.createDataFrame(multicloud_deployment, 
    ["dataset", "cloud_provider", "service", "region", "data_size_gb", "monthly_cost", "role"])

# Solution
print("Solution:")
df_multicloud_strategy = df_multicloud \
    .withColumn("cost_per_gb", col("monthly_cost") / col("data_size_gb")) \
    .withColumn("vendor_lock_risk",
               when(col("cloud_provider") == "aws", 
                   count("*").over(Window.partitionBy(lit(1))) == 
                   count(when(col("cloud_provider") == "aws", 1)).over(Window.partitionBy(lit(1))))
               .otherwise(False)) \
    .withColumn("data_sovereignty",
               when(col("region").startswith("eu-"), "EU")
               .when(col("region").startswith("us-"), "US")
               .when(col("region").startswith("ap-"), "APAC")
               .otherwise("Other")) \
    .withColumn("redundancy_level",
               count("*").over(Window.partitionBy("dataset")))

# Multi-cloud analytics
cloud_cost_analysis = df_multicloud_strategy.groupBy("cloud_provider") \
    .agg(count("*").alias("service_count"),
         sum("monthly_cost").alias("total_cost"),
         avg("cost_per_gb").alias("avg_cost_per_gb"),
         sum("data_size_gb").alias("total_data_gb"))

# Risk assessment
risk_assessment = df_multicloud_strategy.groupBy("dataset") \
    .agg(countDistinct("cloud_provider").alias("provider_diversity"),
         sum("monthly_cost").alias("dataset_cost"),
         first("redundancy_level").alias("redundancy"))

print("Multi-cloud strategy analysis:")
df_multicloud_strategy.select("dataset", "cloud_provider", "cost_per_gb", "data_sovereignty", "redundancy_level").show()
print("Cloud cost analysis:")
cloud_cost_analysis.show()
print("Risk assessment:")
risk_assessment.show()

# Challenge 91: Advanced Data Encryption
print("\n91. ADVANCED DATA ENCRYPTION")
print("Problem: Implement comprehensive data encryption strategy")

encryption_inventory = [
    ("customer_pii", "AES-256", "at_rest", "HSM", "column_level", "HIGH"),
    ("financial_data", "AES-256", "in_transit", "TLS_1.3", "field_level", "CRITICAL"),
    ("analytics_data", "AES-128", "at_rest", "cloud_kms", "table_level", "MEDIUM"),
    ("logs_data", "None", "none", "none", "none", "LOW"),
    ("backup_data", "AES-256", "at_rest", "local_key", "volume_level", "HIGH"),
    ("temp_data", "AES-128", "at_rest", "cloud_kms", "database_level", "LOW"),
    ("archive_data", "AES-256", "at_rest", "HSM", "file_level", "MEDIUM"),
    ("real_time_stream", "AES-256", "in_transit", "mutual_tls", "message_level", "HIGH")
]

df_encryption = spark.createDataFrame(encryption_inventory, 
    ["data_type", "algorithm", "encryption_state", "key_management", "granularity", "sensitivity"])

# Solution
print("Solution:")
df_encryption_analysis = df_encryption \
    .withColumn("encryption_strength",
               when(col("algorithm") == "AES-256", 10)
               .when(col("algorithm") == "AES-128", 7)
               .otherwise(0)) \
    .withColumn("key_security_score",
               when(col("key_management") == "HSM", 10)
               .when(col("key_management") == "cloud_kms", 8)
               .when(col("key_management") == "TLS_1.3", 9)
               .when(col("key_management") == "mutual_tls", 8)
               .otherwise(3)) \
    .withColumn("granularity_score",
               when(col("granularity") == "field_level", 10)
               .when(col("granularity") == "column_level", 9)
               .when(col("granularity") == "message_level", 8)
               .when(col("granularity") == "table_level", 6)
               .otherwise(3)) \
    .withColumn("overall_security_score",
               (col("encryption_strength") + col("key_security_score") + col("granularity_score")) / 3) \
    .withColumn("compliance_status",
               when((col("sensitivity") == "CRITICAL") & (col("overall_security_score") < 9), "NON_COMPLIANT")
               .when((col("sensitivity") == "HIGH") & (col("overall_security_score") < 8), "NON_COMPLIANT")
               .otherwise("COMPLIANT"))

# Security gaps analysis
security_gaps = df_encryption_analysis.filter(col("compliance_status") == "NON_COMPLIANT")
encryption_summary = df_encryption_analysis.groupBy("sensitivity", "compliance_status") \
    .agg(count("*").alias("data_type_count"),
         avg("overall_security_score").alias("avg_security_score"))

print("Encryption analysis:")
df_encryption_analysis.select("data_type", "sensitivity", "overall_security_score", "compliance_status").show()
print("Security gaps:")
security_gaps.select("data_type", "sensitivity", "overall_security_score").show()

# Challenge 92: Automated Data Classification
print("\n92. AUTOMATED DATA CLASSIFICATION")
print("Problem: Implement automated data classification system")

data_samples = [
    ("customers", "email", "john.doe@company.com", "string", 95),
    ("customers", "ssn", "123-45-6789", "string", 100),
    ("customers", "name", "John Doe", "string", 85),
    ("customers", "age", "35", "integer", 10),
    ("orders", "credit_card", "4532-1234-5678-9012", "string", 98),
    ("orders", "amount", "250.75", "decimal", 5),
    ("orders", "order_id", "ORD-12345", "string", 15),
    ("products", "product_name", "Laptop Computer", "string", 20),
    ("products", "price", "1299.99", "decimal", 5),
    ("employees", "phone", "555-123-4567", "string", 80),
    ("employees", "salary", "75000", "integer", 60),
    ("employees", "employee_id", "EMP001", "string", 10)
]

df_classification = spark.createDataFrame(data_samples, 
    ["table_name", "column_name", "sample_value", "data_type", "sensitivity_score"])

# Solution
print("Solution:")
df_auto_classification = df_classification \
    .withColumn("contains_email", col("sample_value").rlike(".*@.*\\..*")) \
    .withColumn("contains_ssn", col("sample_value").rlike("\\d{3}-\\d{2}-\\d{4}")) \
    .withColumn("contains_phone", col("sample_value").rlike("\\d{3}-\\d{3}-\\d{4}")) \
    .withColumn("contains_credit_card", col("sample_value").rlike("\\d{4}-\\d{4}-\\d{4}-\\d{4}")) \
    .withColumn("auto_classification",
               when(col("contains_email"), "PII_EMAIL")
               .when(col("contains_ssn"), "PII_SSN")
               .when(col("contains_phone"), "PII_PHONE")
               .when(col("contains_credit_card"), "PII_PAYMENT")
               .when((col("column_name").contains("salary")) | (col("column_name").contains("amount")), "FINANCIAL")
               .when(col("column_name").contains("name"), "PII_NAME")
               .when(col("sensitivity_score") > 50, "SENSITIVE")
               .otherwise("PUBLIC")) \
    .withColumn("recommended_encryption",
               when(col("auto_classification").startswith("PII"), "AES-256")
               .when(col("auto_classification") == "FINANCIAL", "AES-256")
               .when(col("auto_classification") == "SENSITIVE", "AES-128")
               .otherwise("None")) \
    .withColumn("data_masking_required",
               col("auto_classification").startswith("PII") | 
               (col("auto_classification") == "FINANCIAL"))

# Classification summary
classification_summary = df_auto_classification.groupBy("table_name", "auto_classification") \
    .agg(count("*").alias("column_count"))

# Data protection recommendations
protection_recommendations = df_auto_classification.groupBy("auto_classification") \
    .agg(count("*").alias("total_columns"),
         first("recommended_encryption").alias("encryption_type"),
         count(when(col("data_masking_required"), 1)).alias("masking_required_count"))

print("Automated data classification:")
df_auto_classification.select("table_name", "column_name", "auto_classification", "recommended_encryption").show()
print("Classification summary:")
classification_summary.show()
print("Protection recommendations:")
protection_recommendations.show()

# Challenge 93: Data Mesh Governance
print("\n93. DATA MESH GOVERNANCE")
print("Problem: Implement governance for data mesh architecture")

data_mesh_domains = [
    ("customer_domain", "Customer_Team", "customer_360", "operational", "high", "daily", 1000000),
    ("customer_domain", "Customer_Team", "customer_segments", "analytical", "medium", "weekly", 500000),
    ("order_domain", "Order_Team", "order_processing", "operational", "high", "realtime", 5000000),
    ("order_domain", "Order_Team", "order_analytics", "analytical", "medium", "daily", 2000000),
    ("product_domain", "Product_Team", "product_catalog", "operational", "medium", "daily", 50000),
    ("product_domain", "Product_Team", "product_recommendations", "analytical", "low", "hourly", 100000),
    ("finance_domain", "Finance_Team", "billing_data", "operational", "critical", "realtime", 3000000),
    ("finance_domain", "Finance_Team", "financial_reports", "reporting", "critical", "daily", 100000)
]

df_mesh_governance = spark.createDataFrame(data_mesh_domains, 
    ["domain", "owner_team", "data_product", "product_type", "criticality", "refresh_frequency", "volume"])

# Solution
print("Solution:")
df_mesh_governance_analysis = df_mesh_governance \
    .withColumn("governance_tier",
               when(col("criticality") == "critical", "Tier_1")
               .when(col("criticality") == "high", "Tier_2")
               .when(col("criticality") == "medium", "Tier_3")
               .otherwise("Tier_4")) \
    .withColumn("sla_requirement_hours",
               when(col("refresh_frequency") == "realtime", 0.25)
               .when(col("refresh_frequency") == "hourly", 1)
               .when(col("refresh_frequency") == "daily", 24)
               .otherwise(168)) \
    .withColumn("data_steward_required",
               col("criticality").isin(["critical", "high"])) \
    .withColumn("automated_testing_required",
               col("product_type").isin(["operational", "reporting"])) \
    .withColumn("domain_coupling_score",
               count("*").over(Window.partitionBy("domain"))) \
    .withColumn("governance_complexity",
               when(col("domain_coupling_score") > 3, "High")
               .when(col("domain_coupling_score") > 1, "Medium")
               .otherwise("Low"))

# Domain governance metrics
domain_governance_summary = df_mesh_governance_analysis.groupBy("domain", "owner_team") \
    .agg(count("*").alias("product_count"),
         sum("volume").alias("total_data_volume"),
         count(when(col("data_steward_required"), 1)).alias("steward_required_count"),
         first("governance_complexity").alias("complexity"))

# Cross-domain dependencies
cross_domain_analysis = df_mesh_governance_analysis.select("domain", "data_product", "product_type") \
    .withColumn("potential_consumers",
               when(col("product_type") == "operational", 
                   array(lit("analytical_teams"), lit("reporting_teams")))
               .when(col("product_type") == "analytical",
                   array(lit("ml_teams"), lit("dashboard_teams")))
               .otherwise(array(lit("executive_teams"))))

print("Data mesh governance analysis:")
df_mesh_governance_analysis.select("domain", "data_product", "governance_tier", "sla_requirement_hours", "governance_complexity").show()
print("Domain governance summary:")
domain_governance_summary.show()

# Challenge 94: Edge Computing Analytics
print("\n94. EDGE COMPUTING ANALYTICS")
print("Problem: Implement analytics at edge locations")

edge_analytics_data = [
    ("edge_NYC", "retail_store", "foot_traffic", "2024-01-15 10:00:00", 145, 2.3),
    ("edge_NYC", "retail_store", "temperature", "2024-01-15 10:00:00", 72, 0.1),
    ("edge_LA", "warehouse", "inventory_scan", "2024-01-15 10:01:00", 1250, 1.8),
    ("edge_LA", "warehouse", "equipment_temp", "2024-01-15 10:01:00", 68, 0.2),
    ("edge_Chicago", "manufacturing", "production_rate", "2024-01-15 10:02:00", 89, 3.2),
    ("edge_Chicago", "manufacturing", "quality_score", "2024-01-15 10:02:00", 96, 0.5),
    ("edge_Miami", "logistics", "truck_location", "2024-01-15 10:03:00", 25, 1.1),
    ("edge_Miami", "logistics", "fuel_level", "2024-01-15 10:03:00", 78, 0.3)
]

df_edge_analytics = spark.createDataFrame(edge_analytics_data, 
    ["edge_location", "facility_type", "metric_type", "timestamp", "value", "latency_ms"])
df_edge_analytics = df_edge_analytics.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
df_edge_processing = df_edge_analytics \
    .withColumn("edge_region", 
               when(col("edge_location").contains("NYC"), "Northeast")
               .when(col("edge_location").contains("LA"), "West")
               .when(col("edge_location").contains("Chicago"), "Central")
               .otherwise("Southeast")) \
    .withColumn("processing_tier",
               when(col("latency_ms") < 1.0, "Ultra_Low_Latency")
               .when(col("latency_ms") < 2.0, "Low_Latency")
               .when(col("latency_ms") < 3.0, "Medium_Latency")
               .otherwise("High_Latency")) \
    .withColumn("edge_analytics_type",
               when(col("metric_type").contains("traffic"), "Computer_Vision")
               .when(col("metric_type").contains("temp"), "IoT_Sensor")
               .when(col("metric_type").contains("rate"), "Operational_Analytics")
               .otherwise("Location_Analytics")) \
    .withColumn("cloud_sync_required",
               when(col("facility_type") == "manufacturing", True)
               .when(col("value") > 100, True)
               .otherwise(False))

# Edge performance analysis
edge_performance = df_edge_processing.groupBy("edge_location", "facility_type") \
    .agg(count("*").alias("metric_count"),
         avg("latency_ms").alias("avg_latency"),
         avg("value").alias("avg_metric_value"),
         count(when(col("cloud_sync_required"), 1)).alias("sync_required_count"))

# Regional analytics
regional_summary = df_edge_processing.groupBy("edge_region", "processing_tier") \
    .agg(count("*").alias("measurement_count"),
         countDistinct("edge_location").alias("edge_node_count"))

print("Edge computing analytics:")
df_edge_processing.select("edge_location", "metric_type", "latency_ms", "processing_tier", "edge_analytics_type").show()
print("Edge performance:")
edge_performance.show()
print("Regional summary:")
regional_summary.show()

# Challenge 95: Graph Processing (Enhanced)
print("\n95. GRAPH PROCESSING (ENHANCED)")
print("Problem: Advanced graph analytics for complex networks")

graph_network_data = [
    (1, 2, "follows", 0.8, 5, "social"),
    (1, 3, "collaborates", 0.9, 12, "professional"),
    (2, 4, "friends", 0.7, 8, "social"),
    (3, 4, "colleagues", 0.8, 6, "professional"),
    (4, 5, "mentors", 0.9, 15, "professional"),
    (5, 6, "partners", 1.0, 20, "business"),
    (2, 6, "clients", 0.6, 3, "business"),
    (1, 5, "advised_by", 0.7, 4, "professional"),
    (3, 6, "vendors", 0.5, 2, "business"),
    (4, 6, "customers", 0.6, 7, "business")
]

df_graph_network = spark.createDataFrame(graph_network_data, 
    ["source_id", "target_id", "relationship_type", "strength", "frequency", "context"])

# Solution
print("Solution:")
# Enhanced graph analytics
df_graph_analysis = df_graph_network \
    .withColumn("edge_weight", col("strength") * col("frequency")) \
    .withColumn("relationship_category",
               when(col("context") == "social", 1)
               .when(col("context") == "professional", 2)
               .otherwise(3))

# Calculate centrality measures
out_degree = df_graph_analysis.groupBy("source_id") \
    .agg(count("*").alias("out_degree"),
         sum("edge_weight").alias("out_weight"),
         avg("strength").alias("avg_influence"))

in_degree = df_graph_analysis.groupBy("target_id") \
    .agg(count("*").alias("in_degree"),
         sum("edge_weight").alias("in_weight"),
         avg("strength").alias("avg_reputation"))

# Node importance calculation
node_importance = out_degree.alias("out") \
    .join(in_degree.alias("in"), col("out.source_id") == col("in.target_id"), "full_outer") \
    .select(coalesce(col("out.source_id"), col("in.target_id")).alias("node_id"),
            coalesce(col("out_degree"), lit(0)).alias("out_degree"),
            coalesce(col("in_degree"), lit(0)).alias("in_degree"),
            coalesce(col("out_weight"), lit(0.0)).alias("out_weight"),
            coalesce(col("in_weight"), lit(0.0)).alias("in_weight")) \
    .withColumn("total_degree", col("out_degree") + col("in_degree")) \
    .withColumn("centrality_score", 
               (col("out_weight") + col("in_weight")) / (col("total_degree") + 1))

# Community detection (simplified clustering)
strong_connections = df_graph_analysis.filter(col("strength") > 0.7)
communities = strong_connections.groupBy("source_id") \
    .agg(collect_set("target_id").alias("strong_connections"),
         collect_set("context").alias("connection_contexts"),
         count("*").alias("strong_connection_count"))

print("Graph network analysis:")
df_graph_analysis.select("source_id", "target_id", "relationship_type", "edge_weight", "context").show()
print("Node importance:")
node_importance.orderBy(desc("centrality_score")).show()
print("Communities (strong connections):")
communities.show(truncate=False)

# Challenge 96: Quantum-Safe Cryptography
print("\n96. QUANTUM-SAFE CRYPTOGRAPHY")
print("Problem: Implement quantum-resistant encryption strategies")

quantum_safe_data = [
    ("financial_transactions", "CRYSTALS-Kyber", "post_quantum", "NIST_approved", "critical"),
    ("customer_pii", "CRYSTALS-Dilithium", "post_quantum", "NIST_approved", "high"),
    ("iot_sensor_data", "SPHINCS+", "post_quantum", "NIST_approved", "medium"),
    ("legacy_system_data", "RSA-2048", "classical", "deprecated", "high"),
    ("internal_logs", "AES-256", "symmetric", "current", "low"),
    ("blockchain_data", "FALCON", "post_quantum", "NIST_approved", "high"),
    ("backup_archives", "ChaCha20-Poly1305", "symmetric", "current", "medium"),
    ("real_time_streams", "Kyber768", "post_quantum", "experimental", "medium")
]

df_quantum_safe = spark.createDataFrame(quantum_safe_data, 
    ["data_category", "encryption_algorithm", "crypto_type", "standard_status", "sensitivity"])

# Solution
print("Solution:")
df_quantum_readiness = df_quantum_safe \
    .withColumn("quantum_resistance_score",
               when(col("crypto_type") == "post_quantum", 10)
               .when(col("crypto_type") == "symmetric", 7)
               .otherwise(2)) \
    .withColumn("standardization_score",
               when(col("standard_status") == "NIST_approved", 10)
               .when(col("standard_status") == "current", 8)
               .when(col("standard_status") == "experimental", 5)
               .otherwise(1)) \
    .withColumn("migration_priority",
               when((col("sensitivity") == "critical") & (col("quantum_resistance_score") < 8), "URGENT")
               .when((col("sensitivity") == "high") & (col("quantum_resistance_score") < 8), "HIGH")
               .when(col("quantum_resistance_score") < 5, "MEDIUM")
               .otherwise("LOW")) \
    .withColumn("overall_security_score",
               (col("quantum_resistance_score") + col("standardization_score")) / 2) \
    .withColumn("quantum_ready", col("overall_security_score") >= 8)

# Migration planning
migration_plan = df_quantum_readiness.groupBy("migration_priority") \
    .agg(count("*").alias("system_count"),
         collect_list("data_category").alias("affected_systems"),
         avg("overall_security_score").alias("avg_security_score"))

# Risk assessment
risk_assessment = df_quantum_readiness.filter(~col("quantum_ready")) \
    .select("data_category", "encryption_algorithm", "sensitivity", "migration_priority")

print("Quantum-safe cryptography analysis:")
df_quantum_readiness.select("data_category", "encryption_algorithm", "quantum_resistance_score", "migration_priority", "quantum_ready").show()
print("Migration planning:")
migration_plan.show(truncate=False)
print("High-risk systems requiring immediate attention:")
risk_assessment.show()

# Challenge 97: Autonomous Data Management
print("\n97. AUTONOMOUS DATA MANAGEMENT")
print("Problem: Implement self-managing data systems")

autonomous_metrics = [
    ("storage_optimization", "auto_tiering", "2024-01-15 08:00:00", "completed", 25.5, "cost_saved"),
    ("query_optimization", "index_tuning", "2024-01-15 08:15:00", "running", 15.8, "performance_gain"),
    ("data_quality", "anomaly_detection", "2024-01-15 08:30:00", "completed", 8.2, "quality_improvement"),
    ("capacity_planning", "auto_scaling", "2024-01-15 08:45:00", "completed", 40.0, "resource_efficiency"),
    ("backup_optimization", "intelligent_backup", "2024-01-15 09:00:00", "failed", -5.0, "failure_cost"),
    ("security_patching", "auto_patch", "2024-01-15 09:15:00", "completed", 12.3, "security_improvement"),
    ("data_archival", "lifecycle_mgmt", "2024-01-15 09:30:00", "running", 18.7, "storage_efficiency"),
    ("performance_tuning", "query_rewrite", "2024-01-15 09:45:00", "completed", 22.1, "response_time_improvement")
]

df_autonomous = spark.createDataFrame(autonomous_metrics, 
    ["task_category", "automation_type", "execution_time", "status", "impact_value", "impact_type"])
df_autonomous = df_autonomous.withColumn("execution_time", to_timestamp(col("execution_time")))

# Solution
print("Solution:")
df_autonomous_analysis = df_autonomous \
    .withColumn("automation_maturity",
               when(col("task_category").contains("optimization"), "Advanced")
               .when(col("task_category").contains("quality"), "Intermediate")
               .when(col("task_category").contains("security"), "Basic")
               .otherwise("Experimental")) \
    .withColumn("success_rate_weight",
               when(col("status") == "completed", 1.0)
               .when(col("status") == "running", 0.5)
               .otherwise(0.0)) \
    .withColumn("roi_impact",
               when(col("impact_type").contains("cost"), col("impact_value") * 1000)
               .when(col("impact_type").contains("performance"), col("impact_value") * 500)
               .when(col("impact_type").contains("efficiency"), col("impact_value") * 300)
               .otherwise(col("impact_value") * 100)) \
    .withColumn("automation_confidence",
               when(col("automation_maturity") == "Advanced", 0.9)
               .when(col("automation_maturity") == "Intermediate", 0.7)
               .when(col("automation_maturity") == "Basic", 0.5)
               .otherwise(0.3))

# Autonomous system performance
autonomous_summary = df_autonomous_analysis.groupBy("automation_maturity", "status") \
    .agg(count("*").alias("task_count"),
         avg("impact_value").alias("avg_impact"),
         sum("roi_impact").alias("total_roi"))

# Recommendation engine for autonomous actions
autonomous_recommendations = df_autonomous_analysis \
    .withColumn("recommendation",
               when((col("automation_confidence") > 0.8) & (col("roi_impact") > 10000), "Expand automation")
               .when((col("status") == "failed") & (col("automation_confidence") > 0.5), "Review and retry")
               .when(col("automation_confidence") < 0.4, "Manual oversight required")
               .otherwise("Continue monitoring"))

print("Autonomous data management analysis:")
df_autonomous_analysis.select("task_category", "automation_type", "status", "roi_impact", "automation_confidence").show()
print("Autonomous system performance:")
autonomous_summary.show()
print("Recommendations:")
autonomous_recommendations.select("task_category", "recommendation").distinct().show()

# Challenge 98: Digital Twin Analytics
print("\n98. DIGITAL TWIN ANALYTICS")
print("Problem: Implement digital twin for infrastructure monitoring")

digital_twin_data = [
    ("server_rack_01", "physical", "cpu_usage", "2024-01-15 10:00:00", 75.2, 85.0),
    ("server_rack_01", "digital", "cpu_usage", "2024-01-15 10:00:00", 74.8, 85.0),
    ("server_rack_01", "physical", "memory_usage", "2024-01-15 10:00:00", 68.5, 80.0),
    ("server_rack_01", "digital", "memory_usage", "2024-01-15 10:00:00", 69.1, 80.0),
    ("network_switch_02", "physical", "throughput", "2024-01-15 10:01:00", 450.2, 500.0),
    ("network_switch_02", "digital", "throughput", "2024-01-15 10:01:00", 448.7, 500.0),
    ("storage_array_03", "physical", "iops", "2024-01-15 10:02:00", 12500, 15000),
    ("storage_array_03", "digital", "iops", "2024-01-15 10:02:00", 12650, 15000)
]

df_digital_twin = spark.createDataFrame(digital_twin_data, 
    ["asset_id", "twin_type", "metric_name", "timestamp", "actual_value", "capacity"])
df_digital_twin = df_digital_twin.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
# Calculate twin accuracy and predictions
physical_twin = df_digital_twin.filter(col("twin_type") == "physical") \
    .select("asset_id", "metric_name", "timestamp", 
           col("actual_value").alias("physical_value"), "capacity")

digital_twin = df_digital_twin.filter(col("twin_type") == "digital") \
    .select("asset_id", "metric_name", "timestamp", 
           col("actual_value").alias("predicted_value"))

df_twin_analysis = physical_twin.join(digital_twin, 
    ["asset_id", "metric_name", "timestamp"], "inner") \
    .withColumn("prediction_accuracy", 
               1 - abs(col("physical_value") - col("predicted_value")) / col("physical_value")) \
    .withColumn("utilization_pct", 
               col("physical_value") / col("capacity") * 100) \
    .withColumn("capacity_alert", 
               col("utilization_pct") > 80) \
    .withColumn("prediction_drift", 
               abs(col("physical_value") - col("predicted_value"))) \
    .withColumn("twin_health_score", 
               col("prediction_accuracy") * 100)

# Predictive maintenance indicators
maintenance_indicators = df_twin_analysis \
    .withColumn("maintenance_urgency",
               when(col("utilization_pct") > 90, "CRITICAL")
               .when(col("utilization_pct") > 80, "HIGH")
               .when(col("twin_health_score") < 85, "MONITOR")
               .otherwise("NORMAL")) \
    .withColumn("estimated_failure_hours",
               when(col("utilization_pct") > 95, 24)
               .when(col("utilization_pct") > 90, 72)
               .when(col("utilization_pct") > 85, 168)
               .otherwise(720))

# Digital twin performance metrics
twin_performance = df_twin_analysis.groupBy("asset_id") \
    .agg(avg("prediction_accuracy").alias("avg_accuracy"),
         avg("utilization_pct").alias("avg_utilization"),
         max("prediction_drift").alias("max_drift"),
         count(when(col("capacity_alert"), 1)).alias("alert_count"))

print("Digital twin analytics:")
df_twin_analysis.select("asset_id", "metric_name", "physical_value", "predicted_value", "prediction_accuracy", "utilization_pct").show()
print("Maintenance indicators:")
maintenance_indicators.select("asset_id", "metric_name", "maintenance_urgency", "estimated_failure_hours").show()
print("Twin performance summary:")
twin_performance.show()

# Challenge 99: Sustainable Data Computing
print("\n99. SUSTAINABLE DATA COMPUTING")
print("Problem: Optimize for environmental sustainability")

sustainability_metrics = [
    ("data_center_A", "compute_cluster", "2024-01-15 10:00:00", 150.5, 45.2, 75, "renewable"),
    ("data_center_A", "storage_cluster", "2024-01-15 10:00:00", 85.3, 28.7, 60, "renewable"),
    ("data_center_B", "compute_cluster", "2024-01-15 10:00:00", 200.8, 78.3, 85, "mixed"),
    ("data_center_B", "gpu_cluster", "2024-01-15 10:00:00", 450.2, 180.1, 95, "mixed"),
    ("edge_location_C", "mini_cluster", "2024-01-15 10:00:00", 25.7, 8.9, 45, "grid"),
    ("cloud_region_D", "serverless", "2024-01-15 10:00:00", 75.4, 15.2, 35, "renewable"),
    ("data_center_A", "network_equipment", "2024-01-15 10:00:00", 30.2, 12.1, 50, "renewable"),
    ("data_center_B", "cooling_system", "2024-01-15 10:00:00", 120.6, 45.8, 70, "mixed")
]

df_sustainability = spark.createDataFrame(sustainability_metrics, 
    ["location", "resource_type", "timestamp", "power_consumption_kw", "carbon_footprint_kg", "efficiency_score", "energy_source"])
df_sustainability = df_sustainability.withColumn("timestamp", to_timestamp(col("timestamp")))

# Solution
print("Solution:")
df_green_computing = df_sustainability \
    .withColumn("carbon_intensity", 
               col("carbon_footprint_kg") / col("power_consumption_kw")) \
    .withColumn("energy_source_score",
               when(col("energy_source") == "renewable", 10)
               .when(col("energy_source") == "mixed", 6)
               .otherwise(3)) \
    .withColumn("sustainability_score", 
               (col("efficiency_score") + col("energy_source_score")) / 2) \
    .withColumn("green_rating",
               when(col("sustainability_score") > 8, "Excellent")
               .when(col("sustainability_score") > 6, "Good")
               .when(col("sustainability_score") > 4, "Fair")
               .otherwise("Poor")) \
    .withColumn("optimization_potential",
               when(col("carbon_intensity") > 0.5, "High")
               .when(col("carbon_intensity") > 0.3, "Medium")
               .otherwise("Low")) \
    .withColumn("recommended_action",
               when(col("green_rating") == "Poor", "Urgent migration to renewable")
               .when(col("optimization_potential") == "High", "Improve efficiency")
               .when(col("energy_source") != "renewable", "Switch to renewable energy")
               .otherwise("Continue monitoring"))

# Sustainability reporting
location_sustainability = df_green_computing.groupBy("location", "energy_source") \
    .agg(sum("power_consumption_kw").alias("total_power_kw"),
         sum("carbon_footprint_kg").alias("total_carbon_kg"),
         avg("sustainability_score").alias("avg_sustainability"),
         count("*").alias("resource_count"))

# Carbon footprint reduction opportunities
reduction_opportunities = df_green_computing.groupBy("optimization_potential") \
    .agg(sum("carbon_footprint_kg").alias("potential_reduction_kg"),
         count("*").alias("resource_count"),
         avg("power_consumption_kw").alias("avg_power_consumption"))

print("Sustainable data computing analysis:")
df_green_computing.select("location", "resource_type", "carbon_intensity", "sustainability_score", "green_rating", "recommended_action").show()
print("Location sustainability metrics:")
location_sustainability.show()
print("Carbon reduction opportunities:")
reduction_opportunities.show()

# Challenge 100: Future-Proof Data Architecture
print("\n100. FUTURE-PROOF DATA ARCHITECTURE")
print("Problem: Design architecture for next-generation requirements")

future_architecture_data = [
    ("quantum_computing", "experimental", "high", "2027", "cryptography,optimization", 5),
    ("neuromorphic_chips", "research", "medium", "2026", "ai_inference,pattern_recognition", 7),
    ("dna_storage", "pilot", "low", "2030", "long_term_archive,massive_storage", 3),
    ("photonic_computing", "research", "medium", "2028", "high_speed_processing,ml_training", 6),
    ("edge_ai_clusters", "available", "high", "2025", "real_time_inference,autonomous_systems", 9),
    ("holographic_storage", "experimental", "low", "2029", "ultra_high_density,multimedia", 4),
    ("brain_computer_interfaces", "research", "medium", "2035", "direct_neural_input,accessibility", 2),
    ("molecular_computing", "experimental", "low", "2040", "biological_systems,self_healing", 1),
    ("room_temperature_superconductors", "theoretical", "high", "2032", "zero_loss_transmission,quantum", 3),
    ("6g_networks", "development", "high", "2030", "ultra_low_latency,iot_massive", 8)
]

df_future_arch = spark.createDataFrame(future_architecture_data, 
    ["technology", "maturity_level", "adoption_likelihood", "estimated_availability", "use_cases", "readiness_score"])

# Solution
print("Solution:")
df_future_readiness = df_future_arch \
    .withColumn("years_to_availability", 
               col("estimated_availability").cast(IntegerType()) - 2024) \
    .withColumn("investment_priority",
               when((col("adoption_likelihood") == "high") & (col("years_to_availability") <= 3), "IMMEDIATE")
               .when((col("adoption_likelihood") == "high") & (col("years_to_availability") <= 6), "SHORT_TERM")
               .when(col("adoption_likelihood") == "medium", "MEDIUM_TERM")
               .otherwise("LONG_TERM")) \
    .withColumn("use_case_count", size(split(col("use_cases"), ","))) \
    .withColumn("strategic_value",
               col("readiness_score") * col("use_case_count") * 
               when(col("adoption_likelihood") == "high", 3)
               .when(col("adoption_likelihood") == "medium", 2)
               .otherwise(1)) \
    .withColumn("preparation_required",
               when(col("investment_priority") == "IMMEDIATE", "Start R&D now")
               .when(col("investment_priority") == "SHORT_TERM", "Begin proof of concepts")
               .when(col("investment_priority") == "MEDIUM_TERM", "Monitor developments")
               .otherwise("Track research")) \
    .withColumn("compatibility_risk",
               when(col("maturity_level") == "experimental", "High")
               .when(col("maturity_level") == "research", "Medium")
               .otherwise("Low"))

# Strategic technology roadmap
technology_roadmap = df_future_readiness.groupBy("investment_priority") \
    .agg(count("*").alias("technology_count"),
         avg("strategic_value").alias("avg_strategic_value"),
         collect_list("technology").alias("technologies"),
         avg("years_to_availability").alias("avg_years_to_market"))

# Innovation investment recommendations
investment_recommendations = df_future_readiness.filter(col("strategic_value") > 15) \
    .select("technology", "investment_priority", "strategic_value", "preparation_required") \
    .orderBy(desc("strategic_value"))

# Future data workload projections
workload_projections = df_future_readiness \
    .withColumn("projected_data_volume_growth",
               when(col("technology").contains("storage"), 1000)
               .when(col("technology").contains("quantum"), 100)
               .when(col("technology").contains("ai"), 500)
               .otherwise(50)) \
    .withColumn("projected_processing_speedup",
               when(col("technology").contains("quantum"), 1000000)
               .when(col("technology").contains("photonic"), 10000)
               .when(col("technology").contains("neuromorphic"), 1000)
               .otherwise(10))

print("Future-proof data architecture analysis:")
df_future_readiness.select("technology", "years_to_availability", "investment_priority", "strategic_value", "preparation_required").show()
print("Technology roadmap:")
technology_roadmap.show(truncate=False)
print("Investment recommendations (high strategic value):")
investment_recommendations.show()

print("\n" + "="*80)
print("🎉 CONGRATULATIONS! ALL 100 CHALLENGES COMPLETED! 🎉")
print("="*80)
print("\nCOMPREHENSIVE CHALLENGE SUMMARY:")
print("="*50)
print("✅ SIMPLE LEVEL (1-30): Basic operations, filtering, aggregations, data types")
print("✅ INTERMEDIATE LEVEL (31-65): Complex joins, window functions, UDFs, streaming, optimization")
print("✅ ADVANCED LEVEL (66-100): ML integration, governance, security, cutting-edge technologies")
print("\nCOVERAGE AREAS:")
print("🔹 Data Engineering: ETL, pipelines, performance optimization")
print("🔹 Data Architecture: Lake house, mesh, multi-cloud, federation")
print("🔹 Analytics: Real-time, streaming, OLAP, graph processing")
print("🔹 Machine Learning: Feature engineering, model deployment, AutoML")
print("🔹 Data Governance: Quality, lineage, privacy, compliance")
print("🔹 Security: Encryption, access control, audit trails")
print("🔹 Operations: Monitoring, observability, disaster recovery")
print("🔹 Emerging Technologies: Quantum computing, edge analytics, digital twins")
print("\nThis comprehensive script covers ALL scenarios commonly asked in")
print("senior Databricks engineer interviews at top-tier organizations in 2024!")
print("="*80)

# Clean up
spark.stop()

