#all pyspark concepts in one scripts
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, concat, substring, length, trim,
    to_date, date_format, current_date, datediff,
    row_number, rank, dense_rank, lag, lead,
    avg, sum, count, max, min,
    explode,
    udf, array, map,
    coalesce, isnan, isnull
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkFeatureDemo") \
    .getOrCreate()

# Sample DataFrames
data1 = [
    (1, "Alice", "2024-01-01", 1000, "dept1"),
    (2, "Bob", "2024-01-05", 1500, "dept2"),
    (3, "Charlie", "2024-02-01", 1200, None),
    (4, "David", None, 1100, "dept1"),
    (5, None, "2024-03-01", None, "dept3"),
]

data2 = [
    ("dept1", "HR"),
    ("dept2", "Finance"),
    ("dept3", "IT"),
    ("dept4", "Marketing")
]

df1 = spark.createDataFrame(data1, schema=["id", "name", "date_str", "salary", "dept_id"])
df2 = spark.createDataFrame(data2, schema=["dept_id", "dept_name"])

print("=== Original DataFrames ===")
df1.show()
df2.show()

# 1. Data Type Conversion + Date Functions
df1 = df1.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd")) \
         .withColumn("salary", col("salary").cast("int"))

df1 = df1.withColumn("current_date", current_date()) \
         .withColumn("days_since_hire", datediff(current_date(), col("date")))

# 2. String Functions
df1 = df1.withColumn("name_trimmed", trim(col("name"))) \
         .withColumn("name_length", length(col("name"))) \
         .withColumn("name_substring", substring(col("name"), 1, 3)) \
         .withColumn("name_upper", col("name").substr(1, 1).rlike("[A-Z]")) \
         .withColumn("name_concat", concat(col("name"), lit("_emp")))

# 3. Filtering and Null Handling
df1_filtered = df1.filter(col("salary").isNotNull() & col("date").isNotNull())
df1_filled = df1.fillna({"name": "Unknown", "salary": 0})

# 4. Joins
df_joined = df1.join(df2, on="dept_id", how="left")  # left join to keep all employees even if dept missing
df_inner = df1.join(df2, on="dept_id", how="inner")  # only matched rows
df_left_anti = df1.join(df2, on="dept_id", how="left_anti")  # employees with no matching dept

# 5. Window Functions
windowSpec = Window.partitionBy("dept_id").orderBy(col("salary").desc())

df_window = df_joined.withColumn("rank_salary", rank().over(windowSpec)) \
                     .withColumn("dense_rank_salary", dense_rank().over(windowSpec)) \
                     .withColumn("row_number", row_number().over(windowSpec)) \
                     .withColumn("salary_lag", lag("salary").over(windowSpec)) \
                     .withColumn("salary_lead", lead("salary").over(windowSpec))

# 6. Aggregations and Grouping
agg_df = df_joined.groupBy("dept_name") \
    .agg(
        count("id").alias("num_employees"),
        avg("salary").alias("avg_salary"),
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary"),
        sum("salary").alias("total_salary")
    )

# 7. Conditional Expressions
df_conditional = df_joined.withColumn(
    "salary_category",
    when(col("salary") >= 1300, "High")
    .when((col("salary") < 1300) & (col("salary") >= 1100), "Medium")
    .otherwise("Low or Null")
)

# 8. Working with Arrays and Maps
df_array = df1.withColumn("skills", array(lit("Python"), lit("SQL"))) \
              .withColumn("skill_map", map(lit("primary"), lit("Python"), lit("secondary"), lit("SQL")))

df_exploded = df_array.withColumn("skill", explode(col("skills")))  # explode array

# 9. UDFs
def reverse_string(s):
    if s:
        return s[::-1]
    else:
        return None

reverse_string_udf = udf(reverse_string, StringType())

df_udf = df1.withColumn("name_reversed", reverse_string_udf(col("name")))

# 10. Sorting
df_sorted = df1.orderBy(col("salary").desc_nulls_last(), col("date").asc_nulls_last())

# 11. Handling NaNs and Nulls
df_nulls = df1.withColumn("is_salary_null", isnull(col("salary"))) \
              .withColumn("is_salary_nan", isnan(col("salary"))) \
              .withColumn("salary_coalesced", coalesce(col("salary"), lit(0)))

print("=== Transformed DataFrames ===")
df1.show()
df1_filtered.show()
df1_filled.show()
df_joined.show()
df_window.show()
agg_df.show()
df_conditional.show()
df_array.show()
df_exploded.show()
df_udf.show()
df_sorted.show()
df_nulls.show()

# Stop Spark session
spark.stop()
