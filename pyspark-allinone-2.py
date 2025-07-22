from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, substring, length, trim, upper, lower, initcap,
    to_date, to_timestamp, date_format, datediff, months_between, current_date, add_months, date_add,
    avg, sum, count, max, min, row_number, rank, dense_rank, lag, lead,
    split, array, struct, map_from_arrays, explode, posexplode, from_json, to_json,
    expr, coalesce, isnan, isnull, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField, ArrayType, MapType

# Setup Spark
spark = SparkSession.builder.appName("ExpertPySparkInterviewDemo").getOrCreate()

# -- Sample DataFrames --
emp_data = [
    (1, " Alice ", "2022-01-10", 8000.0, "dept1", "New York", '{"skills":["spark","python"],"props":{"cert":"aws","level":"senior"}}'),
    (2, "BOB", "12/02/2021", 9500.0, "dept2", "San Francisco", '{"skills":["java","spark"],"props":{"cert":"azure","level":"mid"}}'),
    (3, "charlie", "2019-07-30 09:05", 12000.0, "dept1", "New York", '{"skills":["sql"],"props":{"cert":"gcp","level":"senior"}}'),
    (4, "David", None, 7000.0, "dept3", "Boston", '{"skills":["python"],"props":{"cert":"aws","level":"junior"}}'),
    (5, "Eve", "11-12-2020", 11000.0, None, "Los Angeles", '{"skills":["java"],"props":{"cert":"aws","level":"mid"}}'),
    (6, "Frank", "03/03/2018", 10500.0, "dept2", "San Francisco", '{"skills":["hadoop","spark"],"props":{"cert":"databricks","level":"senior"}}'),
    (7, None, "2022-12-01", None, "dept4", "Boston", '{"skills":["python"],"props":{"cert":"azure","level":"junior"}}'),
    (8, "Grace", "07-15-2022", 9800.0, "dept1", None, '{"skills":["sql"],"props":{"cert":"gcp","level":"mid"}}'),
    (9, "Henry", "May 10, 2020", 11500.0, "dept3", "Boston", '{"skills":["hadoop"],"props":{"cert":"aws","level":"senior"}}'),
    (10, "Ivy", "2020/04/23 14:00", 8800.0, "dept3", "Boston", '{"skills":["python","spark"],"props":{"cert":"aws","level":"junior"}}'),
    (11, "John", "2021-08-19T16:20:00", 11500.0, "dept2", "San Francisco", '{"skills":["sql","python"],"props":{"cert":"azure","level":"mid"}}'),
]

df_emp = spark.createDataFrame(emp_data,
    schema=["emp_id", "name", "hire_date_str", "salary", "dept_id", "location", "json_skills"])

dept_data = [
    ("dept1", "HR", "New York"),
    ("dept2", "Finance", "San Francisco"),
    ("dept3", "IT", "Boston"),
    ("dept4", "Marketing", "Chicago"),
    ("dept5", "Sales", "Seattle"),
]
df_dept = spark.createDataFrame(dept_data, ["dept_id", "dept_name", "dept_location"])

bonus_data = [
    (1, 1500.0, "2023-12-31"),
    (2, 1200.0, "2023-11-30"),
    (3, 2000.0, "2023-03-15"),
    (4, 1100.0, None),
    (5, 1700.0, "2023-10-12"),
    (6, None, "2023-09-10"),
    (7, 900.0, "2023-07-01"),
    (9, 1400.0, "2023-07-30"),
    (10, 1200.0, "2023-10-12"),
    (11, 1900.0, "2023-04-28")
]
df_bonus = spark.createDataFrame(bonus_data, ["emp_id", "bonus_amount", "bonus_date_str"])

# -- Multiple Date/Time Formats --
df_emp = df_emp.withColumn("hire_date1", to_date("hire_date_str", "yyyy-MM-dd")) \
    .withColumn("hire_date2", to_date("hire_date_str", "dd/MM/yyyy")) \
    .withColumn("hire_date3", to_date("hire_date_str", "yyyy/MM/dd")) \
    .withColumn("hire_date4", to_date("hire_date_str", "dd-MM-yyyy")) \
    .withColumn("hire_date5", to_date("hire_date_str", "MM-dd-yyyy")) \
    .withColumn("hire_date6", to_date("hire_date_str", "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("hire_date7", to_date("hire_date_str", "yyyy-MM-dd HH:mm")) \
    .withColumn("hire_date8", to_date("hire_date_str", "MMMM dd, yyyy")) \
    .withColumn("hire_date_final", coalesce(
        "hire_date1", "hire_date2", "hire_date3", "hire_date4", "hire_date5", "hire_date6", "hire_date7", "hire_date8"
    )).drop("hire_date1", "hire_date2", "hire_date3", "hire_date4", "hire_date5", "hire_date6", "hire_date7", "hire_date8")

# -- String Functions & Cleaning --
df_emp_str = (
    df_emp.withColumn("name_trimmed", trim(col("name")))
        .withColumn("name_upper", upper(col("name")))
        .withColumn("name_lower", lower(col("name")))
        .withColumn("name_title", initcap(col("name")))
        .withColumn("name_length", length(col("name")))
        .withColumn("name_substring", substring(col("name"), 1, 3))
        .withColumn("concat_id_name", concat_ws("_", col("emp_id"), col("name")))
)

# -- Filtering, Null Handling, Conditional Logic --
df_clean = df_emp_str.fillna({"name": "Unknown", "salary": 0, "location": "Unknown"}) \
    .withColumn("is_salary_null", isnull("salary")) \
    .withColumn("salary_category", when(col("salary") >= 11000, "High")
                                    .when(col("salary") >= 9000, "Medium")
                                    .otherwise("Low or Null")) \
    .filter(col("salary") > 0)

# -- Join Types (inner, left, right, full, semi, anti, broadcast) --
df_inner = df_clean.join(df_dept, "dept_id", "inner")
df_left = df_clean.join(df_dept, "dept_id", "left")
df_right = df_clean.join(df_dept, "dept_id", "right")
df_full = df_clean.join(df_dept, "dept_id", "full")
df_semi = df_clean.join(df_dept, "dept_id", "left_semi")
df_anti = df_clean.join(df_dept, "dept_id", "left_anti")
df_broadcast = df_clean.join(broadcast(df_dept), "dept_id", "left")

# -- Window Functions --
window_dept = Window.partitionBy("dept_id").orderBy(col("salary").desc_nulls_last())
df_window = df_inner.withColumn("salary_rank", rank().over(window_dept)) \
                    .withColumn("salary_row", row_number().over(window_dept)) \
                    .withColumn("salary_lag", lag("salary").over(window_dept)) \
                    .withColumn("salary_lead", lead("salary").over(window_dept))

# -- GroupBy, Aggregate, Pivot --
df_agg = df_inner.groupBy("dept_id", "dept_name").agg(
    count("*").alias("emp_count"),
    avg("salary").alias("salary_avg"),
    max("salary").alias("salary_max"),
    min("salary").alias("salary_min")
)
df_pivot = df_inner.groupBy("dept_name").pivot("location").count()

# -- Array, Map, Explode --
df_arr_map = df_clean.withColumn("skills_array", split(lit("spark,python,sql,hadoop"), ",")) \
                     .withColumn("locations_map", map_from_arrays(array(lit("city"), lit("office")), array(col("location"), lit("HQ"))))
df_explode = df_arr_map.withColumn("one_skill", explode(col("skills_array")))

# -- JSON Handling --
json_schema = StructType([
    StructField("skills", ArrayType(StringType())),
    StructField("props", MapType(StringType(), StringType()))
])
df_json = df_emp.withColumn("skills_json", from_json("json_skills", json_schema))
df_json_exp = df_json.select("emp_id", "name", "skills_json.skills", "skills_json.props", explode("skills_json.skills").alias("single_skill"))

# -- selectExpr, expr, withColumnRenamed, CASE, complex columns --
df_selectexpr = df_inner.selectExpr(
    "emp_id", "name", "salary",
    "CASE WHEN salary >= 10000 THEN 'Top' ELSE 'Staff' END as band",
    "year(hire_date_final) as year_joined",
    "concat(name, ' (', location, ')') as named_loc"
)
df_expr = df_clean.withColumn("tax", expr("salary * CASE WHEN salary > 10000 THEN 0.2 ELSE 0.15 END"))
df_renamed = df_expr.withColumnRenamed("salary", "base_salary").withColumnRenamed("location", "city")

# -- dropDuplicates with various subsets and row_number deduplication --
window_latest = Window.partitionBy("name").orderBy(col("hire_date_final").desc())
df_dedup = df_clean.withColumn("rn", row_number().over(window_latest)).filter("rn=1").drop("rn")
df_dropdup_by_name = df_clean.dropDuplicates(["name"])
df_dropdup_combined = df_clean.dropDuplicates(["name", "location"])

# -- Read/Write CSV, Parquet, Delta --
df_clean.write.mode("overwrite").option("header", True).csv("/tmp/pyspark_demo_csv")
df_clean.write.mode("overwrite").parquet("/tmp/pyspark_demo_parquet")
try:
    df_clean.write.format("delta").mode("overwrite").save("/tmp/pyspark_demo_delta")
except Exception as e:
    print("Delta write skipped (no Delta jar):", e)

# -- Execution Plan (EXPLAIN) --
print("Execution plan for windowed ranking:")
df_window.explain("formatted")

# -- Show Examples --
print("==== String + Cleaning Functions ====")
df_emp_str.show()
print("==== Joined with Broadcast ====")
df_broadcast.show()
print("==== Window Functions ====")
df_window.select("emp_id", "name", "dept_id", "salary", "salary_rank", "salary_lag", "salary_lead").show()
print("==== GroupBy Aggregation ====")
df_agg.show()
print("==== Pivot Table ====")
df_pivot.show()
print("==== Array/Map/Explode ====")
df_explode.select("emp_id", "skills_array", "one_skill", "locations_map").show()
print("==== JSON Parse/Explode ====")
df_json_exp.show()
print("==== selectExpr and expr ====")
df_selectexpr.show()
df_expr.show()
print("==== Renamed Columns ====")
df_renamed.show()
print("==== Duplicate Handling (best/latest records by name) ====")
df_dedup.select("emp_id", "name", "hire_date_final", "salary").show()
