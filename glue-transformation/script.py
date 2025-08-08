
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    col, count, row_number, to_date, when, regexp_replace, concat_ws, lit, lower, abs

)
from pyspark.sql.window import Window

# Initialize Glue job 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read from AWS Glue Data Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="lottery_sales",
    transformation_ctx="Terraform_ETL_Using_Glue"
).toDF()

# Step 2: Drop duplicates
df = df.dropDuplicates()

# Step 3: Cast numeric and date columns
df = df.withColumn("gross ticket sales amount", col("gross ticket sales amount").cast("decimal(20,2)")) \
       .withColumn("net ticket sales amount", col("net ticket sales amount").cast("decimal(20,2)")) \
       .withColumn("ticket price", col("ticket price").cast("decimal(10,2)")) \
       .withColumn("population", regexp_replace(col("population"), ",", "").cast("long")) \
       .withColumn("month ending date", to_date(col("month ending date"), "MM/dd/yyyy"))

# Step 4: Canonical Retailer Location Name
retailer_name_freq = df.groupBy("retailer license number", "retailer location name") \
    .agg(count("*").alias("name_count"))
retailer_window = Window.partitionBy("retailer license number").orderBy(col("name_count").desc())
retailer_ranked = retailer_name_freq.withColumn("rank", row_number().over(retailer_window))
retailer_canonical = retailer_ranked.filter(col("rank") == 1) \
    .selectExpr("`retailer license number` as rl_num", "`retailer location name` as canonical_name")
df = df.join(retailer_canonical, df["retailer license number"] == col("rl_num"), "left") \
       .drop("retailer location name", "rl_num") \
       .withColumnRenamed("canonical_name", "retailer location name")

# Step 5: Canonical Owning Entity Retailer Name
parent_name_freq = df.groupBy("owning entity retailer number", "owning entity retailer name") \
    .agg(count("*").alias("name_count"))
parent_window = Window.partitionBy("owning entity retailer number").orderBy(col("name_count").desc())
parent_ranked = parent_name_freq.withColumn("rank", row_number().over(parent_window))
parent_canonical = parent_ranked.filter(col("rank") == 1) \
    .selectExpr("`owning entity retailer number` as oern", "`owning entity retailer name` as canonical_parent")
df = df.join(parent_canonical, df["owning entity retailer number"] == col("oern"), "left") \
       .drop("owning entity retailer name", "oern") \
       .withColumnRenamed("canonical_parent", "owning entity retailer name")

# Step 6: Add Is_Negative_Sale flag
df = df.withColumn("Is_Negative_Sale", when(col("net ticket sales amount") < 0, 1).otherwise(0))

df = df.withColumn("cancelled_tickets_amount_positive", abs(col("cancelled tickets amount")))

# Add a new column with ticket_price as string
df = df.withColumn("ticket_price_str", col("ticket price").cast(StringType()))

df = df.withColumn(
    "retailer_group",
    when(
        col("retailer license number") == col("owning entity retailer number"),
        "Self-Owned Retailer"
    ).otherwise(
        col("owning entity retailer name")
    )
)


# Step 8: Drop unwanted columns (exact matches)
columns_to_drop = [
    "retailer location address 2",
    "retailer location zip code +4",
    "calendar month",
    "calendar year",
    "calendar month name and number",
    "retailer number and location name",
    "retailer location state",
    "owning entity/chain head number and name"
]
df = df.drop(*columns_to_drop)

# Step 11: Write to S3
output_path = "s3://jay-patil-transformed-bucket/transformed_data/"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Step 12: Commit job
job.commit()
