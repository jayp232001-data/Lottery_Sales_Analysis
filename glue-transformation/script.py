# Import standard system and Spark/Glue libraries
import sys  # Used to access command-line arguments
from pyspark.context import SparkContext  # Spark execution context
from awsglue.context import GlueContext  # Glue-specific extension of SparkContext
from awsglue.job import Job  # AWS Glue job management class
from awsglue.utils import getResolvedOptions  # Helper to extract job parameters

# PySpark types and functions for transformations
from pyspark.sql.types import StringType  #to explicitly cast to string type
from pyspark.sql.functions import (
    col,# Reference a column in DataFrame
    regexp_replace,  # Regular expression replacement in strings
    count,  # Aggregate count function
    row_number,  # Ranking function for canonical name logic
    to_date,  # Convert string to date format
    when,  # Conditional column logic
    concat_ws,  # Concatenate strings with separator
    lit,  # to insert literal values
    lower,  # Convert strings to lowercase
    abs  # Absolute value, used to handle negative numeric values
)
from pyspark.sql.window import Window  # Required for using row_number with partitions


# Initialize Glue job and Spark session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])  # Parse job arguments, expecting JOB_NAME
sc = SparkContext()  # Create SparkContext
glueContext = GlueContext(sc)  # Wrap SparkContext in GlueContext
spark = glueContext.spark_session  # Get SparkSession from GlueContext
job = Job(glueContext)  # Initialize Glue Job object
job.init(args['JOB_NAME'], args)  # Start Glue job with job name argument

df = spark.read.csv("s3://joinedbucketbasedonzipcode/joineddatazipcode/",header=True,inferSchema=True)



#Mamta
# Step : Generate canonical retailer location name by selecting the most frequent retailer_location_name per retailer_license_number
retailer_name_freq = df.groupBy("retailer_license_number", "retailer_location_name") \
    .agg(count("*").alias("name_count"))
retailer_window = Window.partitionBy("retailer_license_number").orderBy(col("name_count").desc())
retailer_ranked = retailer_name_freq.withColumn("rank", row_number().over(retailer_window))
retailer_canonical = retailer_ranked.filter(col("rank") == 1) \
    .selectExpr("retailer_license_number as rl_num", "retailer_location_name as canonical_name")

# Join canonical retailer name back to main DataFrame and replace original retailer_location_name
df = df.join(retailer_canonical, df["retailer_license_number"] == col("rl_num"), "left") \
       .drop("retailer_location_name", "rl_num") \
       .withColumnRenamed("canonical_name", "retailer_location_name")


# Step : Generate canonical owning entity retailer name similarly, picking the most frequent name per owning_entity_retailer_number
parent_name_freq = df.groupBy("owning_entity_retailer_number", "owning_entity_retailer_name") \
    .agg(count("*").alias("name_count"))
parent_window = Window.partitionBy("owning_entity_retailer_number").orderBy(col("name_count").desc())
parent_ranked = parent_name_freq.withColumn("rank", row_number().over(parent_window))
parent_canonical = parent_ranked.filter(col("rank") == 1) \
    .selectExpr("owning_entity_retailer_number as oern", "owning_entity_retailer_name as canonical_parent")

# Join canonical owning entity retailer name back and replace original
df = df.join(parent_canonical, df["owning_entity_retailer_number"] == col("oern"), "left") \
       .drop("owning_entity_retailer_name", "oern") \
       .withColumnRenamed("canonical_parent", "owning_entity_retailer_name")

#Samir

# Step : Standardize column names by stripping whitespace, replacing spaces with underscores, and converting to lowercase
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])


# Step : Remove duplicate rows to ensure data integrity
df = df.dropDuplicates()


# Step : Cast columns to appropriate data types for accurate processing and calculations
df = df.withColumn("gross_ticket_sales_amount", col("gross_ticket_sales_amount").cast("decimal(20,2)")) \
       .withColumn("net_ticket_sales_amount", col("net_ticket_sales_amount").cast("decimal(20,2)")) \
       .withColumn("ticket_price", col("ticket_price").cast("decimal(10,2)")) \
       .withColumn("population", regexp_replace(col("population"), ",", "").cast("long")) \
       .withColumn("month_ending_date", to_date(col("month_ending_date"), "MM/dd/yyyy"))



# Step : Add retailer_group column — "Self-Owned Retailer" if retailer owns itself, else use owning entity name
df = df.withColumn(
    "retailer_group",
    when(
        col("retailer_license_number") == col("owning_entity_retailer_number"),
        "Self-Owned Retailer"
    ).otherwise(
        col("owning_entity_retailer_name")
    )
)


# Step : Drop unwanted or redundant columns to clean dataset
columns_to_drop = [
    "retailer_location_address_2",
    "retailer_location_zip_code_+4",
    "calendar_month",
    "calendar_year",
    "calendar_month_name_and_number",
    "retailer_number_and_location_name",
    "retailer_location_state",
    "owning_entity/chain_head_number_and_name"
]
df = df.drop(*columns_to_drop)



#Jay

# Step : Convert cancelled, promotional, and ticket returns amounts to positive values using absolute function
df = df.withColumn("cancelled_tickets_amount_positive", abs(col("cancelled_tickets_amount")))

df = df.withColumn("ticket_returns_amount_positive", abs(col("ticket_returns_amount")))

# Step : Derive number of tickets returned and sold by dividing amounts by ticket_price (handle divide by zero by conditional)
df = df.withColumn(
    "number_of_ticket_returned",
    when(col("ticket_price") > 0, abs(col("ticket_returns_amount")) / col("ticket_price")).otherwise(0)
).withColumn(
    "number_of_ticket_sold",
    when(col("ticket_price") > 0, abs(col("net_ticket_sales_amount")) / col("ticket_price")).otherwise(0)
)



#Yash
# Step : Add flag column 'is_negative_sale' to identify rows where net ticket sales amount is negative (e.g., refunds)
df = df.withColumn("is_negative_sale", when(col("net_ticket_sales_amount") < 0, 1).otherwise(0))


# Step : Add string version of ticket_price for any string operations or output formatting
df = df.withColumn("ticket_price_str", col("ticket_price").cast(StringType()))
df = df.withColumn("promotional_tickets_amount_positive", abs(col("promotional_tickets_amount")))

# Step : Add region column by mapping retailer_location_county to predefined Texas regions (panhandle, north texas, etc.)
# Lists of counties for each region (all lowercase for case-insensitive match)
panhandle = ['armstrong','briscoe','carson','castro','childress','collingsworth','dallam','deaf smith','donley','gray','hall','hansford','hartley','hemphill','hutchinson','lipscomb','moore','ochiltree','oldham','parmer','potter','randall','roberts','sherman','swisher','wheeler']
north_texas = ['collin','dallas','denton','ellis','erath','hood','hunt','johnson','kaufman','navarro','palo pinto','parker','rockwall','somervell','tarrant','wise','archer','baylor','clay','cottle','foard','hardeman','jack','montague','wichita','wilbarger','young','cooke','fannin','grayson']
east_texas = ['bowie','cass','delta','franklin','hopkins','lamar','morris','red river','titus','anderson','camp','cherokee','gregg','harrison','henderson','marion','panola','rains','rusk','smith','upshur','van zandt','wood','angelina','houston','jasper','nacogdoches','newton','polk','sabine','san augustine','san jacinto','shelby','trinity','tyler','hardin','jefferson','orange']
upper_gulf = ['austin','brazoria','chambers','colorado','fort bend','galveston','harris','liberty','matagorda','montgomery','walker','waller','wharton']
south_texas = ['atascosa','bandera','bexar','comal','frio','gillespie','guadalupe','karnes','kendall','kerr','medina','wilson','calhoun','dewitt','goliad','gonzales','jackson','lavaca','victoria','aransas','bee','brooks','duval','jim wells','kenedy','kleberg','live oak','mcmullen','nueces','refugio','san patricio','cameron','hidalgo','willacy','jim hogg','starr','webb','zapata','dimmit','edwards','kinney','la salle','maverick','real','uvalde','val verde','zavala']
west_texas = ['coke','concho','crockett','irion','kimble','mason','mcculloch','menard','reagan','schleicher','sterling','sutton','tom green','andrews','borden','crane','dawson','ector','gaines','glasscock','howard','loving','martin','midland','pecos','reeves','terrell','upton','ward','winkler','brewster','culberson','el paso','hudspeth','jeff davis','presidio','bailey','cochran','crosby','dickens','floyd','garza','hale','hockley','king','lamb','lubbock','lynn','motley','terry','yoakum','brown','callahan','coleman','comanche','eastland','fisher','haskell','jones','kent','knox','mitchell','nolan','runnels','scurry','shackelford','stephens','stonewall','taylor','throckmorton']
central_texas = ['brazos','burleson','grimes','leon','madison','robertson','washington','bastrop','blanco','burnet','caldwell','fayette','hays','lee','llano','travis','williamson','bell','coryell','hamilton','lampasas','milam','mills','san saba','bosque','falls','freestone','hill','limestone','mclennan']

df = df.withColumn(
    "region",
    when(lower(col("retailer_location_county")).isin(panhandle), "Panhandle")
    .when(lower(col("retailer_location_county")).isin(north_texas), "North Texas")
    .when(lower(col("retailer_location_county")).isin(east_texas), "East Texas")
    .when(lower(col("retailer_location_county")).isin(upper_gulf), "Upper Gulf Coast")
    .when(lower(col("retailer_location_county")).isin(south_texas), "South Texas")
    .when(lower(col("retailer_location_county")).isin(west_texas), "West Texas")
    .when(lower(col("retailer_location_county")).isin(central_texas), "Central Texas")
    .otherwise("Unknown")
)


# Final output S3 path for storing data in Parquet format
output_path = "s3://texaslotteryfinaltransformdata-group1/output/"

#Write DataFrame to Parquet partitioned by fiscal_year
df.write \
    .mode("overwrite") \
    .partitionBy("fiscal_year") \
    .parquet(output_path)

job.commit()
