
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

# Step : Read from AWS Glue Data Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="test_db",
    table_name="lottery_sales",
    transformation_ctx="Terraform_ETL_Using_Glue"
).toDF()

# Step : Standardize column names (replace spaces with underscores and lowercase)
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

# Step : Drop duplicates
df = df.dropDuplicates()

# Step : Cast numeric and date columns
df = df.withColumn("gross_ticket_sales_amount", col("gross_ticket_sales_amount").cast("decimal(20,2)")) \
       .withColumn("net_ticket_sales_amount", col("net_ticket_sales_amount").cast("decimal(20,2)")) \
       .withColumn("ticket_price", col("ticket_price").cast("decimal(10,2)")) \
       .withColumn("month_ending_date", to_date(col("month_ending_date"), "MM/dd/yyyy"))


# Step : Canonical Retailer Location Name
retailer_name_freq = df.groupBy("retailer_license_number", "retailer_location_name") \
    .agg(count("*").alias("name_count"))
retailer_window = Window.partitionBy("retailer_license_number").orderBy(col("name_count").desc())
retailer_ranked = retailer_name_freq.withColumn("rank", row_number().over(retailer_window))
retailer_canonical = retailer_ranked.filter(col("rank") == 1) \
    .selectExpr("retailer_license_number as rl_num", "retailer_location_name as canonical_name")
df = df.join(retailer_canonical, df["retailer_license_number"] == col("rl_num"), "left") \
       .drop("retailer_location_name", "rl_num") \
       .withColumnRenamed("canonical_name", "retailer_location_name")

# Step : Canonical Owning Entity Retailer Name

parent_name_freq = df.groupBy("owning_entity_retailer_number", "owning_entity_retailer_name") \
    .agg(count("*").alias("name_count"))
parent_window = Window.partitionBy("owning_entity_retailer_number").orderBy(col("name_count").desc())
parent_ranked = parent_name_freq.withColumn("rank", row_number().over(parent_window))
parent_canonical = parent_ranked.filter(col("rank") == 1) \
    .selectExpr("owning_entity_retailer_number as oern", "owning_entity_retailer_name as canonical_parent")
df = df.join(parent_canonical, df["owning_entity_retailer_number"] == col("oern"), "left") \
       .drop("owning_entity_retailer_name", "oern") \
       .withColumnRenamed("canonical_parent", "owning_entity_retailer_name")

# Step : Add Is_Negative_Sale flag
df = df.withColumn("is_negative_sale", when(col("net_ticket_sales_amount") < 0, 1).otherwise(0))

df = df.withColumn("cancelled_tickets_amount_positive", abs(col("cancelled_tickets_amount")))

# Step : Add ticket_price_str
df = df.withColumn("ticket_price_str", col("ticket_price").cast(StringType()))

# Step : Add retailer_group
df = df.withColumn(
    "retailer_group",
    when(
        col("retailer_license_number") == col("owning_entity_retailer_number"),
        "Self-Owned Retailer"
    ).otherwise(
        col("owning_entity_retailer_name")
    )
)

# Step : Add region column based on 'retailer location county'
# Define region mappings
panhandle = ['armstrong','briscoe','carson','castro','childress','collingsworth','dallam','deaf smith','donley','gray','hall','hansford','hartley','hemphill','hutchinson','lipscomb','moore','ochiltree','oldham','parmer','potter','randall','roberts','sherman','swisher','wheeler']

north_texas = ['collin','dallas','denton','ellis','erath','hood','hunt','johnson','kaufman','navarro','palo pinto','parker','rockwall','somervell','tarrant','wise',
               'archer','baylor','clay','cottle','foard','hardeman','jack','montague','wichita','wilbarger','young',
               'cooke','fannin','grayson']

east_texas = ['bowie','cass','delta','franklin','hopkins','lamar','morris','red river','titus',
              'anderson','camp','cherokee','gregg','harrison','henderson','marion','panola','rains','rusk','smith','upshur','van zandt','wood',
              'angelina','houston','jasper','nacogdoches','newton','polk','sabine','san augustine','san jacinto','shelby','trinity','tyler',
              'hardin','jefferson','orange']

upper_gulf = ['austin','brazoria','chambers','colorado','fort bend','galveston','harris','liberty','matagorda','montgomery','walker','waller','wharton']

south_texas = ['atascosa','bandera','bexar','comal','frio','gillespie','guadalupe','karnes','kendall','kerr','medina','wilson',
               'calhoun','dewitt','goliad','gonzales','jackson','lavaca','victoria',
               'aransas','bee','brooks','duval','jim wells','kenedy','kleberg','live oak','mcmullen','nueces','refugio','san patricio',
               'cameron','hidalgo','willacy',
               'jim hogg','starr','webb','zapata',
               'dimmit','edwards','kinney','la salle','maverick','real','uvalde','val verde','zavala']

west_texas = ['coke','concho','crockett','irion','kimble','mason','mcculloch','menard','reagan','schleicher','sterling','sutton','tom green',
              'andrews','borden','crane','dawson','ector','gaines','glasscock','howard','loving','martin','midland','pecos','reeves','terrell','upton','ward','winkler',
              'brewster','culberson','el paso','hudspeth','jeff davis','presidio',
              'bailey','cochran','crosby','dickens','floyd','garza','hale','hockley','king','lamb','lubbock','lynn','motley','terry','yoakum',
              'brown','callahan','coleman','comanche','eastland','fisher','haskell','jones','kent','knox','mitchell','nolan','runnels','scurry','shackelford','stephens','stonewall','taylor','throckmorton']

central_texas = ['brazos','burleson','grimes','leon','madison','robertson','washington',
                 'bastrop','blanco','burnet','caldwell','fayette','hays','lee','llano','travis','williamson',
                 'bell','coryell','hamilton','lampasas','milam','mills','san saba',
                 'bosque','falls','freestone','hill','limestone','mclennan']

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

# Step 11: Full location string
df = df.withColumn(
    "location_full",
    concat_ws(", ", col("retailer_location_county"), col("retailer_location_city"), lit("Texas"), lit("USA"))
)

# Step 12: Drop unwanted columns
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

# Step 13: Write partitioned by fiscal_year
output_path = "s3://jay-patil-transformed-bucket/transformed_data/"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Step 14: Commit job
job.commit()