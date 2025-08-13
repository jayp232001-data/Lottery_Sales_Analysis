
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
    database="lotteryfinal",
    table_name="part_00000_f4ecfb09_7078_46bc_a277_c37f5e02741e_c000_csv",
    transformation_ctx="datasource"
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

# Step 7: Add region column based on 'retailer location county'
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

# Create region column using when().otherwise() chain
df = df.withColumn(
    "region",
    when(lower(col("retailer location county")).isin(panhandle), "Panhandle")
    .when(lower(col("retailer location county")).isin(north_texas), "North Texas")
    .when(lower(col("retailer location county")).isin(east_texas), "East Texas")
    .when(lower(col("retailer location county")).isin(upper_gulf), "Upper Gulf Coast")
    .when(lower(col("retailer location county")).isin(south_texas), "South Texas")
    .when(lower(col("retailer location county")).isin(west_texas), "West Texas")
    .when(lower(col("retailer location county")).isin(central_texas), "Central Texas")
    .otherwise("Unknown")
)


df = df.withColumn(
    "location_full",
    concat_ws(", ", col("retailer location county"), col("retailer location city"), lit("Texas"), lit("USA"))
)


# Step 9: Derived columns - number of tickets sold and returned
df = df.withColumn(
    "number_of_ticket_returned",
    when(col("ticket price") > 0, abs(col("ticket returns amount")) / col("ticket price")).otherwise(0)
).withColumn(
    "number_of_ticket_sold",
    when(col("ticket price") > 0, col("net ticket sales amount") / col("ticket price")).otherwise(0)
)

df = df.withColumn("ticket_returns_amount_positive", abs(col("ticket returns amount")))

df = df.withColumn("promotional_tickets_amount_positive", abs(col("promotional tickets amount")))

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

# Step 10: Reorder columns - preserve original order + derived fields
preserved_columns = [c for c in df.columns if c not in ["Is_Negative_Sale", "region", "number_of_ticket_returned", "number_of_ticket_sold"]]
final_columns = preserved_columns + ["Is_Negative_Sale", "region", "number_of_ticket_returned", "number_of_ticket_sold"]
df = df.select(final_columns)

# Step 11: Write to S3
output_path = "s3://final-transformedbucket/masterdata2/"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Step 12: Commit job
job.commit()
