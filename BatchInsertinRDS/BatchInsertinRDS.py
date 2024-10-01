import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Import DynamicFrame
from pyspark.sql import functions as F  # Import functions for DataFrame operations

# Get the arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Load data from Glue Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="rdsdatabase",
    table_name="generatetbdata",
    transformation_ctx="datasource0"
)

# Step 2: Convert DynamicFrame to DataFrame for easier manipulation
df = datasource0.toDF()

# Print the schema to check column names
df.printSchema()

# Step 3: Rename columns to match the target table schema in RDS
df = df.withColumnRenamed("number of tourists", "Number_of_Tourists") \
       .withColumnRenamed("main enemy", "Main_Enemy") \
       .withColumnRenamed("develop status", "Develop_Status")

# Step 4: Convert the Date column to the correct format (if necessary)
df = df.withColumn("Date", F.to_date(F.unix_timestamp(df["Date"], "MM/dd/yyyy").cast("timestamp")))

# Step 5: Define batch size
batch_size = 200000
total_count = df.count()

# Step 6: Batch insert data into RDS
for i in range(0, total_count, batch_size):
    batch = df.limit(batch_size)  # Select a batch of records
    # Write the batch to RDS
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(batch, glueContext, "batch_df"),
        connection_type="jdbc",
        connection_options={
            "url": "jdbc:mysql://database-2.ctqw2uoc8tsd.ap-south-1.rds.amazonaws.com/rdsdatabases",
            "dbtable": "generatetbdatas",
            "user": "admin",
            "password": "Bls4967at"
        },
        transformation_ctx="datasink"
    )
    # Remove the processed batch from df
    df = df.subtract(batch)

job.commit()
