# Configure the AWS provider with the specified region
provider "aws" {
  region = var.aws_region
}

###############################################
# AWS Glue Job for Data Transformation
###############################################
resource "aws_glue_job" "transform_job" {
  name     = "lottery-transform-job"                   # Glue job name
  role_arn = var.iam_role                              # IAM role ARN

  # Job command and script settings
  command {
    name            = "glueetl"                        # Job type (glueetl = Spark ETL)
    script_location = var.glue_script_s3_path          # S3 path to transformation script
    python_version  = "3"                              # Use Python 3
  }

  default_arguments = {
    "--job-bookmark-option"                   = "job-bookmark-disable"
    "--enable-metrics"                        = "true"
    "--enable-continuous-cloudwatch-log"      = "true"
    "--enable-spark-ui"                       = "true"
      }

  # Retry and timeout configurations
  max_retries = 0                                     # Retry once if the job fails
  timeout     = 120                                     # Timeout in minutes
  glue_version = "5.0"                                 # Glue runtime version

  # Optional: Specify number of workers and type
  number_of_workers = 2                                # Number of DPUs
  worker_type       = "G.1X"                           # Worker type (G.1X or G.2X)

  # Optional: Job description
  description = "ETL job to transform lottery data"

  
}

###############################################
# AWS Glue Crawler for Transformed Data
###############################################
resource "aws_glue_crawler" "transformed_data_crawler" {
  name          = "lottery-transformed-crawler"        # Unique name of the crawler
  role          = var.iam_role                         # IAM role used by the crawler
  database_name = var.transformed_glue_database        # Glue database to store transformed data catalog

  # Define the S3 data source for crawling transformed data
  s3_target {
    path = var.transformed_data_s3_path                # S3 path to transformed data
  }

  depends_on = [aws_glue_job.transform_job]

}