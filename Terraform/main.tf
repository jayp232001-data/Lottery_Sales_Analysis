# Configure AWS provider
provider "aws" {
  region = var.aws_region  # AWS region (passed via variable)
}

###############################################
# AWS Glue Job for Data Transformation
###############################################
resource "aws_glue_job" "transform_job" {
  name     = "lottery-transform-job"         # Unique Glue job name
  role_arn = var.iam_role                    # IAM role ARN with Glue permissions

  command {
    name            = "glueetl"              # Job type: Spark ETL
    script_location = var.glue_script_s3_path # S3 path to transformation script
    python_version  = "3"                    # Use Python 3
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"       # Disable incremental bookmarks
    "--enable-metrics"                   = "true"                       # Enable CloudWatch metrics
    "--enable-continuous-cloudwatch-log" = "true"                       # Enable continuous CloudWatch logging
    "--enable-spark-ui"                  = "true"                       # Enable Spark UI for debugging
  }

  max_retries     = 0           # No retries on failure
  timeout         = 120         # Timeout in minutes
  glue_version    = "5.0"       # Glue runtime version
  number_of_workers = 2         # Number of workers (DPUs)
  worker_type     = "G.1X"      # Worker type (4 vCPU, 16 GB RAM)
  description     = "ETL job to transform lottery data"  # Job description
}

###############################################
# AWS Glue Crawler for Transformed Data
###############################################
resource "aws_glue_crawler" "transformed_data_crawler" {
  name          = "lottery-transformed-crawler" # Unique crawler name
  role          = var.iam_role                  # IAM role with crawler permissions
  database_name = var.transformed_glue_database # Glue database for schema storage

  s3_target {
    path = var.transformed_data_s3_path         # S3 location of transformed data
  }

  depends_on = [aws_glue_job.transform_job]     # Create after Glue job
}
