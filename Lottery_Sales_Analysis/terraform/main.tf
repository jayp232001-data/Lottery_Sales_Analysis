provider "aws" {
  region = var.aws_region
}

resource "aws_glue_job" "lottery_sales_etl_job" {
  name     = var.glue_job_name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
  }

  default_arguments = {
    "--job-language" = "python"
  }

  max_retries = 1
}

resource "aws_glue_catalog_database" "etl_db" {
  name = var.glue_database_name
}

resource "aws_glue_crawler" "etl_crawler" {
  name         = var.glue_crawler_name
  role         = var.glue_role_arn
  database_name = var.glue_database_name
  s3_target {
    path = var.crawler_s3_path
  }
}
