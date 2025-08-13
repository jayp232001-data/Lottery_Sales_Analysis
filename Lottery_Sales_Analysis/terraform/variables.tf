variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "glue_job_name" {
  description = "Name of the Glue job"
  default     = "lottery-sales-etl-job"
}

variable "glue_crawler_name" {
  description = "Name of the Glue crawler"
  default     = "lottery-sales-crawler"
}

variable "script_s3_path" {
  description = "S3 path to the Glue ETL script"
  default     = "s3://projectbucketversionone/script.py"
}

variable "output_bucket" {
  description = "S3 bucket to store cleaned output"
  default     = "cdacprojectlsabucketterra"
}

variable "glue_role_arn" {
  description = "IAM Role ARN for Glue Job and Crawler"
  default     = "arn:aws:iam::379195343737:role/LabRole"
}

variable "glue_database_name" {
  description = "Name of the Glue database"
  default     = "etl_db"
}

variable "crawler_s3_path" {
  description = "S3 path for Glue Crawler to crawl"
  default     = "s3://cdacprojectlsabucketterra/"
}
