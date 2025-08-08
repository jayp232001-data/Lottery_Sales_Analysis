# AWS region
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
}

# IAM Role
variable "iam_role" {
  description = "IAM role ARN for Glue Job and Crawler"
  type        = string
}

# Raw S3 Path
variable "raw_data_s3_path" {
  description = "S3 path to raw data"
  type        = string
}

# Transformed S3 Path
variable "transformed_data_s3_path" {
  description = "S3 path for transformed data"
  type        = string
}

# Glue script location
variable "glue_script_s3_path" {
  description = "S3 path to Glue transformation script"
  type        = string
}

# Raw Glue Database
variable "raw_glue_database" {
  description = "Glue database name for raw data"
  type        = string
}

# Transformed Glue Database
variable "transformed_glue_database" {
  description = "Glue database name for transformed data"
  type        = string
}
