#####################################################
# Outputs for AWS Glue Job and Crawler Configuration
#####################################################

# Output the Glue Job Name
output "glue_job_name" {
  value       = aws_glue_job.transform_job.name
  description = "The name of the AWS Glue ETL Job for data transformation"
}

# Output the IAM Role used by Glue resources
output "glue_job_role_arn" {
  value       = aws_glue_job.transform_job.role_arn
  description = "The IAM role ARN assigned to the AWS Glue Job"
}

# Output the script location used by the Glue Job
output "glue_script_location" {
  value       = aws_glue_job.transform_job.command[0].script_location
  description = "The S3 location of the transformation script used in the Glue Job"
}

# Output the Glue Job Timeout
output "glue_job_timeout" {
  value       = aws_glue_job.transform_job.timeout
  description = "The timeout duration in minutes for the Glue Job"
}

# Output the number of workers assigned to the Glue Job
output "glue_job_number_of_workers" {
  value       = aws_glue_job.transform_job.number_of_workers
  description = "The number of DPUs allocated to the Glue Job"
}

# Output the worker type used in the Glue Job
output "glue_job_worker_type" {
  value       = aws_glue_job.transform_job.worker_type
  description = "The type of workers used by the Glue Job (e.g., G.1X or G.2X)"
}

# Output the Glue Crawler Name
output "crawler_name" {
  value       = aws_glue_crawler.transformed_data_crawler.name
  description = "The name of the AWS Glue Crawler for transformed data"
}

# Output the S3 path used as source for the Crawler
output "crawler_s3_target_path" {
  value       = aws_glue_crawler.transformed_data_crawler.s3_target[0].path
  description = "The S3 path used as the data source for the transformed data crawler"
}

# Output the Glue Database where crawler stores metadata
output "crawler_database_name" {
  value       = aws_glue_crawler.transformed_data_crawler.database_name
  description = "The Glue database name where the crawler writes metadata"
}