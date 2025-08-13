output "glue_job_name" {
  value = aws_glue_job.lottery_sales_etl_job.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.etl_crawler.name
}
