variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "databricks-lambda-arichitecture"
}

variable "databricks_host" {
  description = "Databricks workspace host"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  type        = string
  description = "Databricks personal access token"
  sensitive   = true
}

variable "lambda_timeout" {
  type        = number
  default     = 30
  description = "Lambda function timeout in seconds"
}

variable "s3_bucket_retention_days" {
  type        = number
  default     = 30
  description = "S3 bucket object expiration days"
}
