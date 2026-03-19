output "batch_bucket_name" {
  description = "S3 bucket name for batch data"
  value       = aws_s3_bucket.batch_data.id
}

output "batch_bucket_arn" {
  description = "S3 bucket ARN for batch data"
  value       = aws_s3_bucket.batch_data.arn
}

output "streaming_bucket_name" {
  description = "S3 bucket name for streaming data"
  value       = aws_s3_bucket.streaming_data.id
}

output "streaming_bucket_arn" {
  description = "S3 bucket ARN for streaming data"
  value       = aws_s3_bucket.streaming_data.arn
}

output "kinesis_stream_name" {
  description = "Kinesis stream name"
  value       = aws_kinesis_stream.events.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.events.arn
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.s3_processor.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.s3_processor.arn
}

output "lambda_role_arn" {
  description = "Lambda execution role ARN"
  value       = aws_iam_role.lambda_execution.arn
}

output "databricks_instance_profile_arn" {
  description = "Instance profile ARN for Databricks job clusters"
  value       = aws_iam_instance_profile.databricks_instance_profile.arn
}
