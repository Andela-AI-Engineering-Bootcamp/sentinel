output "ingest_api_endpoint" {
  value = "https://${aws_api_gateway_rest_api.ingest_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.prod.stage_name}/ingest"
}

output "ingest_api_key_id" {
  value = aws_api_gateway_api_key.ingest_key.id
}

output "ingest_lambda_name" {
  value = aws_lambda_function.ingest.function_name
}
