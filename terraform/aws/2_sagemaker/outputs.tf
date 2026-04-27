output "sagemaker_endpoint_name" {
  value = aws_sagemaker_endpoint.embedding_endpoint.name
}

output "sagemaker_model_name" {
  value = aws_sagemaker_model.embedding_model.name
}
