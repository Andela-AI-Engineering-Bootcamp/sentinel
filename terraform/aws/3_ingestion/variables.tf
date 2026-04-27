variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "lambda_zip_path" {
  type    = string
  default = "../../backend/ingest/ingest_lambda.zip"
}

variable "lambda_function_name" {
  type    = string
  default = "sentinel-ingest"
}

variable "api_name" {
  type    = string
  default = "sentinel-ingest-api"
}
