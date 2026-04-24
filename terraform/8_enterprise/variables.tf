variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "planner_lambda_name" {
  type    = string
  default = "sentinel-planner"
}

variable "api_lambda_name" {
  type    = string
  default = "sentinel-api"
}

variable "sqs_queue_name" {
  type    = string
  default = "sentinel-analysis-jobs"
}

variable "alarm_email" {
  type    = string
  default = ""
}

variable "bedrock_region" {
  description = "AWS region for Bedrock (may differ from main region)"
  type        = string
  default     = "eu-west-1"
}

variable "bedrock_model_id" {
  description = "Bedrock model ID to monitor (e.g., amazon.nova-pro-v1:0)"
  type        = string
  default     = "openai.gpt-oss-120b-1:0"
}