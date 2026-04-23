variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "sqs_queue_name" {
  type    = string
  default = "sentinel-analysis-jobs"
}

variable "aurora_cluster_arn" {
  type = string
}

variable "aurora_secret_arn" {
  type = string
}

variable "lambda_zip_paths" {
  type = map(string)
  default = {
    planner      = "../../backend/planner/planner_lambda.zip"
    normalizer   = "../../backend/normalizer/normalizer_lambda.zip"
    summarizer   = "../../backend/summarizer/summarizer_lambda.zip"
    investigator = "../../backend/investigator/investigator_lambda.zip"
    remediator   = "../../backend/remediator/remediator_lambda.zip"
  }
}

variable "bedrock_model_id" {
  type        = string
  default = "openai.gpt-oss-120b-1:0"
}

variable "bedrock_region" {
  type    = string
  default = "eu-west-1"
}

variable "root_cause_model" {
  type    = string
  default = "eu.amazon.nova-pro-v1:0"
}

variable "remediation_model" {
  type    = string
  default = "eu.amazon.nova-pro-v1:0"
}

variable "support_model" {
  type    = string
  default = "openai.gpt-oss-120b-1:0"
}
