variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "service_name" {
  type    = string
  default = "sentinel-intel"
}

variable "openai_api_key" {
  description = "OpenAI API key for the researcher agent"
  type        = string
  sensitive   = true
}

variable "sentinel_ingest_api_endpoint" {
  description = "Sentinel Ingest API endpoint from Part 3"
  type        = string
}

variable "sentinel_ingest_api_key" {
  description = "Sentinel Ingest API key from Part 3"
  type        = string
  sensitive   = true
}

variable "scheduler_enabled" {
  description = "Enable automated research scheduler"
  type        = bool
  default     = false
}

variable "bedrock_model_id" {
  description = "Bedrock model ID used by researcher (without the bedrock/ prefix)"
  type        = string
  default     = "openai.gpt-oss-120b-1:0"
}

variable "bedrock_region" {
  type    = string
  default = "eu-west-1"
}


