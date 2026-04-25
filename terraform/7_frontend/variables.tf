variable "aws_region" {
  description = "AWS region used by the API"
  type    = string
}

variable "frontend_bucket_name" {
  description = "Frontend bucket name used by the API"
  type = string
}

variable "api_lambda_zip" {
  type    = string
  default = "../../backend/api/api_lambda.zip"
}

variable "api_lambda_name" {
  description = "API Lambda name used by the API"
  type    = string
}

# Clerk validation happens in Lambda, not at API Gateway level
variable "clerk_jwks_url" {
  description = "Clerk JWKS URL for JWT validation in Lambda"
  type        = string
}

variable "clerk_issuer" {
  description = "Clerk issuer URL (kept for Lambda environment)"
  type        = string
}

# Clerk validation happens in Lambda, not at API Gateway level
variable "bedrock_model_id" {
  description = "Bedrock model ID used by the API"
  type        = string
}

variable "bedrock_region" {
  description = "Bedrock region used by the API"
  type        = string
}

variable "openrouter_api_key" {
  description = "OpenRouter API key used by the API"
  type        = string
}

variable "openrouter_model" {
  description = "OpenRouter model used by the API"
  type        = string
}

variable "openrouter_base_url" {  
  description = "OpenRouter base URL used by the API"
  type        = string
}

variable "resend_api_key" {
  description = "Resend API key used by the API"
  type        = string
}

variable "resend_from" {
  description = "Resend from email used by the API"
  type        = string
}

variable "reminder_interval_seconds" {
  description = "Reminder interval seconds used by the API"
  type        = string
}