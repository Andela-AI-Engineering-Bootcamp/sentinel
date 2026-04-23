variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "frontend_bucket_name" {
  type = string
}

variable "api_lambda_zip" {
  type    = string
  default = "../../backend/api/api_lambda.zip"
}

variable "api_lambda_name" {
  type    = string
  default = "sentinel-api"
}

# Clerk validation happens in Lambda, not at API Gateway level
variable "clerk_jwks_url" {
  description = "Clerk JWKS URL for JWT validation in Lambda"
  type        = string
}

variable "clerk_issuer" {
  description = "Clerk issuer URL (kept for Lambda environment)"
  type        = string
  default     = ""  # Not actually used but kept for backwards compatibility
}