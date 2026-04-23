terraform {
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

# Data sources
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# Reference Part 5 Database resources
data "terraform_remote_state" "database" {
  backend = "local"
  config = {
    path = "../5_database/terraform.tfstate"
  }
}

# Reference Part 6 Agents resources
data "terraform_remote_state" "agents" {
  backend = "local"
  config = {
    path = "../6_agents/terraform.tfstate"
  }
}

locals {
  name_prefix = "sentinel"

  common_tags = {
    Project     = "sentinel"
    Part        = "7_frontend"
    ManagedBy   = "terraform"
  }
}


resource "aws_s3_bucket" "frontend" {
  bucket = var.frontend_bucket_name
  tags   = local.common_tags
}



resource "aws_s3_bucket_website_configuration" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "404.html"
  }
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "frontend_public" {
  bucket = aws_s3_bucket.frontend.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = "*",
      Action = ["s3:GetObject"],
      Resource = ["${aws_s3_bucket.frontend.arn}/*"]
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.frontend]
}

resource "aws_iam_role" "api_lambda_role" {
  name = "sentinel-api-lambda-role"
  tags = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "api_lambda_basic" {
  role       = aws_iam_role.api_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "api" {
  function_name = var.api_lambda_name
  role          = aws_iam_role.api_lambda_role.arn
  runtime       = "python3.12"
  handler       = "api.lambda_handler.handler"
  filename      = var.api_lambda_zip
  source_code_hash = filebase64sha256(var.api_lambda_zip)
  timeout       = 60
  memory_size   = 1024
  tags = local.common_tags

  environment {
    variables = {
      # Database configuration from Part 5
      AURORA_CLUSTER_ARN = data.terraform_remote_state.database.outputs.aurora_cluster_arn
      AURORA_SECRET_ARN  = data.terraform_remote_state.database.outputs.aurora_secret_arn
      AURORA_DATABASE    = data.terraform_remote_state.database.outputs.database_name
      DEFAULT_AWS_REGION = var.aws_region

      # SQS configuration from Part 6
      SQS_QUEUE_URL = data.terraform_remote_state.agents.outputs.sqs_queue_url

      # Clerk configuration for JWT validation
      CLERK_JWKS_URL = var.clerk_jwks_url
      CLERK_ISSUER   = var.clerk_issuer

      # Runtime configuration for the current SQLite-backed API implementation
      SENTINEL_DB_PATH = "/tmp/sentinel.db"
      AUTH_DISABLED    = "false"

      # CORS configuration consumed by the FastAPI app
      ALLOWED_ORIGINS = "http://localhost:3000,https://${aws_cloudfront_distribution.frontend.domain_name}"
    }
  }

}

resource "aws_apigatewayv2_api" "api" {
  name          = "sentinel-http-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api_lambda" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "catch_all" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.api_lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "allow_http_api" {
  statement_id  = "AllowHttpApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}

resource "aws_cloudfront_distribution" "frontend" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"
  tags                = local.common_tags
  comment             = "Sentinel Frontend"

  origin {
    domain_name = aws_s3_bucket_website_configuration.frontend.website_endpoint
    origin_id   = "sentinel-frontend-origin"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    allowed_methods  = ["GET", "HEAD", "OPTIONS"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = "sentinel-frontend-origin"

    forwarded_values {
      query_string = true
      cookies {
        forward = "none"
      }
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 3600
    max_ttl                = 86400
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
