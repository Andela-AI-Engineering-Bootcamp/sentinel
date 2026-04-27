terraform {
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

resource "aws_iam_role" "ingest_lambda_role" {
  name = "sentinel-ingest-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "ingest" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.ingest_lambda_role.arn
  runtime       = "python3.12"
  handler       = "ingest_lambda.lambda_handler"
  filename      = var.lambda_zip_path
  timeout       = 30
  memory_size   = 512
}

resource "aws_api_gateway_rest_api" "ingest_api" {
  name = var.api_name
}

resource "aws_api_gateway_resource" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.ingest_api.id
  parent_id   = aws_api_gateway_rest_api.ingest_api.root_resource_id
  path_part   = "ingest"
}

resource "aws_api_gateway_method" "post_ingest" {
  rest_api_id      = aws_api_gateway_rest_api.ingest_api.id
  resource_id      = aws_api_gateway_resource.ingest.id
  http_method      = "POST"
  authorization    = "NONE"
  api_key_required = true
}

resource "aws_api_gateway_integration" "post_ingest" {
  rest_api_id             = aws_api_gateway_rest_api.ingest_api.id
  resource_id             = aws_api_gateway_resource.ingest.id
  http_method             = aws_api_gateway_method.post_ingest.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.ingest.invoke_arn
}

resource "aws_lambda_permission" "allow_api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.ingest_api.execution_arn}/*/*"
}

resource "aws_api_gateway_deployment" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.ingest_api.id

  depends_on = [aws_api_gateway_integration.post_ingest]

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.ingest.id,
      aws_api_gateway_method.post_ingest.id,
      aws_api_gateway_integration.post_ingest.id,
    ]))
  }
}

resource "aws_api_gateway_stage" "prod" {
  deployment_id = aws_api_gateway_deployment.ingest.id
  rest_api_id   = aws_api_gateway_rest_api.ingest_api.id
  stage_name    = "prod"
}

resource "aws_api_gateway_api_key" "ingest_key" {
  name    = "sentinel-ingest-api-key"
  enabled = true
}

resource "aws_api_gateway_usage_plan" "ingest_plan" {
  name = "sentinel-ingest-usage-plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.ingest_api.id
    stage  = aws_api_gateway_stage.prod.stage_name
  }
}

resource "aws_api_gateway_usage_plan_key" "ingest_key_attach" {
  key_id        = aws_api_gateway_api_key.ingest_key.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.ingest_plan.id
}
