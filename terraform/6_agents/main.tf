terraform {
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

resource "aws_sqs_queue" "dlq" {
  name = "${var.sqs_queue_name}-dlq"

  tags = {
    Project = "sentinel"
    Part    = "6"
  }
}

resource "aws_sqs_queue" "jobs" {
  name = var.sqs_queue_name
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 86400  # 1 day
  receive_wait_time_seconds = 10     # Long polling
  visibility_timeout_seconds = 910   # 15 minutes + 10 seconds buffer (matches Planner Lambda timeout)
  

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Project = "sentinel"
    Part    = "6"
  }
}

resource "aws_iam_role" "agent_role" {
  name = "sentinel-agent-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = "sentinel"
    Part    = "6"
  }
}

resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.agent_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "agent_inline" {
  name = "sentinel-agent-inline-policy"
  role = aws_iam_role.agent_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"]
        Resource = [aws_sqs_queue.jobs.arn, aws_sqs_queue.dlq.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["rds-data:ExecuteStatement", "rds-data:BatchExecuteStatement", "rds-data:BeginTransaction", "rds-data:CommitTransaction", "rds-data:RollbackTransaction"]
        Resource = [var.aurora_cluster_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [var.aurora_secret_arn]
      },
      {
        Effect = "Allow"
        Action = ["bedrock:InvokeModel", "bedrock:Converse"]
        Resource = "*"
      }
    ]
  })
}

locals {
  function_settings = {
    planner      = { handler = "lambda_handler.lambda_handler", timeout = 300, memory = 2048 }
    normalizer   = { handler = "lambda_handler.lambda_handler", timeout = 60, memory = 512 }
    summarizer   = { handler = "lambda_handler.lambda_handler", timeout = 60, memory = 768 }
    investigator = { handler = "lambda_handler.lambda_handler", timeout = 120, memory = 1024 }
    remediator   = { handler = "lambda_handler.lambda_handler", timeout = 120, memory = 1024 }
  }
}

resource "aws_lambda_function" "agents" {
  for_each = local.function_settings

  function_name = "sentinel-${each.key}"
  role          = aws_iam_role.agent_role.arn
  runtime       = "python3.12"
  handler       = each.value.handler
  filename      = var.lambda_zip_paths[each.key]
  timeout       = each.value.timeout
  memory_size   = each.value.memory

  environment {
    variables = {
      BEDROCK_REGION            = var.bedrock_region
      BEDROCK_MODEL_ROOT_CAUSE  = var.root_cause_model
      BEDROCK_MODEL_REMEDIATION = var.remediation_model
      BEDROCK_MODEL_SUPPORT     = var.support_model
      AURORA_CLUSTER_ARN        = var.aurora_cluster_arn
      AURORA_SECRET_ARN         = var.aurora_secret_arn
      SQS_QUEUE_URL             = aws_sqs_queue.jobs.id
    }
  }

  tags = {
    Project = "sentinel"
    Part    = "6"
  }
}

resource "aws_lambda_event_source_mapping" "planner_sqs" {
  event_source_arn = aws_sqs_queue.jobs.arn
  function_name    = aws_lambda_function.agents["planner"].arn
  batch_size       = 1
  enabled          = true
}
