terraform {
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

# Data source for current caller identity
data "aws_caller_identity" "current" {}

# ECR repository for the researcher Docker image
resource "aws_ecr_repository" "sentinel_intel" {
  name                 = "sentinel-intel"
  image_tag_mutability = "MUTABLE"
  force_delete         = true # Allow deletion even with images

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = {
    Project = "sentinel"
    Part    = "4"
  }
}

resource "aws_iam_role" "app_runner_access_role" {
  name = "sentinel-apprunner-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { 
        Service = "build.apprunner.amazonaws.com" 
      }      
    },
    {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "tasks.apprunner.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project = "sentinel"
    Part    = "4"
  }
}

resource "aws_iam_role_policy_attachment" "app_runner_ecr_access" {
  role       = aws_iam_role.app_runner_access_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

# IAM role for App Runner instance (runtime access to AWS services)
resource "aws_iam_role" "app_runner_instance_role" {
  name = "sentinel-app-runner-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "tasks.apprunner.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Project = "sentinel"
    Part    = "4"
  }
}

# Policy for App Runner instance to access Bedrock
resource "aws_iam_role_policy" "app_runner_instance_bedrock_access" {
  name = "sentinel-app-runner-instance-bedrock-policy"
  role = aws_iam_role.app_runner_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:ListFoundationModels"
        ]
        Resource = "*"
      }
    ]
  })
}


resource "aws_apprunner_service" "sentinel_intel" {
  service_name = var.service_name

  source_configuration {
    auto_deployments_enabled = false

    authentication_configuration {
      access_role_arn = aws_iam_role.app_runner_access_role.arn
    }

    image_repository {
      image_identifier = "${aws_ecr_repository.sentinel_intel.repository_url}:latest"
      image_configuration {
        port = "8000"
        runtime_environment_variables = {
          SENTINEL_INGEST_API_KEY = var.sentinel_ingest_api_key
          SENTINEL_INGEST_API_ENDPOINT = var.sentinel_ingest_api_endpoint
          BEDROCK_MODEL_ID = var.bedrock_model_id
          BEDROCK_REGION = var.bedrock_region
        }
      }
      image_repository_type = "ECR"
    }
  }

  instance_configuration {
    cpu               = "1 vCPU"
    memory            = "2 GB"
    instance_role_arn = aws_iam_role.app_runner_instance_role.arn
  }

  tags = {
    Project = "sentinel"
    Part    = "4"
  }

}
