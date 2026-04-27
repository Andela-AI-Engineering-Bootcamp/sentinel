terraform {
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

resource "aws_iam_role" "sagemaker_execution" {
  name = "sentinel-sagemaker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_full_access" {
  role       = aws_iam_role.sagemaker_execution.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

resource "aws_sagemaker_model" "embedding_model" {
  name               = var.model_name
  execution_role_arn = aws_iam_role.sagemaker_execution.arn

  primary_container {
    image = var.huggingface_image_uri

    environment = {
      HF_MODEL_ID = var.hf_model_id
      HF_TASK     = "feature-extraction"
    }
  }

  depends_on = [aws_iam_role_policy_attachment.sagemaker_full_access]
}

resource "aws_sagemaker_endpoint_configuration" "embedding_config" {
  name = var.endpoint_config_name

  production_variants {
    variant_name           = "AllTraffic"
    model_name             = aws_sagemaker_model.embedding_model.name
    serverless_config {
      max_concurrency = 10
      memory_size_in_mb = 3072
    }
  }
}

resource "time_sleep" "wait_for_iam_propagation" {
  depends_on = [
    aws_iam_role_policy_attachment.sagemaker_full_access
  ]
  
  create_duration = "15s"
}

resource "aws_sagemaker_endpoint" "embedding_endpoint" {
  name                 = var.endpoint_name
  endpoint_config_name = aws_sagemaker_endpoint_configuration.embedding_config.name

  depends_on = [
    time_sleep.wait_for_iam_propagation
  ]
}
