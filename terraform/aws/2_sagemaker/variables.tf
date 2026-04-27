variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "endpoint_name" {
  type    = string
  default = "sentinel-embedding-endpoint"
}

variable "model_name" {
  type    = string
  default = "sentinel-embedding-model"
}

variable "endpoint_config_name" {
  type    = string
  default = "sentinel-embedding-endpoint-config"
}

variable "hf_model_id" {
  type    = string
  default = "sentence-transformers/all-MiniLM-L6-v2"
}

variable "huggingface_image_uri" {
  type    = string
  default = "763104351884.dkr.ecr.eu-west-1.amazonaws.com/huggingface-pytorch-inference:2.1.0-transformers4.37.0-cpu-py310-ubuntu22.04"
}
