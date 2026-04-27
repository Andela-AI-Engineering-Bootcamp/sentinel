variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "db_name" {
  type    = string
  default = "sentinel"
}

variable "db_username" {
  type    = string
  default = "sentinel_admin"
}

variable "min_capacity" {
  type    = number
  default = 0.5
}

variable "max_capacity" {
  type    = number
  default = 1.0
}

variable "cluster_identifier" {
  type    = string
  default = "sentinel-aurora-cluster"
}
