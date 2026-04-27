output "aurora_cluster_arn" {
  description = "ARN of the RDS instance (named aurora_cluster_arn for compatibility)"
  value       = aws_db_instance.rds.arn
}

output "aurora_cluster_endpoint" {
  description = "Endpoint for the RDS instance"
  value       = aws_db_instance.rds.address
}

output "aurora_secret_arn" {
  description = "ARN of the Secrets Manager secret containing database credentials"
  value       = aws_secretsmanager_secret.db_credentials.arn
}

output "database_name" {
  description = "Name of the database"
  value       = aws_db_instance.rds.db_name
}

output "lambda_role_arn" {
  description = "ARN of the IAM role for Lambda functions to access RDS"
  value       = aws_iam_role.lambda_rds_role.arn
}

output "data_api_enabled" {
  description = "Status of Data API"
  value       = "Conditional (Standard RDS)"
}

output "setup_instructions" {
  description = "Instructions for setting up the database"
  value = <<-EOT
    
    ✅ Standard RDS PostgreSQL instance deployed! (Free Tier Eligible)
    
    Database Details:
    - Instance: ${aws_db_instance.rds.identifier}
    - Database: ${aws_db_instance.rds.db_name}
    - Engine: PostgreSQL ${aws_db_instance.rds.engine_version}
    
    Add the following to your .env file:
    AURORA_CLUSTER_ARN=${aws_db_instance.rds.arn}
    AURORA_SECRET_ARN=${aws_secretsmanager_secret.db_credentials.arn}
    AURORA_DATABASE=${aws_db_instance.rds.db_name}
    
    Note: If the Data API is not supported for this instance type, 
    the backend will fall back to a standard connection.
    
    To set up the database schema:
    cd backend/database
    uv run run_migrations.py
    
    To load sample data:
    uv run reset_db.py --with-test-data
  EOT
}
