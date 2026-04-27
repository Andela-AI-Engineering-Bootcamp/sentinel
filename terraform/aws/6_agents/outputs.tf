output "sqs_queue_url" {
  description = "URL of the SQS queue for job submission"
  value = aws_sqs_queue.jobs.url
}

output "sqs_queue_arn" {
  description = "ARN of the SQS queue"
  value = aws_sqs_queue.jobs.arn
}

output "agent_lambda_names" {
  description = "Names of the agent Lambda functions"
  value = [for f in aws_lambda_function.agents : f.function_name]
}


output "setup_instructions" {
  description = "Instructions for testing the agents"
  value = <<-EOT
    
    ✅ Agent infrastructure deployed successfully!
    
    Lambda Functions:
    - Planner: ${aws_lambda_function.agents["planner"].function_name}
    - Normalizer: ${aws_lambda_function.agents["normalizer"].function_name}
    - Summarizer: ${aws_lambda_function.agents["summarizer"].function_name}
    - Investigator: ${aws_lambda_function.agents["investigator"].function_name}
    - Remediator: ${aws_lambda_function.agents["remediator"].function_name}
    
    SQS Queue: ${aws_sqs_queue.jobs.url}
    
    To test the system:
    - Run the simple integration test:
       cd ../../backend && uv run test_simple.py
    - Run the full integration test:
       cd ../../backend && uv run test_full.py

    Bedrock Model: ${var.bedrock_model_id}
    Region: ${var.bedrock_region}
  EOT
}