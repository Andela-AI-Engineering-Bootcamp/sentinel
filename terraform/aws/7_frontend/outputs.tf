output "cloudfront_url" {
  value = "https://${aws_cloudfront_distribution.frontend.domain_name}"
}

output "api_gateway_url" {
  value = aws_apigatewayv2_stage.default.invoke_url
}

output "frontend_bucket" {
  value = aws_s3_bucket.frontend.bucket
}

output "lambda_function_name" {
  description = "Name of the API Lambda function"
  value       = aws_lambda_function.api.function_name
}

output "setup_instructions" {
  description = "Instructions for completing the deployment"
  value = <<-EOT

    ✅ Frontend & API infrastructure deployed successfully!

    CloudFront URL: https://${aws_cloudfront_distribution.frontend.domain_name}
    API Gateway: ${aws_apigatewayv2_api.api.api_endpoint}
    S3 Bucket: ${aws_s3_bucket.frontend.id}
    Lambda Function: ${aws_lambda_function.api.function_name}

    Next steps:

    1. If you deployed manually (not using scripts/deploy.py):
       a. Build and deploy the frontend:
          cd frontend
          NEXT_PUBLIC_API_URL=${aws_apigatewayv2_stage.default.invoke_url} npm run build
          aws s3 sync out/ s3://${aws_s3_bucket.frontend.id}/ --delete
          # Or from the repo root:
          # cd scripts && uv run deploy.py

       b. Invalidate CloudFront cache:
          aws cloudfront create-invalidation \
            --distribution-id ${aws_cloudfront_distribution.frontend.id} \
            --paths "/*"

    2. Test the deployment:
       - Visit: https://${aws_cloudfront_distribution.frontend.domain_name}
       - Sign in with Clerk
       - Check API calls in Network tab

    3. Monitor in AWS Console:
       - CloudWatch Logs: /aws/lambda/${aws_lambda_function.api.function_name}
       - API Gateway metrics
       - CloudFront metrics

    To destroy: cd scripts && uv run destroy.py
  EOT
}
