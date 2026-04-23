# Guide 4 - Intel Service (App Runner)

## Objective
Deploy App Runner service for supporting LLM-assisted context analysis.

## Model Guidance
- Supporting analysis model: `openai.gpt-oss-120b-1:0`
- Bedrock region should match model availability.

## Build and Push Image

```bash
# Initialize Terraform (creates local state file)
terraform init

# Deploy only the ECR repository and IAM roles (not App Runner yet)
terraform apply -target=aws_ecr_repository.sentinel_intel -target=aws_iam_role.app_runner_role
```

Type `yes` when prompted. This creates:
- ECR repository for your Docker images
- IAM roles with proper permissions for App Runner

Save the ECR repository URL shown in the output - you'll need it in Step 2.

## Step 2: Build and Deploy the Researcher

Now we'll build the Docker container and deploy it to App Runner.

```bash
# Navigate to the backend/researcher directory
uv run deploy.py
```

This script will:
1. Build a Docker image (with `--platform linux/amd64` for compatibility)
2. Push it to your ECR repository
3. Trigger an App Runner deployment
4. Wait for the deployment to complete (3-5 minutes)
5. Display your service URL when ready

**Important Note for Apple Silicon Mac Users:**
The deployment script automatically builds for `linux/amd64` architecture to ensure compatibility with AWS App Runner. This is why you'll see "Building Docker image for linux/amd64..." in the output.

When the Docker image push completes, you'll see:
```
✅ Docker image pushed successfully!
```


## Deploy Infra
```bash
cd ../../terraform/4_intel
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

## Save Outputs
Add to `.env`:
- `SENTINEL_INTEL_URL`

## Verify
```bash
cd ../../backend/intel
uv run test_intel.py
```

## Next
Continue to [5_database.md](5_database.md).
