# Guide 6 - Agent Orchestra

## Objective
Deploy planner + 4 specialist agents with SQS orchestration.

## Model Policy
- Root cause: `us.amazon.nova-pro-v1:0`
- Remediation: `us.amazon.nova-pro-v1:0`
- Supporting analysis: `openai.gpt-oss-120b-1:0`

## Guardrails Included
- Prompt injection filtering in Normalizer
- Evidence extraction and grounding checks
- Low-confidence fallback when evidence is weak

## Deploy Infra
```bash
cd backend
uv run package_docker.py

cd terraform/6_agents
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

## Local Validation
```bash
cd ../../backend
uv run test_simple.py
uv run test_full.py
```

## Next
Continue to [7_frontend.md](7_frontend.md).
