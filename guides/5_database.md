# Guide 5 - Database (Aurora Serverless v2)

## Objective
Deploy Aurora with Data API and initialize Sentinel data schema.

## Deploy Infra
```bash
cd terraform/5_database
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

## Save Outputs
Add to `.env`:
- `AURORA_CLUSTER_ARN`
- `AURORA_SECRET_ARN`
- `AURORA_DATABASE`

## Initialize Local DB Utilities
```bash
cd ../../backend/database
uv run run_migrations.py
uv run seed_data.py
uv run verify_database.py
```

## Next
Continue to [6_agents.md](6_agents.md).
