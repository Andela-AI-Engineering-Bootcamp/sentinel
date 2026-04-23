# Guide 7 - Frontend + API Deployment

## Objective
Run and deploy the dashboard + API layer with Clerk authentication.

## Clerk Setup
Configure `frontend/.env.local`:
- `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY`
- `CLERK_SECRET_KEY`
- `NEXT_PUBLIC_CLERK_SIGN_IN_URL=/sign-in`
- `NEXT_PUBLIC_CLERK_SIGN_UP_URL=/sign-up`

Configure backend `.env`:
- `CLERK_ISSUER`
- `CLERK_JWKS_URL`
- `AUTH_DISABLED=false`

## Local Run
```bash
cd frontend
npm install

cd scripts
uv run run_local.py
```

Frontend: `http://localhost:3000`
Backend: `http://localhost:8000`

If Clerk env vars are absent, local runner defaults backend to `AUTH_DISABLED=true` for development convenience.

## Package API Lambda
```bash
cd ../backend/api
uv run package_docker.py
```

## Deploy Infra
```bash
cd ../../terraform/7_frontend
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

## Deploy Frontend Assets
```bash
export SENTINEL_FRONTEND_BUCKET={bucketname}

cd ../../scripts
uv run deploy.py
```

## Verify Auth
1. Open CloudFront URL
2. Sign in via Clerk
3. Submit incident on `/`
4. Confirm `/dashboard` only shows incidents for that Clerk user

## Next
Continue to [8_enterprise.md](8_enterprise.md).
