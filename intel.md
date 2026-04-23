# Sentinel Project Intelligence

## Overview
Sentinel is a production-leaning AI incident intelligence platform built using Alex-style architecture. The goal is to accept incident inputs and produce structured summaries, severity assessments, root cause analysis, and remediation steps, all displayed on an AWS-deployed dashboard.

## Core Components
- **Backend Agents**: Modular agents for planning, normalizing, summarizing, investigating, and remediating incidents.
- **Frontend**: A Next.js-based dashboard for viewing incident data and analysis.
- **Infrastructure**: Terraform-managed AWS resources (SageMaker, Lambda, etc.).
- **Guardrails**: Integrated strategies for prompt injection detection, evidence grounding, and confidence-aware fallbacks.

## Directory Structure
- `backend/`: Contains the AI agent modules.
- `frontend/`: Next.js web application.
- `terraform/`: Infrastructure as Code for AWS deployment.
- `guides/`: Detailed documentation for each phase of the project.
- `assets/`: Project images and static assets.

## Local Development

### Prerequisites
- **Python**: Installed and managed via `uv`.
- **Node.js**: Installed for the Next.js frontend.
- **Environment**: Create a `.env` in the root and a `frontend/.env.local` based on `.env.example`.

### Environment Configuration

To get the application fully functional locally, you need to populate `.env` with keys from various providers.

#### 1. LLM Provider (Choose One)
Sentinel supports either OpenRouter or AWS Bedrock for its agent orchestra.
- **OpenRouter (Recommended for local)**:
    - Set `USE_OPEN_ROUTER=true` and `USE_BEDROCK=false`.
    - Get an API key from [OpenRouter](https://openrouter.ai/keys).
    - Default model is `openai/gpt-4o-mini`.
- **AWS Bedrock**:
    - Set `USE_OPEN_ROUTER=false` and `USE_BEDROCK=true`.
    - Requires AWS credentials with Bedrock access (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
    - Models like `us.amazon.nova-pro-v1:0` must be enabled in your AWS console.

#### 2. Authentication (Clerk)
- **Production-like flow**:
    - Sign up at [Clerk](https://dashboard.clerk.com/).
    - Create an application and get the **Publishable Key** and **Secret Key**.
    - `CLERK_JWKS_URL` can be found in your Clerk dashboard under **API Keys -> Advanced**.
- **Local Bypass**:
    - If you leave Clerk keys empty, `scripts/run_local.py` will automatically set `AUTH_DISABLED=true`, allowing you to use the app without logging in.

#### 3. Notifications (Resend)
- Used for sending follow-up reminders.
- Get an API key from [Resend](https://resend.com/api-keys).
- You can use the `onboarding@resend.dev` address for testing.

#### 4. AWS Infrastructure (Optional for basic local run)
- `S3_BUCKET`: Required if you intend to test document uploads or PDF exports that rely on S3.
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`: Required for Bedrock or S3 interactions.

### Starting Applications
The easiest way to start both the backend and frontend together is using the provided orchestrator script:

```bash
cd scripts
uv run run_local.py
```

- **Backend**: `http://localhost:8000` (FastAPI/Uvicorn)
- **Frontend**: `http://localhost:3000` (Next.js Dev Server)

#### Manual Backend Start
```bash
cd backend
uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
```

#### Manual Frontend Start
```bash
cd frontend
npm install
npm run dev
```

---

## Intelligence by File

### [AGENTS.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/AGENTS.md)
- **Patterns**: Follows Alex-style for rapid production delivery.
- **Tools**: `uv` for Python management, Nova Pro for high-level reasoning (investigator, remediator), GPT OSS 120B for support.
- **Workflow**: Incremental diagnosis, independent Terraform directories with local state.
- **Agent Modules**: Planner (orchestrator), Normalizer (cleaner), Summarizer (narrative), Investigator (root cause), Remediator (actions).

### [SENTINEL_HANDOVER.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/SENTINEL_HANDOVER.md)
- **Status**: Scaffold created, 8-guide framework in place.
- **Next Steps**: Guide 1 (IAM policies), Terraform implementation, API/Planner implementation.

### [gameplan.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/gameplan.md)
- **Goal**: Build/deploy by Friday.
- **MVP Outcomes**: Incident input -> Summary/Severity/Root Cause/Remediation -> Dashboard -> AWS Deployment.
- **Execution Order**: 8-step guide sequence (permissions to enterprise).
- **Guardrail Strategy**: Prompt injection removal in Normalizer, evidence extraction, confidence-aware fallback.
- **Delivery Focus**: Coherent vertical slice, cost monitoring, deterministic fallbacks.

## Guides and Architecture

### [1_permissions.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/1_permissions.md)
- **Focus**: Minimum IAM permissions for deployment.
- **Key Services**: SageMaker, Bedrock, Lambda, API Gateway, RDS, SQS, EventBridge, CloudWatch, S3, App Runner, Secrets Manager.

### [2_sagemaker.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/2_sagemaker.md)
- **Focus**: Serverless embeddings endpoint deployment.
- **Endpoint Name**: `sentinel-embedding-endpoint`.

### [3_ingestion.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/3_ingestion.md)
- **Focus**: Incident ingestion pipeline (Lambda + API Gateway).
- **Verification**: `backend/ingest/test_ingest.py`.

### [4_intel.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/4_intel.md)
- **Focus**: Intel Service (App Runner) for supporting LLM context analysis.
- **Model**: `openai.gpt-oss-120b-1:0`.

### [5_database.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/5_database.md)
- **Focus**: Aurora Serverless v2 with Data API.
- **Initialization**: Migration, seeding, and verification scripts in `backend/database`.

### [6_agents.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/6_agents.md)
- **Focus**: Planner and specialist agents orchestration via SQS.
- **Models**: Root cause and remediation use `us.amazon.nova-pro-v1:0`. Supporting analysis uses `openai.gpt-oss-120b-1:0`.
- **Guardrails**: Prompt injection filtering, evidence extraction, grounding checks.

### [7_frontend.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/7_frontend.md)
- **Focus**: Dashboard + API Layer deployment with Clerk authentication.
- **Tools**: Next.js, Clerk for auth, `deploy.py` for frontend assets.

### [8_enterprise.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/8_enterprise.md)
- **Focus**: Operational hardening, CloudWatch dashboards, and alarms.
- **Cost Discipline**: `scripts/check_costs.py`.

### [agent_architecture.md](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/guides/agent_architecture.md)
- **Roles**: Planner (orchestrator), Normalizer (guardrails/evidence), Summarizer (severity), Investigator (root cause), Remediator (actions).
- **Workflow**: Sequence diagram involving user, planner, specialist agents, and Aurora.

### [backend/common/models.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/common/models.py)
- **Data Models**: Defines `IncidentInput`, `GuardrailReport`, `NormalizedIncident`, `IncidentSummary`, `RootCauseAnalysis`, `RemediationPlan`, and the final `IncidentAnalysis`.
- **Severity & Confidence**: Uses `Literal` for `low`, `medium`, `high`, `critical` (Severity) and `low`, `medium`, `high` (Confidence).
- **Extensibility**: Includes models for clarification questions, integrations, and digest requests.

### [backend/common/pipeline.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/common/pipeline.py)
- **Orchestration**: `run_job` handles the sequence of agent calls: Normalizer -> Summarizer -> Investigator -> Remediator.
- **Integration**: Dispatches notifications via `_fire_integrations` for `high` or `critical` severity incidents.
- **Persistence**: Maps agent outputs to database records, including seeding granular remediation actions.

### [backend/common/store.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/common/store.py)
- **Functionality**: Comprehensive database interface for Aurora/SQLite, managing incidents, jobs, remediation actions, integrations, and user preferences.

### [backend/normalizer/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/normalizer/)
- **Purpose**: Sanitizes raw incident text and extracts evidence snippets using common guardrail utilities.

### [backend/summarizer/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/summarizer/)
- **Purpose**: Produces concise summaries and severity classifications.
- **Mechanism**: Prefers Bedrock LLM with a fallback to rule-based heuristics.

### [backend/investigator/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/investigator/)
- **Purpose**: Conducts root-cause analysis (RCA).
- **Mechanism**: Utilizes Nova Pro for deep analysis, supports streaming for real-time UI updates, and enforces evidence-based grounding.

### [backend/remediator/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/remediator/)
- **Purpose**: Recommends prioritized remediation actions and follow-up checks.
- **Mechanism**: Enriches analysis with operator context (clarifications) when available and ensures actions are linked to discovered evidence.

### [backend/reports/digest.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/reports/digest.py)
- **Purpose**: Generates periodic incident digests, summarizing operational metrics and key incidents over a given timeframe (e.g., 7 days).

### [backend/scheduler/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/scheduler/)
- **Purpose**: A Lambda-based scheduler (likely triggered by EventBridge) that orchestrates periodic tasks such as sending pending follow-up reminders and generating digests.

### [backend/common/pdf_report.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/common/pdf_report.py)
- **Purpose**: Utilities for rendering detailed incident analysis reports into printable PDF documents using the `fpdf2` library.

## Frontend Architecture

### [frontend/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/frontend/)
- **Framework**: Next.js using the Pages Router.
- **Authentication**: Integrated with Clerk for SaaS-style auth. It supports an `AUTH_DISABLED` mode for local development.
- **Visuals**: Modern, "card-elevated" aesthetic with dark mode support. Utilizes `recharts` for incident data visualization.

### Key Pages
- **[index.js](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/frontend/pages/index.js)**: The "Analyze" page where users submit raw incident logs.
- **[dashboard.js](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/frontend/pages/dashboard.js)**: The main operational view for reviewing past runs, viewing log statistics, and deep-diving into AI-generated analysis.
- **[settings.js](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/frontend/pages/settings.js)**: Configuration for integrations and user preferences.

### Core Components
- **AppShell**: Provides consistent navigation and layout across the application.
- **IncidentInput**: Handles the submission and validation of log text.
- **AnalysisReport**: Renders the combined output of the agent orchestra (Summary, RCA, Remediation).
- **LogDataCharts**: Displays visual metrics derived from the incident data.

## API Layer Intelligence

### [backend/api/main.py](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/backend/api/main.py)
- **Framework**: FastAPI with asynchronous support.
- **Endpoint Categories**:
    - **Incident Ingestion**: Supports both background (`POST /api/incidents`) and synchronous (`POST /api/incidents/analyze-sync`) analysis.
    - **Operational View**: Provides detailed job lists and enriched single-job views, including log statistics and similar incident analysis.
    - **Real-time Updates**: Implements Server-Sent Events (SSE) for both pipeline stage tracking and live model investigation streams.
    - **Reporting**: Offers structured exports in JSON and PDF formats.
    - **Analytics**: Calculates operational metrics like Mean Time To Resolve (MTTR).
    - **Remediation Management**: Enables tracking, assigning, and updating remediation actions.
    - **Interactive Assistant**: Includes an action-specific chat interface powered by Bedrock, allowing engineers to ask for guidance on specific tasks.
    - **Follow-ups & Clarifications**: Manages automated email reminders and a targeted Q&A workflow to refine remediation plans.
- **External Integration**: Interfaces with the Clerk Backend API to fetch team member information and handle authenticated user contexts.

## Infrastructure and Scripts

### [terraform/](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/terraform/)
- **Structure**: Independent directories (2-8) matching the project guides.
- **Components**: Covers SageMaker, Ingestion (Lambda/API Gateway), Intel Service (App Runner), Database (Aurora), Agent Orchestra (SQS/Lambda), Frontend (S3/CloudFront), and Enterprise Monitoring (CloudWatch).
- **Pattern**: Uses `terraform.tfvars.example` for environment-specific configuration.

## Infrastructure Implementation Details

### [terraform/6_agents/main.tf](file:///f:/CODE/Andela-AI-Engineering-Bootcamp/sentinel/terraform/6_agents/main.tf)
- **Asynchronous Processing**: Uses AWS SQS with a Dead Letter Queue (DLQ) for resilient job handling.
- **Trigger**: The `planner` Lambda is mapped to the SQS queue with a `batch_size` of 1, ensuring sequential or controlled parallel analysis.
- **Shared Identity**: A single IAM role grants all agents access to:
    - **SQS**: Messaging operations.
    - **RDS Data API**: Database interactions with Aurora Serverless.
    - **Bedrock**: LLM invocation (`InvokeModel` and `Converse`).
    - **Secrets Manager**: Retrieval of database credentials.
- **Deployment Pattern**: Uses Terraform's `for_each` to deploy multiple agent Lambdas from a common configuration map, managing variations in timeout and memory.
