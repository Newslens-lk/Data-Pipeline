# Deploying to managed Airflow

The DAG and `include/` code are cloud-agnostic — `include/db/postgres_client.py`
picks the right object storage hook based on the Airflow connection type, so
the same code runs on any of these. What differs is deployment mechanics.

## AWS: MWAA (Managed Workflows for Apache Airflow)

- Upload `dags/`, `include/`, `plugins/` to the S3 bucket MWAA is configured
  to read (standard MWAA layout, not a special convention of this project).
- `requirements.txt` goes to the same bucket; MWAA installs it into the
  environment. Pin versions (already done here) — MWAA rebuilds on every
  requirements.txt change and unpinned deps cause non-reproducible builds.
- Create the `news_postgres` connection either via MWAA's UI or by seeding it
  through `MWAA__CORE__SQL_ALCHEMY_CONN`-style env config / Secrets Manager
  backend (`airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`).
- Object storage: use S3 directly (`news_object_storage` connection type `aws`,
  MWAA's execution role needs read/write on the landing bucket).
- Postgres: RDS Postgres with the `pgvector` extension (`CREATE EXTENSION vector;`
  — supported on RDS Postgres 15+ and Aurora Postgres). Run `include/db/schema.sql`
  once via a bootstrap task or `psql` from a bastion/Cloud Shell.
- Bias model + embedding model: if they need GPU inference at meaningful
  volume, MWAA workers are not GPU instances — consider running those two
  tasks via `EcsRunTaskOperator` / a SageMaker endpoint invoked from the task,
  rather than in-process in the Airflow worker.

## GCP: Cloud Composer

- `dags/`, `include/`, `plugins/` sync to the Composer environment's GCS bucket
  (`gs://<composer-bucket>/dags/...`, `.../data/include/...` — Composer mounts
  the bucket's `dags/` and `data/` folders into the worker filesystem).
- `requirements.txt` installs via `gcloud composer environments update
  --update-pypi-packages-from-file`.
- Object storage: GCS (`news_object_storage` connection type
  `google_cloud_platform`, workload identity / service account with
  Storage Object Admin on the landing bucket).
- Postgres: Cloud SQL for Postgres with `pgvector` (supported on Cloud SQL
  Postgres 15+). Connect via Cloud SQL Proxy sidecar or private IP.
- Heavier ML tasks: consider `GKEStartPodOperator` or Vertex AI custom jobs
  for the bias classifier / embedding steps if Composer's worker pool isn't
  sized for it.

## Azure: Data Factory managed Airflow

- Upload `dags/`, `include/`, `plugins/` to the Azure Blob Storage container
  backing the managed Airflow environment.
- `requirements.txt`: set via the environment's Airflow configuration
  (Data Factory UI → Airflow environment → Airflow requirements).
- Object storage: Azure Blob (`news_object_storage` connection type `wasb`).
- Postgres: Azure Database for PostgreSQL Flexible Server with `pgvector`
  (enable via server parameters: `azure.extensions = VECTOR`).
- Heavier ML tasks: `AzureContainerInstancesOperator` or an AKS job for
  GPU-bound bias classification / embedding if needed.

## Common to all three

- Secrets (API keys, DB passwords) belong in the platform's secrets backend
  (Secrets Manager / Secret Manager / Key Vault), wired to Airflow via the
  corresponding `secrets.backend` config — not in `.env` or Airflow Variables
  in plaintext for anything beyond local dev.
- `CONFIG.bias_model_uri` should point at wherever you version your fine-tuned
  checkpoints (S3/GCS/ADLS path, or an MLflow/model-registry URI) — retrain
  and promote a new version without touching DAG code.
- Set `max_active_runs=1` (already in the DAG) if you keep the
  time-windowed clustering approach — concurrent runs writing to overlapping
  time windows will race.
