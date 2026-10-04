# NewLens Deployment Guide

## Infrastructure Overview

| Component | Where | How |
|-----------|-------|-----|
| Pipeline (Airflow + MinIO + containers) | AWS EC2 t3.medium | Docker Compose |
| News DB (Postgres + pgvector) | Same EC2 instance | Docker Compose (pipeline stack) |
| Web App (FastAPI + Next.js) | Same EC2 instance | Docker Compose (web-app stack) |

**Instance:** `i-0b7ae7b02f6621554` in `ap-south-1`
**Public IP:** `13.201.133.212`
**OS:** Ubuntu 22.04 LTS
**Storage:** 30GB gp3
**SSH Key:** `~/.ssh/newslens-key.pem`

## Live URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Web App (Frontend) | http://13.201.133.212:3000 | — |
| Web App (Backend API) | http://13.201.133.212:8000 | — |
| Airflow UI | http://13.201.133.212:8080 | airflow / airflow |
| MinIO Console | http://13.201.133.212:9001 | minioadmin / minioadmin |
| News DB (Postgres) | 13.201.133.212:5433 | news / news / news_pipeline |

## Security Group

**Name:** `newslens-sg` (`sg-054890d40dea0a846`)

Open ports: 22 (SSH), 3000 (frontend), 8000 (backend API), 8080 (Airflow), 9001 (MinIO)

---

## SSH Access

```bash
ssh -i ~/.ssh/newslens-key.pem ubuntu@13.201.133.212
```

---

## Project Layout on EC2

```
/home/ubuntu/
├── Data-Pipeline/          # Pipeline repo (Airflow + containers)
│   ├── docker-compose.yaml
│   ├── dags/
│   ├── containers/         # 7 pipeline stage containers
│   ├── build_images.sh
│   └── .env
└── Web-app/                # Web application repo
    ├── docker-compose.yml
    ├── backend/
    │   └── .env
    └── frontend/
```

---

## How It Was Deployed

### 1. EC2 Instance
- Launched via AWS CLI with Ubuntu 22.04 AMI
- Security group created with required ports opened
- SSH key pair generated (`newslens-key`)

### 2. Docker Installed
```bash
sudo apt install docker.io docker-compose-v2 git
sudo usermod -aG docker ubuntu
```

### 3. Pipeline Stack
```bash
git clone https://github.com/Newslens-lk/Data-Pipeline.git
cd Data-Pipeline
# Created .env with credentials
./build_images.sh          # Builds all 7 pipeline container images
docker compose up -d       # Starts Airflow, MinIO, Postgres, news-db
```

**Key adjustments made on EC2 (differs from local):**
- Docker network name: `data-pipeline_default` (not `newslens_pipeline_default`)
- Docker GID: `121` (not `984`) in `docker-compose.yaml` → `group_add`
- MinIO image: transferred from local machine (Docker Hub no longer hosts it)
- MinIO bucket: manually created (`mc mb local/newslens-pipeline`)
- XGBoost model: transferred from local MinIO to EC2 MinIO
- Airflow variable `pipeline_env`: set via CLI with all credentials

### 4. Web App Stack
```bash
git clone https://github.com/Newslens-lk/Web-app.git
cd Web-app
# Created backend/.env
# Updated docker-compose.yml:
#   - network: data-pipeline_default (external)
#   - NEXT_PUBLIC_API_BASE: http://13.201.133.212:8000/api (build arg)
#   - CORS_ORIGINS: includes EC2 IP
docker compose up -d --build
```

### 5. Database Seeded
Local news-db was dumped and restored on EC2:
```bash
docker exec <local-news-db> pg_dump -U news -d news_pipeline > dump.sql
scp dump.sql ubuntu@<ec2-ip>:/tmp/
# On EC2: psql -f dump.sql
```

---

## Running Operations

### Trigger the Pipeline
**Via Airflow UI:**
1. Go to http://13.201.133.212:8080
2. Find `news_event_pipeline` DAG
3. Click the play button → Trigger DAG

**Via CLI (from EC2):**
```bash
cd Data-Pipeline
sudo docker compose exec airflow-apiserver airflow dags trigger news_event_pipeline
```

**Via Web App Admin API:**
```bash
curl -X POST http://13.201.133.212:8000/api/admin/pipeline/trigger \
  -H "X-API-Key: changeme"
```

### Topic Assignment

**Single event:**
```bash
curl -X POST http://13.201.133.212:8000/api/events/{event_id}/assign-topic
```

**Bulk (all events without topics):**
```bash
# Start bulk job
curl -X POST http://13.201.133.212:8000/api/admin/assign-topics \
  -H "X-API-Key: changeme"

# Check job progress
curl http://13.201.133.212:8000/api/admin/assign-topics/{job_id} \
  -H "X-API-Key: changeme"
```

### Summarisation

**Single event (generates on-demand, caches in DB):**
```bash
curl -X POST http://13.201.133.212:8000/api/events/{event_id}/summarize
```

**Force re-summarise:**
```bash
curl -X POST http://13.201.133.212:8000/api/events/{event_id}/resummarize
```

### Check Pipeline Status
```bash
curl http://13.201.133.212:8000/api/admin/pipeline/status \
  -H "X-API-Key: changeme"
```

---

## Debugging & Troubleshooting

### View Container Status
```bash
ssh -i ~/.ssh/newslens-key.pem ubuntu@13.201.133.212
sudo docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

### View Logs

**Pipeline services:**
```bash
cd Data-Pipeline
sudo docker compose logs airflow-scheduler --tail 50
sudo docker compose logs airflow-apiserver --tail 50
sudo docker compose logs news-db --tail 50
sudo docker compose logs minio --tail 50
```

**Web app:**
```bash
cd Web-app
sudo docker compose logs backend --tail 50
sudo docker compose logs frontend --tail 50
```

**Pipeline task logs:**
Check in Airflow UI → click on failed task → Log tab

### Common Issues

#### "Permission denied" on docker.sock
The scheduler can't launch pipeline containers.
```bash
# Check Docker GID on host
getent group docker | cut -d: -f3
# Update group_add in docker-compose.yaml to match
# Then recreate (not restart) the scheduler:
cd Data-Pipeline
sudo docker compose down airflow-scheduler
sudo docker compose up -d airflow-scheduler
```

#### DAG not showing in Airflow UI
```bash
cd Data-Pipeline
sudo docker compose exec airflow-apiserver airflow dags reserialize
```

#### "NoSuchBucket" error
MinIO bucket doesn't exist:
```bash
sudo docker exec data-pipeline-minio-1 mc alias set local http://localhost:9000 minioadmin minioadmin
sudo docker exec data-pipeline-minio-1 mc mb local/newslens-pipeline
```

#### XGBoost model not found (HeadObject 404)
Upload the model to MinIO:
```bash
# From local machine
scp xgb_model.joblib ubuntu@13.201.133.212:/tmp/
# On EC2
sudo docker cp /tmp/xgb_model.joblib data-pipeline-minio-1:/tmp/
sudo docker exec data-pipeline-minio-1 mc cp /tmp/xgb_model.joblib local/newslens-pipeline/models/bias-classifier-xgb/xgb_model.joblib
```

#### Modal auth error (embedder/bias-transformers)
Modal token missing from `pipeline_env`. Update the Airflow variable:
```bash
cd Data-Pipeline
sudo docker compose exec airflow-apiserver airflow variables get pipeline_env
# Add MODAL_TOKEN_ID and MODAL_TOKEN_SECRET to the JSON, then:
sudo docker compose exec airflow-apiserver airflow variables set pipeline_env '<updated-json>'
```

#### Web app "Application error"
Usually the backend crashed or can't reach the DB:
```bash
cd Web-app
sudo docker compose logs backend --tail 30
# If tables don't exist, run the pipeline first or restore a DB dump
# Then restart:
sudo docker compose restart backend
```

#### Frontend can't fetch data (network error in browser)
`NEXT_PUBLIC_API_BASE` is baked at build time. If the IP changes:
```bash
cd Web-app
# Update the build arg in docker-compose.yml
sudo docker compose build --no-cache frontend
sudo docker compose up -d frontend
```

### Check MinIO Contents
```bash
sudo docker exec data-pipeline-minio-1 mc ls local/newslens-pipeline/ --recursive
```

### Query the Database
```bash
sudo docker exec data-pipeline-news-db-1 psql -U news -d news_pipeline -c "SELECT source_name, count(*) FROM articles GROUP BY source_name ORDER BY count DESC;"
```

---

## Restart Everything

```bash
# Pipeline stack
cd ~/Data-Pipeline && sudo docker compose down && sudo docker compose up -d

# Web app stack
cd ~/Web-app && sudo docker compose down && sudo docker compose up -d
```

## Rebuild Pipeline Images
If container code changes:
```bash
cd ~/Data-Pipeline
git pull
./build_images.sh
```

## Rebuild Web App
If frontend/backend code changes:
```bash
cd ~/Web-app
git pull
sudo docker compose up -d --build
```

---

## Cost

- **Instance type:** t3.medium (~$0.046/hr ≈ $1.10/day ≈ $8/week)
- **Storage:** 30GB gp3 (~$2.40/month)
- **Data transfer:** minimal for demo usage

### Stop to Save Money
```bash
aws ec2 stop-instances --instance-ids i-0b7ae7b02f6621554
```
Note: Public IP changes on restart unless you attach an Elastic IP.

### Start Again
```bash
aws ec2 start-instances --instance-ids i-0b7ae7b02f6621554
# Get new public IP
aws ec2 describe-instances --instance-ids i-0b7ae7b02f6621554 --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
# If IP changed, rebuild frontend with new NEXT_PUBLIC_API_BASE
```

### Terminate (delete everything)
```bash
aws ec2 terminate-instances --instance-ids i-0b7ae7b02f6621554
aws ec2 delete-security-group --group-id sg-054890d40dea0a846
aws ec2 delete-key-pair --key-name newslens-key
```
