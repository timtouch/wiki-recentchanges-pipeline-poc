# Wiki POC — Phase 1: Bronze Ingestion

## Repo layout

```
wiki-poc/
├── producer/
│   ├── producer.py          # SSE consumer + S3 writer
│   ├── requirements.txt
│   └── Dockerfile
├── terraform/
│   └── main.tf              # ECR, ECS, DynamoDB, IAM, CloudWatch
└── databricks/
    └── bronze_recentchange.py   # DLT Bronze table (Auto Loader)
```

---

## Step 1 — Deploy AWS infrastructure

```bash
cd terraform

# First-time init
terraform init

# Review the plan
terraform plan -var="alert_email=you@example.com"

# Apply
terraform apply -var="alert_email=you@example.com"
```

Note the `ecr_repository_url` output — you'll need it in Step 2.

---

## Step 2 — Build & push the producer image

```bash
cd producer

# Authenticate Docker to ECR
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin <ecr_repository_url>

# Build and push
docker build -t wiki-producer .
docker tag wiki-producer:latest <ecr_repository_url>:latest
docker push <ecr_repository_url>:latest
```

After the first push, ECS will pull the image and start the task automatically
(the service was created with `desired_count=1`).

---

## Step 3 — Verify the producer is running

```bash
# Tail CloudWatch logs
aws logs tail /ecs/wiki-producer --follow

# Check the ECS service
aws ecs describe-services \
  --cluster wiki-poc \
  --services wiki-producer \
  --query 'services[0].{status:status,running:runningCount,desired:desiredCount}'

# Verify files are landing in S3
aws s3 ls s3://wiki-raw-poc/recentchange/ --recursive | head -20

# Check the DynamoDB checkpoint
aws dynamodb get-item \
  --table-name wiki_producer_checkpoint \
  --key '{"stream_id": {"S": "recentchange"}}'
```

---

## Step 4 — Create the Bronze DLT pipeline in Databricks

1. In your Databricks workspace, go to **Delta Live Tables → Create pipeline**
2. Set:
   - **Pipeline name:** `wiki_poc_bronze`
   - **Product edition:** Core (sufficient for Bronze)
   - **Pipeline mode:** Continuous
   - **Source code:** point to `databricks/bronze_recentchange.py`
   - **Target catalog:** `wiki_poc`
   - **Target schema:** `bronze`
   - **Serverless:** enabled
3. Click **Start**

After a minute or two you should see `wiki_poc.bronze.recentchange_raw` populated
and row counts climbing.

---

## Validation checklist

- [ ] ECS service shows `runningCount=1`
- [ ] JSONL files appearing under `s3://wiki-raw-poc/recentchange/year=.../`
- [ ] DynamoDB checkpoint item updated every ~60 seconds
- [ ] CloudWatch log group `/ecs/wiki-producer` has structured log lines
- [ ] Bronze table row count growing in Databricks
- [ ] Kill the Fargate task manually → confirm it restarts and resumes from `Last-Event-ID`

---

## Environment variables (producer)

| Variable | Default | Description |
|---|---|---|
| `SSE_URL` | Wikimedia stream URL | Override for testing |
| `S3_BUCKET` | `wiki-raw-poc` | Raw landing bucket |
| `S3_PREFIX` | `recentchange` | Key prefix inside bucket |
| `DYNAMO_TABLE` | `wiki_producer_checkpoint` | Checkpoint table name |
| `CHECKPOINT_KEY` | `recentchange` | Row key in the checkpoint table |
| `AWS_REGION` | `us-east-1` | AWS region |
| `FLUSH_INTERVAL_SECS` | `60` | Max seconds between S3 flushes |
| `FLUSH_SIZE_BYTES` | `5242880` | Max buffer size before flush (5 MB) |
| `BACKOFF_CAP_SECS` | `30` | Max reconnect backoff (seconds) |
