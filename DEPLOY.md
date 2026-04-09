# Verite — Week 2 Deployment Guide

Deploy the full serverless pipeline to AWS using SAM.

---

## Prerequisites

### 1. Install AWS SAM CLI

SAM (Serverless Application Model) is the tool that packages and deploys
your Lambda functions and infrastructure in one command.

Download the Windows installer:
https://github.com/aws/aws-sam-cli/releases/latest/download/AWS_SAM_CLI_64_PY3.msi

Run it, accept all defaults. Then verify:
```powershell
sam --version
```

### 2. Install Docker Desktop

SAM uses Docker to build Lambda packages locally.

Download: https://www.docker.com/products/docker-desktop/

Install and start Docker Desktop. Verify:
```powershell
docker --version
```

---

## Deploy

Run all commands from the **project root** (`verite/` folder).

### Step 1 — Build

Packages your Lambda functions and resolves dependencies:
```powershell
sam build --template infrastructure/template.yaml
```

### Step 2 — Deploy (first time)

Walks you through deployment interactively:
```powershell
sam deploy --guided --region us-east-2
```

Answer the prompts:
```
Stack Name:                     verite-dev
AWS Region:                     us-east-2
Parameter Environment [dev]:    dev
Confirm changes before deploy:  y
Allow SAM CLI IAM role creation: y
Disable rollback:               n
Save arguments to samconfig.toml: y
```

SAM will show you a changeset (all the resources it will create) and ask
for final confirmation. Type `y` to deploy.

### Step 3 — Deploy (subsequent times)

After the first deploy, settings are saved to `samconfig.toml`:
```powershell
sam build --template infrastructure/template.yaml && sam deploy --region us-east-2
```

---

## Test the live pipeline

After deployment, SAM prints the stack outputs including your bucket name.

### Upload a review file to trigger the pipeline:
```powershell
aws s3 cp sample_data/reviews_sample.json s3://verite-reviews-dev-<YOUR_ACCOUNT_ID>/ --region us-east-2
```

Replace `<YOUR_ACCOUNT_ID>` with your AWS account ID (visible in the top-right
of the AWS Console).

### Watch the enricher logs in real time:
```powershell
sam logs --name VeriteEnricherFunction --stack-name verite-dev --region us-east-2 --tail
```

You should see enrichment log lines appearing within 5–10 seconds of the upload.

### Check the DLQ for any failed reviews:
```powershell
aws sqs get-queue-attributes `
  --queue-url $(aws sqs get-queue-url --queue-name verite-reviews-dlq-dev --region us-east-2 --query QueueUrl --output text) `
  --attribute-names ApproximateNumberOfMessages `
  --region us-east-2
```

A count of `0` means every review enriched successfully.

---

## Tear down (avoid charges)

When you are done testing, delete all resources:
```powershell
sam delete --stack-name verite-dev --region us-east-2
```

This removes every resource SAM created. S3 buckets with objects in them
must be emptied first:
```powershell
aws s3 rm s3://verite-reviews-dev-<YOUR_ACCOUNT_ID>/ --recursive --region us-east-2
sam delete --stack-name verite-dev --region us-east-2
```
