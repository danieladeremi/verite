# Verite

> *French for "truth" — extracting signal from the noise of customer reviews.*

Verite is a serverless AWS pipeline that ingests e-commerce product reviews,
uses a large language model via Amazon Bedrock to extract structured intelligence
(sentiment, issues, urgency, suggested actions), and surfaces real-time alerts
to sellers. Built end-to-end in Python on AWS-native services.

---

## Architecture

```
S3 (upload) → SQS (queue) → Lambda (validate) → Lambda (enrich via Bedrock)
                                                        ↓
                                               DynamoDB (store)
                                                        ↓
                                         API Gateway → Query Lambda
                                                        ↓
                                               Seller dashboard
```

CloudWatch monitors every layer.

---

## Project structure

```
verite/
├── README.md
├── requirements.txt
├── sample_data/
│   └── reviews_sample.json       # 10 sample reviews across 3 products
├── functions/
│   ├── enricher/
│   │   ├── handler.py            # Local runner + pipeline orchestration
│   │   └── bedrock_client.py     # Bedrock API client + response parser
│   ├── ingestor/                 # Week 2: S3 → SQS fan-out Lambda
│   └── query_api/                # Week 3: API Gateway → DynamoDB Lambda
├── infrastructure/
│   └── template.yaml             # Week 2: AWS SAM deployment template
└── tests/
    └── test_enricher.py          # Unit tests (no AWS credentials needed)
```

---

## Week 1 — Local runner

Week 1 validates the LLM enrichment quality on sample data before
any AWS infrastructure is touched.

### Prerequisites

1. **Python 3.11+**
2. **AWS credentials configured** with Bedrock access:
   ```bash
   aws configure
   ```
   Or set environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_DEFAULT_REGION=us-east-1
   ```

3. **Enable Bedrock model access** in the AWS Console:
   - Go to Amazon Bedrock → Model access
   - Request access to `Claude 3.5 Sonnet`
   - Wait for approval (usually instant for Claude models)

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Run the enrichment pipeline

```bash
cd functions/enricher
python handler.py
```

With custom options:

```bash
# Use a different input file
python handler.py --input ../../sample_data/reviews_sample.json

# Use a different AWS region
python handler.py --region us-west-2

# Save enriched results to a file
python handler.py --output enriched_results.json
```

### Run the tests

The unit tests mock all Bedrock calls — no AWS credentials needed.

```bash
pip install pytest
pytest tests/test_enricher.py -v
```

---

## What Verite extracts from each review

| Field              | Type             | Description                                      |
|--------------------|------------------|--------------------------------------------------|
| `sentiment`        | positive/neutral/negative | Overall review sentiment               |
| `score`            | float 0.0–1.0    | Sentiment intensity (1.0 = most positive)        |
| `issues`           | list of strings  | Specific product problems mentioned              |
| `praise`           | list of strings  | Specific positives mentioned                     |
| `suggested_action` | string           | One actionable recommendation for the seller     |
| `urgency`          | low/medium/high  | How urgently the seller should respond           |

---

## Roadmap

| Week | Milestone |
|------|-----------|
| ✅ 1 | Local enrichment runner + Bedrock integration + unit tests |
| 2    | AWS infrastructure: S3 + SQS + Lambda + SAM deployment |
| 3    | DynamoDB storage + API Gateway query layer |
| 4    | CloudWatch alarms + seller dashboard + demo |
