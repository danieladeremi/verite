"""
verite/functions/ingestor/handler.py

Ingestor Lambda — triggered by S3 ObjectCreated events.

Responsibilities:
  1. Read the uploaded review JSON file from S3
  2. Validate it contains a list of review objects
  3. Fan each review out as an individual SQS message

By sending one SQS message per review, we get:
  - Independent retries (one bad review doesn't block others)
  - Parallel processing (SQS + Lambda scale together automatically)
  - A clear audit trail (each message maps to one review)
"""

import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client  = boto3.client("s3")
sqs_client = boto3.client("sqs")

QUEUE_URL = os.environ["REVIEWS_QUEUE_URL"]

# Required fields every review must have before we accept it into the pipeline
REQUIRED_REVIEW_FIELDS = {"review_id", "product_id", "product_name", "rating", "title", "body"}


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point for the ingestor Lambda.

    AWS invokes this with an S3 event payload each time a .json file
    is uploaded to the Verite reviews bucket.

    Returns a summary dict for CloudWatch logging.
    """
    results = {"processed_files": 0, "total_reviews_queued": 0, "errors": []}

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]

        logger.info(f"Processing upload: s3://{bucket}/{key}")

        try:
            reviews = _read_reviews_from_s3(bucket, key)
            queued  = _fan_out_to_sqs(reviews, source_key=key)
            results["processed_files"] += 1
            results["total_reviews_queued"] += queued
            logger.info(f"Queued {queued} reviews from {key}")

        except IngestorError as e:
            # Log and continue — one bad file shouldn't block others in the batch
            logger.error(f"Failed to process {key}: {e}")
            results["errors"].append({"file": key, "error": str(e)})

    logger.info(f"Ingestor complete: {results}")
    return results


def _read_reviews_from_s3(bucket: str, key: str) -> list[dict]:
    """
    Downloads and parses the review JSON file from S3.

    Returns a list of validated review dicts.
    Raises IngestorError if the file can't be read or doesn't contain valid reviews.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw = response["Body"].read().decode("utf-8")
    except ClientError as e:
        raise IngestorError(
            f"Could not read s3://{bucket}/{key}: {e.response['Error']['Code']}"
        ) from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise IngestorError(f"File is not valid JSON: {e}") from e

    if not isinstance(data, list):
        raise IngestorError(
            f"Expected a JSON array of reviews, got {type(data).__name__}"
        )

    if len(data) == 0:
        raise IngestorError("File contains an empty review list — nothing to process")

    # Validate each review has required fields before queuing
    valid_reviews = []
    for i, review in enumerate(data):
        missing = REQUIRED_REVIEW_FIELDS - review.keys()
        if missing:
            logger.warning(
                f"Review at index {i} missing fields {missing} — skipping"
            )
            continue
        valid_reviews.append(review)

    if not valid_reviews:
        raise IngestorError("No valid reviews found in file after field validation")

    logger.info(
        f"Validated {len(valid_reviews)}/{len(data)} reviews from {key}"
    )
    return valid_reviews


def _fan_out_to_sqs(reviews: list[dict], source_key: str) -> int:
    """
    Sends each review as an individual SQS message.

    Uses send_message_batch (up to 10 per call) to reduce API calls.
    Logs any partial failures within a batch.

    Returns the number of successfully queued messages.
    """
    queued = 0
    # SQS batch API accepts at most 10 messages per call
    batch_size = 10

    for batch_start in range(0, len(reviews), batch_size):
        batch = reviews[batch_start : batch_start + batch_size]

        entries = [
            {
                "Id": str(i),  # Unique within this batch call
                "MessageBody": json.dumps({
                    "review": review,
                    "source_key": source_key
                })
            }
            for i, review in enumerate(batch)
        ]

        try:
            response = sqs_client.send_message_batch(
                QueueUrl=QUEUE_URL,
                Entries=entries
            )
        except ClientError as e:
            raise IngestorError(
                f"SQS batch send failed: {e.response['Error']['Code']}"
            ) from e

        # SQS batch can partially succeed — log any individual failures
        if response.get("Failed"):
            for failure in response["Failed"]:
                review_id = batch[int(failure["Id"])].get("review_id", "unknown")
                logger.error(
                    f"Failed to queue review {review_id}: "
                    f"[{failure['Code']}] {failure['Message']}"
                )

        successful = len(response.get("Successful", []))
        queued += successful
        logger.info(
            f"Batch queued: {successful}/{len(batch)} messages "
            f"(reviews {batch_start}–{batch_start + len(batch) - 1})"
        )

    return queued


class IngestorError(Exception):
    """Raised when the ingestor cannot process an uploaded file."""
    pass
