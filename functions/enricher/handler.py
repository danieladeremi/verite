"""
verite/functions/enricher/handler.py

Week 3 enricher with dual modes:
1. Lambda mode: SQS -> Bedrock -> DynamoDB
2. Local mode: command-line runner used in Week 1 quality validation
"""

import argparse
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Ensure local imports work both in Lambda and when run as a script.
sys.path.insert(0, str(Path(__file__).parent))

from bedrock_client import BedrockInvocationError, invoke_bedrock


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


DYNAMODB_RESOURCE = boto3.resource("dynamodb")
REQUIRED_REVIEW_FIELDS = {"review_id", "product_id", "product_name", "rating", "title", "body"}


def lambda_handler(event: dict, context) -> dict:
    """
    Lambda entrypoint.

    Expected SQS message body:
    {
      "review": { ...review fields... },
      "source_key": "s3/object/key.json"
    }

    Returns `batchItemFailures` for partial SQS retries.
    """
    table = _get_table()
    failures = []
    processed = 0

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown-message")
        try:
            payload = json.loads(record["body"])
            review = payload["review"]
            source_key = payload.get("source_key", "unknown")

            _validate_review_shape(review)
            region = os.getenv("BEDROCK_REGION", "us-east-2")
            enriched = enrich_review(review, region=region)
            item = _build_dynamodb_item(review=review, enrichment=enriched, source_key=source_key)

            table.put_item(Item=item)
            processed += 1
            logger.info(
                "Stored enriched review review_id=%s product_id=%s sort_key=%s",
                item["review_id"],
                item["product_id"],
                item["review_sort_key"],
            )
        except Exception as exc:  # noqa: BLE001 - we need per-record fault isolation
            logger.exception("Failed to process messageId=%s: %s", message_id, exc)
            failures.append({"itemIdentifier": message_id})

    summary = {
        "processed": processed,
        "failed": len(failures),
        "batchItemFailures": failures,
    }
    logger.info("Enricher batch summary: %s", summary)
    return summary


def _get_table():
    table_name = os.getenv("ENRICHED_REVIEWS_TABLE")
    if not table_name:
        raise RuntimeError("Missing required env var: ENRICHED_REVIEWS_TABLE")
    return DYNAMODB_RESOURCE.Table(table_name)


def _validate_review_shape(review: dict) -> None:
    if not isinstance(review, dict):
        raise ValueError("review payload must be an object")
    missing = REQUIRED_REVIEW_FIELDS - set(review.keys())
    if missing:
        raise ValueError(f"review payload missing required fields: {sorted(missing)}")


def _normalize_review_date(review: dict) -> str:
    review_date = review.get("date")
    if isinstance(review_date, str):
        try:
            datetime.strptime(review_date, "%Y-%m-%d")
            return review_date
        except ValueError:
            logger.warning("Invalid review date '%s'; falling back to current UTC date", review_date)
    return datetime.now(timezone.utc).date().isoformat()


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _build_dynamodb_item(review: dict, enrichment: dict, source_key: str) -> dict:
    review_date = _normalize_review_date(review)
    processed_at = datetime.now(timezone.utc).isoformat()
    review_sort_key = f"{review_date}#{review['review_id']}"

    return {
        "product_id": review["product_id"],
        "review_sort_key": review_sort_key,
        "review_id": review["review_id"],
        "product_name": review["product_name"],
        "rating": int(review["rating"]),
        "title": review["title"],
        "body": review["body"],
        "review_date": review_date,
        "source_key": source_key,
        "sentiment": enrichment["sentiment"],
        "score": _to_decimal(enrichment["score"]),
        "issues": enrichment.get("issues", []),
        "praise": enrichment.get("praise", []),
        "suggested_action": enrichment.get("suggested_action", ""),
        "urgency": enrichment.get("urgency", "low"),
        "processing_time_s": _to_decimal(enrichment.get("processing_time_s", 0)),
        "processed_at": processed_at,
    }


# Local runner functionality (Week 1) retained below.

RESET = "\033[0m"
BOLD = "\033[1m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
CYAN = "\033[36m"
DIM = "\033[2m"


def _colour_sentiment(sentiment: str) -> str:
    colours = {"positive": GREEN, "neutral": YELLOW, "negative": RED}
    return f"{colours.get(sentiment, RESET)}{sentiment.upper()}{RESET}"


def _colour_urgency(urgency: str) -> str:
    colours = {"high": RED, "medium": YELLOW, "low": GREEN}
    return f"{colours.get(urgency, RESET)}{urgency.upper()}{RESET}"


def _score_bar(score: float, width: int = 20) -> str:
    score = max(0.0, min(1.0, float(score)))
    filled = round(score * width)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {score:.2f}"


def enrich_review(review: dict, region: str) -> dict:
    """
    Calls Bedrock to enrich a single review and merges the enrichment fields
    back into the original review payload.
    """
    logger.info('Enriching review %s - "%s"', review["review_id"], review["title"])
    start = time.time()
    enrichment = invoke_bedrock(review, region=region)
    elapsed = time.time() - start

    logger.info(
        "  -> %s score=%.2f urgency=%s (%.1fs)",
        _colour_sentiment(enrichment["sentiment"]),
        enrichment["score"],
        _colour_urgency(enrichment["urgency"]),
        elapsed,
    )

    return {**review, **enrichment, "processing_time_s": round(elapsed, 2)}


def process_reviews(reviews: list[dict], region: str) -> list[dict]:
    """
    Local batch processing helper for Week 1 dry-runs.
    Failures are logged and skipped so one bad review does not stop the run.
    """
    results = []
    failed = []

    for review in reviews:
        try:
            enriched = enrich_review(review, region)
            results.append(enriched)
        except BedrockInvocationError as exc:
            logger.error("  x Failed to enrich %s: %s", review["review_id"], exc)
            failed.append(review["review_id"])
        time.sleep(0.5)

    if failed:
        logger.warning("%s review(s) failed enrichment: %s", len(failed), failed)

    return results


def print_individual_results(results: list[dict]) -> None:
    print("\n" + "=" * 70)
    print(f"{BOLD}{CYAN}  VERITE - Individual Review Results{RESET}")
    print("=" * 70 + "\n")

    for review in results:
        print(f"{BOLD}[{review['review_id']}] {review['product_name']}{RESET}")
        print(f"  Rating   : {'*' * review['rating']}{'.' * (5 - review['rating'])} ({review['rating']}/5)")
        print(f"  Sentiment: {_colour_sentiment(review['sentiment'])}  {_score_bar(review['score'])}")
        print(f"  Urgency  : {_colour_urgency(review['urgency'])}")

        if review["issues"]:
            print("  Issues   :")
            for issue in review["issues"]:
                print(f"    {RED}x{RESET} {issue}")

        if review["praise"]:
            print("  Praise   :")
            for praise in review["praise"]:
                print(f"    {GREEN}+{RESET} {praise}")

        print(f"  Action   : {CYAN}{review['suggested_action']}{RESET}")
        print(f"  {DIM}Processed in {review['processing_time_s']}s{RESET}")
        print()


def print_product_summary(results: list[dict]) -> None:
    by_product = defaultdict(list)
    for review in results:
        by_product[review["product_id"]].append(review)

    print("\n" + "=" * 70)
    print(f"{BOLD}{CYAN}  VERITE - Product Aggregate Summary{RESET}")
    print("=" * 70 + "\n")

    for product_id, reviews in by_product.items():
        product_name = reviews[0]["product_name"]
        avg_score = sum(float(r["score"]) for r in reviews) / len(reviews)

        sentiments = [r["sentiment"] for r in reviews]
        sentiment_counts = {name: sentiments.count(name) for name in ["positive", "neutral", "negative"]}
        high_urgency = [r for r in reviews if r["urgency"] == "high"]

        issue_counter = Counter()
        for review in reviews:
            for issue in review["issues"]:
                issue_counter[issue.lower()] += 1

        print(f"{BOLD}{product_name}{RESET}  {DIM}({product_id}){RESET}")
        print(f"  Reviews analysed : {len(reviews)}")
        print(f"  Avg sentiment    : {_score_bar(avg_score)}")
        print(
            "  Breakdown        : "
            f"{GREEN}{sentiment_counts['positive']} positive{RESET}  "
            f"{YELLOW}{sentiment_counts['neutral']} neutral{RESET}  "
            f"{RED}{sentiment_counts['negative']} negative{RESET}"
        )

        if high_urgency:
            print(f"  {RED}{BOLD}! HIGH URGENCY ALERTS: {len(high_urgency)} review(s){RESET}")
            for review in high_urgency:
                print(f"    [{review['review_id']}] {review['title']}")

        top_issues = issue_counter.most_common(3)
        if top_issues:
            print("  Top issues       :")
            for issue, count in top_issues:
                suffix = f" ({count}x)" if count > 1 else ""
                print(f"    {RED}x{RESET} {issue}{DIM}{suffix}{RESET}")
        print()


def print_pipeline_stats(results: list[dict], total_input: int) -> None:
    print("-" * 70)
    success_rate = (len(results) / total_input * 100) if total_input else 0.0
    avg_time = (sum(float(r["processing_time_s"]) for r in results) / len(results)) if results else 0.0
    total_time = sum(float(r["processing_time_s"]) for r in results)
    print(
        f"  {DIM}Pipeline stats: "
        f"{len(results)}/{total_input} enriched ({success_rate:.0f}% success) | "
        f"avg {avg_time:.1f}s/review | total {total_time:.1f}s{RESET}"
    )
    print("-" * 70 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verite - local review enrichment runner")
    parser.add_argument(
        "--input",
        default="sample_data/reviews_sample.json",
        help="Path to reviews JSON file (default: sample_data/reviews_sample.json)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for Bedrock (default: us-east-1)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path for enriched JSON",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        raise SystemExit(1)

    with input_path.open(encoding="utf-8") as handle:
        reviews = json.load(handle)

    logger.info("Loaded %s reviews from %s", len(reviews), input_path)
    logger.info("Bedrock region: %s", args.region)
    logger.info("Starting enrichment")

    results = process_reviews(reviews, region=args.region)
    if not results:
        logger.error("No reviews were successfully enriched. Exiting.")
        raise SystemExit(1)

    print_individual_results(results)
    print_product_summary(results)
    print_pipeline_stats(results, total_input=len(reviews))

    if args.output:
        output_path = Path(args.output)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)
        logger.info("Enriched results written to %s", output_path)


if __name__ == "__main__":
    main()
