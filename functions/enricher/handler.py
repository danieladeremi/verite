"""
verite/functions/enricher/handler.py

Week 1 local runner for Verite.

Run this script directly to process the sample reviews dataset
and validate the LLM enrichment quality before touching any AWS infrastructure.

Usage:
    python functions/enricher/handler.py
    python functions/enricher/handler.py --input sample_data/reviews_sample.json
    python functions/enricher/handler.py --input sample_data/reviews_sample.json --region us-west-2
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from collections import defaultdict

# Ensure the current directory is on the path so bedrock_client.py is found
sys.path.insert(0, str(Path(__file__).parent))

from bedrock_client import invoke_bedrock, BedrockInvocationError

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Colour helpers (terminal output only) ────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[31m"
YELLOW = "\033[33m"
GREEN  = "\033[32m"
CYAN   = "\033[36m"
DIM    = "\033[2m"

def _colour_sentiment(sentiment: str) -> str:
    colours = {"positive": GREEN, "neutral": YELLOW, "negative": RED}
    c = colours.get(sentiment, RESET)
    return f"{c}{sentiment.upper()}{RESET}"

def _colour_urgency(urgency: str) -> str:
    colours = {"high": RED, "medium": YELLOW, "low": GREEN}
    c = colours.get(urgency, RESET)
    return f"{c}{urgency.upper()}{RESET}"

def _score_bar(score: float, width: int = 20) -> str:
    filled = round(score * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {score:.2f}"


# ── Core processing ───────────────────────────────────────────────────────────

def enrich_review(review: dict, region: str) -> dict:
    """
    Calls Bedrock to enrich a single review, then merges the result
    back with the original review metadata.

    Returns a combined dict: original review fields + enrichment fields.
    """
    logger.info(f"Enriching review {review['review_id']} — \"{review['title']}\"")

    start = time.time()
    enrichment = invoke_bedrock(review, region=region)
    elapsed = time.time() - start

    logger.info(
        f"  → {_colour_sentiment(enrichment['sentiment'])} "
        f"score={enrichment['score']:.2f} "
        f"urgency={_colour_urgency(enrichment['urgency'])} "
        f"({elapsed:.1f}s)"
    )

    return {**review, **enrichment, "processing_time_s": round(elapsed, 2)}


def process_reviews(reviews: list[dict], region: str) -> list[dict]:
    """
    Processes a list of reviews, enriching each one.
    Failed reviews are logged and skipped — a single bad review
    should never halt the entire pipeline.

    Returns a list of successfully enriched review dicts.
    """
    results = []
    failed = []

    for review in reviews:
        try:
            enriched = enrich_review(review, region)
            results.append(enriched)
        except BedrockInvocationError as e:
            logger.error(f"  ✗ Failed to enrich {review['review_id']}: {e}")
            failed.append(review["review_id"])
        # Small delay to avoid hitting Bedrock rate limits during local testing
        time.sleep(0.5)

    if failed:
        logger.warning(f"\n{len(failed)} review(s) failed enrichment: {failed}")

    return results


# ── Report generation ─────────────────────────────────────────────────────────

def print_individual_results(results: list[dict]) -> None:
    print(f"\n{'═' * 70}")
    print(f"{BOLD}{CYAN}  VERITE — Individual Review Results{RESET}")
    print(f"{'═' * 70}\n")

    for r in results:
        print(f"{BOLD}[{r['review_id']}] {r['product_name']}{RESET}")
        print(f"  Rating   : {'★' * r['rating']}{'☆' * (5 - r['rating'])} ({r['rating']}/5)")
        print(f"  Sentiment: {_colour_sentiment(r['sentiment'])}  {_score_bar(r['score'])}")
        print(f"  Urgency  : {_colour_urgency(r['urgency'])}")

        if r["issues"]:
            print(f"  Issues   :")
            for issue in r["issues"]:
                print(f"    {RED}✗{RESET} {issue}")

        if r["praise"]:
            print(f"  Praise   :")
            for item in r["praise"]:
                print(f"    {GREEN}✓{RESET} {item}")

        print(f"  Action   : {CYAN}{r['suggested_action']}{RESET}")
        print(f"  {DIM}Processed in {r['processing_time_s']}s{RESET}")
        print()


def print_product_summary(results: list[dict]) -> None:
    """Groups results by product and prints aggregate signals per product."""
    by_product = defaultdict(list)
    for r in results:
        by_product[r["product_id"]].append(r)

    print(f"\n{'═' * 70}")
    print(f"{BOLD}{CYAN}  VERITE — Product Aggregate Summary{RESET}")
    print(f"{'═' * 70}\n")

    for product_id, reviews in by_product.items():
        product_name = reviews[0]["product_name"]
        avg_score = sum(r["score"] for r in reviews) / len(reviews)
        sentiments = [r["sentiment"] for r in reviews]
        urgencies  = [r["urgency"]  for r in reviews]

        sentiment_counts = {s: sentiments.count(s) for s in ["positive", "neutral", "negative"]}
        high_urgency     = [r for r in reviews if r["urgency"] == "high"]

        # Aggregate all issues across reviews for this product
        all_issues = []
        for r in reviews:
            all_issues.extend(r["issues"])
        issue_freq = defaultdict(int)
        for issue in all_issues:
            # Normalise to lowercase for deduplication
            issue_freq[issue.lower()] += 1
        top_issues = sorted(issue_freq.items(), key=lambda x: -x[1])[:3]

        print(f"{BOLD}{product_name}{RESET}  {DIM}({product_id}){RESET}")
        print(f"  Reviews analysed : {len(reviews)}")
        print(f"  Avg sentiment    : {_score_bar(avg_score)}")
        print(
            f"  Breakdown        : "
            f"{GREEN}{sentiment_counts['positive']} positive{RESET}  "
            f"{YELLOW}{sentiment_counts['neutral']} neutral{RESET}  "
            f"{RED}{sentiment_counts['negative']} negative{RESET}"
        )

        if high_urgency:
            print(f"  {RED}{BOLD}⚠ HIGH URGENCY ALERTS: {len(high_urgency)} review(s){RESET}")
            for r in high_urgency:
                print(f"    [{r['review_id']}] {r['title']}")

        if top_issues:
            print(f"  Top issues       :")
            for issue, count in top_issues:
                freq_label = f" (×{count})" if count > 1 else ""
                print(f"    {RED}✗{RESET} {issue}{DIM}{freq_label}{RESET}")

        print()


def print_pipeline_stats(results: list[dict], total_input: int) -> None:
    print(f"{'─' * 70}")
    success_rate = len(results) / total_input * 100 if total_input else 0
    avg_time = sum(r["processing_time_s"] for r in results) / len(results) if results else 0
    total_time = sum(r["processing_time_s"] for r in results)
    print(
        f"  {DIM}Pipeline stats: "
        f"{len(results)}/{total_input} enriched ({success_rate:.0f}% success)  |  "
        f"avg {avg_time:.1f}s/review  |  "
        f"total {total_time:.1f}s{RESET}"
    )
    print(f"{'─' * 70}\n")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Verite — local review enrichment runner (Week 1)"
    )
    parser.add_argument(
        "--input",
        default="sample_data/reviews_sample.json",
        help="Path to the reviews JSON file (default: sample_data/reviews_sample.json)"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for Bedrock (default: us-east-1)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write enriched results as JSON"
    )
    args = parser.parse_args()

    # ── Load reviews ──────────────────────────────────────────────────────────
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    with open(input_path) as f:
        reviews = json.load(f)

    logger.info(f"Loaded {len(reviews)} reviews from {input_path}")
    logger.info(f"Bedrock region: {args.region}")
    logger.info(f"Starting enrichment...\n")

    # ── Enrich ────────────────────────────────────────────────────────────────
    results = process_reviews(reviews, region=args.region)

    if not results:
        logger.error("No reviews were successfully enriched. Exiting.")
        sys.exit(1)

    # ── Print reports ─────────────────────────────────────────────────────────
    print_individual_results(results)
    print_product_summary(results)
    print_pipeline_stats(results, total_input=len(reviews))

    # ── Optional JSON output ──────────────────────────────────────────────────
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Enriched results written to {output_path}")


if __name__ == "__main__":
    main()
