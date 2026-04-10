"""
verite/functions/query_api/handler.py

Week 3 query API:
- Reads enriched review records from DynamoDB
- Aggregates top issues by frequency
- Builds daily sentiment trend over a configurable date window
"""

import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key


DYNAMODB_RESOURCE = boto3.resource("dynamodb")
VALID_SENTIMENTS = {"positive", "neutral", "negative"}
MAX_WINDOW_DAYS = 90


def lambda_handler(event: dict, context) -> dict:
    try:
        table = _get_table()
        product_id = _get_required_product_id(event)
        window_days = _parse_window_days(event)
        end_date = _parse_end_date(event)
        start_date = end_date - timedelta(days=window_days - 1)

        items = _query_reviews(
            table=table,
            product_id=product_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

        body = _build_summary(
            product_id=product_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            items=items,
        )
        return _response(200, body)
    except ValueError as exc:
        return _response(400, {"error": str(exc)})
    except Exception as exc:  # noqa: BLE001 - API should not expose internals
        return _response(500, {"error": "Internal server error", "details": str(exc)})


def _get_table():
    table_name = os.getenv("ENRICHED_REVIEWS_TABLE")
    if not table_name:
        raise ValueError("Missing required env var: ENRICHED_REVIEWS_TABLE")
    return DYNAMODB_RESOURCE.Table(table_name)


def _get_required_product_id(event: dict) -> str:
    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")
    if not product_id:
        raise ValueError("Missing required path parameter: product_id")
    return product_id


def _parse_window_days(event: dict) -> int:
    query = event.get("queryStringParameters") or {}
    raw_days = query.get("days", "7")
    try:
        days = int(raw_days)
    except (TypeError, ValueError) as exc:
        raise ValueError("Query parameter 'days' must be an integer") from exc
    if days < 1 or days > MAX_WINDOW_DAYS:
        raise ValueError(f"Query parameter 'days' must be between 1 and {MAX_WINDOW_DAYS}")
    return days


def _parse_end_date(event: dict):
    query = event.get("queryStringParameters") or {}
    raw_end_date = query.get("end_date")
    if raw_end_date:
        try:
            return datetime.strptime(raw_end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Query parameter 'end_date' must be in YYYY-MM-DD format") from exc
    return datetime.now(timezone.utc).date()


def _query_reviews(table, product_id: str, start_date: str, end_date: str) -> list[dict]:
    # Lexicographic range works because review_sort_key starts with YYYY-MM-DD.
    start_key = f"{start_date}#"
    end_key = f"{end_date}#\uffff"

    items = []
    query_kwargs = {
        "KeyConditionExpression": Key("product_id").eq(product_id)
        & Key("review_sort_key").between(start_key, end_key)
    }

    while True:
        response = table.query(**query_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        query_kwargs["ExclusiveStartKey"] = last_key

    return items


def _build_summary(product_id: str, start_date: str, end_date: str, items: list[dict]) -> dict:
    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    issue_counter = Counter()
    trend_by_day = {}
    high_urgency_count = 0
    total_score = 0.0

    for item in items:
        sentiment = item.get("sentiment", "neutral")
        if sentiment not in VALID_SENTIMENTS:
            sentiment = "neutral"

        score = _to_float(item.get("score", 0.0))
        review_date = item.get("review_date") or str(item.get("review_sort_key", "")).split("#")[0]
        urgency = item.get("urgency", "low")

        sentiment_counts[sentiment] += 1
        total_score += score

        if urgency == "high":
            high_urgency_count += 1

        for issue in item.get("issues", []):
            cleaned = str(issue).strip()
            if cleaned:
                issue_counter[cleaned.lower()] += 1

        if review_date not in trend_by_day:
            trend_by_day[review_date] = {
                "date": review_date,
                "total_reviews": 0,
                "positive": 0,
                "neutral": 0,
                "negative": 0,
                "score_sum": 0.0,
            }

        day_bucket = trend_by_day[review_date]
        day_bucket["total_reviews"] += 1
        day_bucket[sentiment] += 1
        day_bucket["score_sum"] += score

    total_reviews = len(items)
    average_score = round(total_score / total_reviews, 4) if total_reviews else 0.0

    sentiment_trend = []
    for day in sorted(trend_by_day.keys()):
        bucket = trend_by_day[day]
        day_avg = round(bucket["score_sum"] / bucket["total_reviews"], 4) if bucket["total_reviews"] else 0.0
        sentiment_trend.append(
            {
                "date": bucket["date"],
                "total_reviews": bucket["total_reviews"],
                "positive": bucket["positive"],
                "neutral": bucket["neutral"],
                "negative": bucket["negative"],
                "average_score": day_avg,
            }
        )

    top_issues = [{"issue": issue, "count": count} for issue, count in issue_counter.most_common(5)]

    return {
        "product_id": product_id,
        "window": {
            "start_date": start_date,
            "end_date": end_date,
            "days": _days_between(start_date, end_date),
        },
        "review_count": total_reviews,
        "average_sentiment_score": average_score,
        "sentiment_breakdown": sentiment_counts,
        "high_urgency_count": high_urgency_count,
        "top_issues": top_issues,
        "sentiment_trend": sentiment_trend,
    }


def _days_between(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return (end - start).days + 1


def _to_float(value) -> float:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value))


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
