"""
verite/tests/test_query_api.py

Unit tests for the Week 3 query API aggregation logic.
"""

import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from functions.query_api.handler import _build_summary, lambda_handler


SAMPLE_ITEMS = [
    {
        "product_id": "B08XYZ1234",
        "review_sort_key": "2025-11-10#R100",
        "review_date": "2025-11-10",
        "sentiment": "negative",
        "score": Decimal("0.12"),
        "issues": ["battery dies quickly", "charging port loose"],
        "urgency": "high",
    },
    {
        "product_id": "B08XYZ1234",
        "review_sort_key": "2025-11-10#R101",
        "review_date": "2025-11-10",
        "sentiment": "neutral",
        "score": Decimal("0.52"),
        "issues": ["charging port loose"],
        "urgency": "medium",
    },
    {
        "product_id": "B08XYZ1234",
        "review_sort_key": "2025-11-11#R102",
        "review_date": "2025-11-11",
        "sentiment": "positive",
        "score": Decimal("0.91"),
        "issues": [],
        "urgency": "low",
    },
]


class TestBuildSummary(unittest.TestCase):
    def test_aggregates_sentiment_issues_and_trend(self):
        summary = _build_summary(
            product_id="B08XYZ1234",
            start_date="2025-11-10",
            end_date="2025-11-11",
            items=SAMPLE_ITEMS,
        )

        self.assertEqual(summary["review_count"], 3)
        self.assertEqual(summary["high_urgency_count"], 1)
        self.assertEqual(summary["sentiment_breakdown"]["positive"], 1)
        self.assertEqual(summary["sentiment_breakdown"]["neutral"], 1)
        self.assertEqual(summary["sentiment_breakdown"]["negative"], 1)

        self.assertEqual(summary["top_issues"][0]["issue"], "charging port loose")
        self.assertEqual(summary["top_issues"][0]["count"], 2)

        self.assertEqual(len(summary["sentiment_trend"]), 2)
        self.assertEqual(summary["sentiment_trend"][0]["date"], "2025-11-10")
        self.assertEqual(summary["sentiment_trend"][0]["total_reviews"], 2)
        self.assertEqual(summary["sentiment_trend"][1]["date"], "2025-11-11")
        self.assertEqual(summary["sentiment_trend"][1]["total_reviews"], 1)


class TestLambdaHandler(unittest.TestCase):
    @patch("functions.query_api.handler._query_reviews")
    @patch("functions.query_api.handler._get_table")
    def test_returns_200_for_valid_request(self, mock_get_table, mock_query_reviews):
        mock_get_table.return_value = MagicMock()
        mock_query_reviews.return_value = SAMPLE_ITEMS

        event = {
            "pathParameters": {"product_id": "B08XYZ1234"},
            "queryStringParameters": {"days": "7", "end_date": "2025-11-11"},
        }

        response = lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 200)

        body = json.loads(response["body"])
        self.assertEqual(body["product_id"], "B08XYZ1234")
        self.assertEqual(body["review_count"], 3)

    @patch("functions.query_api.handler._get_table")
    def test_returns_400_for_invalid_days(self, mock_get_table):
        mock_get_table.return_value = MagicMock()
        event = {
            "pathParameters": {"product_id": "B08XYZ1234"},
            "queryStringParameters": {"days": "abc"},
        }

        response = lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 400)
        body = json.loads(response["body"])
        self.assertIn("days", body["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
