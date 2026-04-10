"""
verite/tests/test_enricher.py

Unit tests for Verite's enrichment logic.

These tests cover the parsing and validation layer in bedrock_client.py
without making any real Bedrock API calls — all LLM responses are mocked.

Run with:
    pytest tests/test_enricher.py -v
"""

import json
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Add project root to path so package imports resolve in pytest.
sys.path.insert(0, str(Path(__file__).parent.parent))

from functions.enricher.bedrock_client import (
    _parse_enrichment,
    _validate_enrichment,
    build_user_message,
    invoke_bedrock,
    BedrockInvocationError,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

VALID_ENRICHMENT = {
    "sentiment": "negative",
    "score": 0.15,
    "issues": ["left earbud stopped working", "charging case scratches easily"],
    "praise": ["good initial sound quality"],
    "suggested_action": "Investigate durability of left earbud connection and improve case coating.",
    "urgency": "medium"
}

SAMPLE_REVIEW = {
    "review_id": "R001",
    "product_id": "B08XYZ1234",
    "product_name": "Wireless Bluetooth Headphones",
    "rating": 2,
    "title": "Stopped working after 2 weeks",
    "body": "The sound quality was great at first but the left earbud stopped working."
}


# ── Tests: build_user_message ─────────────────────────────────────────────────

class TestBuildUserMessage(unittest.TestCase):

    def test_includes_all_review_fields(self):
        msg = build_user_message(SAMPLE_REVIEW)
        self.assertIn("Wireless Bluetooth Headphones", msg)
        self.assertIn("2", msg)
        self.assertIn("Stopped working after 2 weeks", msg)
        self.assertIn("left earbud stopped working", msg)

    def test_includes_rating_label(self):
        msg = build_user_message(SAMPLE_REVIEW)
        self.assertIn("Rating:", msg)

    def test_includes_product_label(self):
        msg = build_user_message(SAMPLE_REVIEW)
        self.assertIn("Product:", msg)


# ── Tests: _parse_enrichment ──────────────────────────────────────────────────

class TestParseEnrichment(unittest.TestCase):

    def test_parses_valid_json(self):
        raw = json.dumps(VALID_ENRICHMENT)
        result = _parse_enrichment(raw)
        self.assertEqual(result["sentiment"], "negative")
        self.assertAlmostEqual(result["score"], 0.15)
        self.assertEqual(len(result["issues"]), 2)

    def test_strips_markdown_fences(self):
        raw = f"```json\n{json.dumps(VALID_ENRICHMENT)}\n```"
        result = _parse_enrichment(raw)
        self.assertEqual(result["urgency"], "medium")

    def test_strips_plain_code_fences(self):
        raw = f"```\n{json.dumps(VALID_ENRICHMENT)}\n```"
        result = _parse_enrichment(raw)
        self.assertEqual(result["sentiment"], "negative")

    def test_raises_on_non_json(self):
        with self.assertRaises(BedrockInvocationError):
            _parse_enrichment("This is not JSON at all.")

    def test_raises_on_truncated_json(self):
        with self.assertRaises(BedrockInvocationError):
            _parse_enrichment('{"sentiment": "positive", "score": 0.9')


# ── Tests: _validate_enrichment ───────────────────────────────────────────────

class TestValidateEnrichment(unittest.TestCase):

    def test_passes_on_valid_data(self):
        # Should not raise
        _validate_enrichment(VALID_ENRICHMENT)

    def test_raises_on_missing_key(self):
        incomplete = {k: v for k, v in VALID_ENRICHMENT.items() if k != "urgency"}
        with self.assertRaises(BedrockInvocationError) as ctx:
            _validate_enrichment(incomplete)
        self.assertIn("urgency", str(ctx.exception))

    def test_raises_on_invalid_sentiment(self):
        bad = {**VALID_ENRICHMENT, "sentiment": "mixed"}
        with self.assertRaises(BedrockInvocationError) as ctx:
            _validate_enrichment(bad)
        self.assertIn("sentiment", str(ctx.exception))

    def test_raises_on_invalid_urgency(self):
        bad = {**VALID_ENRICHMENT, "urgency": "critical"}
        with self.assertRaises(BedrockInvocationError) as ctx:
            _validate_enrichment(bad)
        self.assertIn("urgency", str(ctx.exception))

    def test_raises_on_score_out_of_range(self):
        bad = {**VALID_ENRICHMENT, "score": 1.5}
        with self.assertRaises(BedrockInvocationError) as ctx:
            _validate_enrichment(bad)
        self.assertIn("score", str(ctx.exception))

    def test_raises_on_score_below_zero(self):
        bad = {**VALID_ENRICHMENT, "score": -0.1}
        with self.assertRaises(BedrockInvocationError):
            _validate_enrichment(bad)

    def test_raises_if_issues_not_a_list(self):
        bad = {**VALID_ENRICHMENT, "issues": "broken hinge"}
        with self.assertRaises(BedrockInvocationError):
            _validate_enrichment(bad)

    def test_raises_if_praise_not_a_list(self):
        bad = {**VALID_ENRICHMENT, "praise": "great sound"}
        with self.assertRaises(BedrockInvocationError):
            _validate_enrichment(bad)

    def test_accepts_score_boundary_zero(self):
        boundary = {**VALID_ENRICHMENT, "score": 0.0}
        _validate_enrichment(boundary)  # should not raise

    def test_accepts_score_boundary_one(self):
        boundary = {**VALID_ENRICHMENT, "score": 1.0}
        _validate_enrichment(boundary)  # should not raise

    def test_accepts_all_valid_sentiments(self):
        for s in ["positive", "neutral", "negative"]:
            data = {**VALID_ENRICHMENT, "sentiment": s}
            _validate_enrichment(data)  # should not raise

    def test_accepts_all_valid_urgencies(self):
        for u in ["low", "medium", "high"]:
            data = {**VALID_ENRICHMENT, "urgency": u}
            _validate_enrichment(data)  # should not raise


# ── Tests: invoke_bedrock (mocked) ────────────────────────────────────────────

class TestInvokeBedrock(unittest.TestCase):

    def _make_mock_client(self, response_body: dict) -> MagicMock:
        """
        Builds a mock boto3 bedrock-runtime client that returns
        the given response_body when invoke_model is called.
        """
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps({
            "content": [{"text": json.dumps(response_body)}]
        }).encode()

        mock_client = MagicMock()
        mock_client.invoke_model.return_value = {"body": mock_response_body}
        return mock_client

    @patch("functions.enricher.bedrock_client.boto3.client")
    def test_returns_enrichment_on_success(self, mock_boto3_client):
        mock_boto3_client.return_value = self._make_mock_client(VALID_ENRICHMENT)

        result = invoke_bedrock(SAMPLE_REVIEW, region="us-east-1")

        self.assertEqual(result["sentiment"], "negative")
        self.assertAlmostEqual(result["score"], 0.15)
        self.assertIn("left earbud stopped working", result["issues"])

    @patch("functions.enricher.bedrock_client.boto3.client")
    def test_passes_correct_model_id(self, mock_boto3_client):
        mock_client = self._make_mock_client(VALID_ENRICHMENT)
        mock_boto3_client.return_value = mock_client

        invoke_bedrock(SAMPLE_REVIEW, region="us-east-1")

        call_kwargs = mock_client.invoke_model.call_args[1]
        self.assertIn("claude-sonnet-4-5", call_kwargs["modelId"])

    @patch("functions.enricher.bedrock_client.boto3.client")
    def test_raises_on_invalid_model_response(self, mock_boto3_client):
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps({
            "content": [{"text": "Sorry, I cannot help with that."}]
        }).encode()
        mock_client = MagicMock()
        mock_client.invoke_model.return_value = {"body": mock_response_body}
        mock_boto3_client.return_value = mock_client

        with self.assertRaises(BedrockInvocationError):
            invoke_bedrock(SAMPLE_REVIEW, region="us-east-1")

    @patch("functions.enricher.bedrock_client.boto3.client")
    def test_raises_on_boto3_client_error(self, mock_boto3_client):
        from botocore.exceptions import ClientError
        mock_client = MagicMock()
        mock_client.invoke_model.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "Not authorized"}},
            "InvokeModel"
        )
        mock_boto3_client.return_value = mock_client

        with self.assertRaises(BedrockInvocationError):
            invoke_bedrock(SAMPLE_REVIEW, region="us-east-1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
