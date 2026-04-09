"""
verite/functions/enricher/bedrock_client.py

Handles all communication with Amazon Bedrock.
Sends a review to Claude and returns a structured enrichment result.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# The Claude model hosted on Amazon Bedrock.
# Using Claude 3.5 Sonnet — strong structured output reliability.
BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# The system prompt is the core of Verite's intelligence.
# It instructs the model to return ONLY valid JSON — no prose, no markdown fences.
SYSTEM_PROMPT = """
You are a product quality analyst for an e-commerce marketplace.
Given a customer review, extract structured intelligence that helps the seller
understand what is working and what needs to be fixed.

Return ONLY a valid JSON object with exactly this schema — no preamble,
no explanation, no markdown code fences:

{
  "sentiment": "<positive | neutral | negative>",
  "score": <float between 0.0 and 1.0, where 1.0 is most positive>,
  "issues": [<list of specific product problems mentioned, max 5 strings>],
  "praise": [<list of specific positives mentioned, max 5 strings>],
  "suggested_action": "<one clear, actionable sentence for the seller>",
  "urgency": "<low | medium | high>"
}

Urgency rules:
- high: safety concerns, product broken on arrival, or multiple severe defects
- medium: recurring quality issues or significant feature failures
- low: minor inconveniences or mostly positive with small complaints
"""


def build_user_message(review: dict) -> str:
    """
    Formats a review dict into the prompt text sent to the model.
    Including the rating gives the model a calibration signal alongside the text.
    """
    return (
        f"Product: {review['product_name']}\n"
        f"Rating: {review['rating']} out of 5\n"
        f"Title: {review['title']}\n"
        f"Review: {review['body']}"
    )


def invoke_bedrock(review: dict, region: str = "us-east-2") -> dict:
    """
    Sends a single review to Bedrock and returns the parsed enrichment result.

    Args:
        review: A review dict with keys: product_name, rating, title, body
        region: AWS region where Bedrock is enabled (default: us-east-2)

    Returns:
        A dict with keys: sentiment, score, issues, praise, suggested_action, urgency

    Raises:
        BedrockInvocationError: If the API call fails or returns unparseable output.
    """
    client = boto3.client("bedrock-runtime", region_name=region)

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": build_user_message(review)
            }
        ]
    }

    try:
        response = client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body),
            contentType="application/json",
            accept="application/json"
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        raise BedrockInvocationError(
            f"Bedrock API call failed [{error_code}]: {e}"
        ) from e

    raw_body = response["body"].read()
    response_json = json.loads(raw_body)

    # Bedrock returns the model output inside content[0].text
    raw_text = response_json["content"][0]["text"].strip()

    return _parse_enrichment(raw_text)


def _parse_enrichment(raw_text: str) -> dict:
    """
    Parses the model's raw text response into a validated enrichment dict.
    The model is instructed to return only JSON, but we defensively strip
    any accidental markdown fences just in case.

    Args:
        raw_text: The string returned by the model.

    Returns:
        A validated enrichment dict.

    Raises:
        BedrockInvocationError: If the text cannot be parsed or is missing required keys.
    """
    # Strip markdown code fences if the model accidentally included them
    cleaned = raw_text
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        cleaned = "\n".join(
            line for line in lines
            if not line.strip().startswith("```")
        )

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise BedrockInvocationError(
            f"Model returned non-JSON output. Raw text: {raw_text[:300]}"
        ) from e

    _validate_enrichment(result)
    return result


def _validate_enrichment(data: dict) -> None:
    """
    Ensures the parsed dict has all required keys and valid values.
    Raises BedrockInvocationError if validation fails.
    """
    required_keys = {"sentiment", "score", "issues", "praise", "suggested_action", "urgency"}
    missing = required_keys - data.keys()
    if missing:
        raise BedrockInvocationError(f"Enrichment result missing keys: {missing}")

    valid_sentiments = {"positive", "neutral", "negative"}
    if data["sentiment"] not in valid_sentiments:
        raise BedrockInvocationError(
            f"Invalid sentiment value: '{data['sentiment']}'. "
            f"Must be one of {valid_sentiments}"
        )

    valid_urgencies = {"low", "medium", "high"}
    if data["urgency"] not in valid_urgencies:
        raise BedrockInvocationError(
            f"Invalid urgency value: '{data['urgency']}'. "
            f"Must be one of {valid_urgencies}"
        )

    if not isinstance(data["score"], (int, float)) or not (0.0 <= data["score"] <= 1.0):
        raise BedrockInvocationError(
            f"score must be a float between 0.0 and 1.0, got: {data['score']}"
        )

    if not isinstance(data["issues"], list):
        raise BedrockInvocationError("'issues' must be a list")

    if not isinstance(data["praise"], list):
        raise BedrockInvocationError("'praise' must be a list")


class BedrockInvocationError(Exception):
    """Raised when a Bedrock call fails or returns an invalid response."""
    pass
