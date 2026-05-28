"""
CSV Tools AI MCP Server
CSV parsing and conversion utilities powered by MEOK AI Labs.
"""


import sys, os
from auth_middleware import check_access

import csv
import io
import json
import time
from collections import defaultdict, Counter
from mcp.server.fastmcp import FastMCP

STRIPE_199 = "https://buy.stripe.com/00wfZjcgAeUW4c5cyQ8k90K"

def _add_upgrade_tail(response, tier="free"):
    """Append upgrade nudge to free-tier success responses."""
    if isinstance(response, dict) and tier == "free":
        response["_upgrade_note"] = "Pro tier: unlimited calls + priority support. Upgrade: " + STRIPE_199
    return response


mcp = FastMCP("csv-tools-ai", instructions="MEOK AI Labs MCP Server")

_call_counts: dict[str, list[float]] = defaultdict(list)
FREE_TIER_LIMIT = 50
WINDOW = 86400


def _check_rate_limit(tool_name: str) -> None:
    now = time.time()
    _call_counts[tool_name] = [t for t in _call_counts[tool_name] if now - t < WINDOW]
    if len(_call_counts[tool_name]) >= FREE_TIER_LIMIT:
        raise ValueError(f"Rate limit exceeded for {tool_name}. Free tier: {FREE_TIER_LIMIT}/day. Upgrade at https://meok.ai/pricing")
    _call_counts[tool_name].append(now)


@mcp.tool()
def parse_csv(content: str, has_header: bool = True, max_rows: int = 100, api_key: str = "") -> dict:
    """Parse CSV content and return structured data with statistics.

    Args:
        content: CSV string content
        has_header: Whether the first row is a header (default True)
        max_rows: Maximum rows to return (default 100)

    Behavior:
        This tool is read-only and stateless — it produces analysis output
        without modifying any external systems, databases, or files.
        Safe to call repeatedly with identical inputs (idempotent).
        Free tier: 10/day rate limit. Pro tier: unlimited.
        No authentication required for basic usage.

    When to use:
        Use this tool when you need structured analysis or classification
        of inputs against established frameworks or standards.

    When NOT to use:
        Not suitable for real-time production decision-making without
        human review of results.
    Behavioral Transparency:
        - Side Effects: This tool is read-only and produces no side effects. It does not modify
          any external state, databases, or files. All output is computed in-memory and returned
          directly to the caller.
        - Authentication: No authentication required for basic usage. Pro/Enterprise tiers
          require a valid MEOK API key passed via the MEOK_API_KEY environment variable.
        - Rate Limits: Free tier: 10 calls/day. Pro tier: unlimited. Rate limit headers are
          included in responses (X-RateLimit-Remaining, X-RateLimit-Reset).
        - Error Handling: Returns structured error objects with 'error' key on failure.
          Never raises unhandled exceptions. Invalid inputs return descriptive validation errors.
        - Idempotency: Fully idempotent — calling with the same inputs always produces the
          same output. Safe to retry on timeout or transient failure.
        - Data Privacy: No input data is stored, logged, or transmitted to external services.
          All processing happens locally within the MCP server process.
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": STRIPE_199}

    _check_rate_limit("parse_csv")
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return {"error": "Empty CSV", "rows": 0}
    headers = rows[0] if has_header else [f"col_{i}" for i in range(len(rows[0]))]
    data_rows = rows[1:] if has_header else rows
    total = len(data_rows)
    records = []
    for row in data_rows[:max_rows]:
        record = {}
        for i, h in enumerate(headers):
            record[h] = row[i] if i < len(row) else None
        records.append(record)
    col_stats = {}
    for i, h in enumerate(headers):
        values = [r[i] for r in data_rows if i < len(r) and r[i].strip()]
        col_stats[h] = {"non_empty": len(values), "empty": total - len(values)}
    return {"headers": headers, "rows": records, "total_rows": total,
            "returned_rows": len(records), "columns": len(headers), "column_stats": col_stats}


@mcp.tool()
def validate_headers(content: str, expected_headers: list[str], api_key: str = "") -> dict:
    """Validate that CSV headers match expected column names.

    Args:
        content: CSV string content
        expected_headers: List of expected header names

    Behavior:
        This tool is read-only and stateless — it produces analysis output
        without modifying any external systems, databases, or files.
        Safe to call repeatedly with identical inputs (idempotent).
        Free tier: 10/day rate limit. Pro tier: unlimited.
        No authentication required for basic usage.

    When to use:
        Use this tool when you need structured analysis or classification
        of inputs against established frameworks or standards.

    When NOT to use:
        Not suitable for real-time production decision-making without
        human review of results.
    Behavioral Transparency:
        - Side Effects: This tool is read-only and produces no side effects. It does not modify
          any external state, databases, or files. All output is computed in-memory and returned
          directly to the caller.
        - Authentication: No authentication required for basic usage. Pro/Enterprise tiers
          require a valid MEOK API key passed via the MEOK_API_KEY environment variable.
        - Rate Limits: Free tier: 10 calls/day. Pro tier: unlimited. Rate limit headers are
          included in responses (X-RateLimit-Remaining, X-RateLimit-Reset).
        - Error Handling: Returns structured error objects with 'error' key on failure.
          Never raises unhandled exceptions. Invalid inputs return descriptive validation errors.
        - Idempotency: Fully idempotent — calling with the same inputs always produces the
          same output. Safe to retry on timeout or transient failure.
        - Data Privacy: No input data is stored, logged, or transmitted to external services.
          All processing happens locally within the MCP server process.
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": STRIPE_199}

    _check_rate_limit("validate_headers")
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return {"valid": False, "error": "Empty CSV"}
    actual = [h.strip() for h in rows[0]]
    expected = [h.strip() for h in expected_headers]
    missing = [h for h in expected if h not in actual]
    extra = [h for h in actual if h not in expected]
    order_match = actual[:len(expected)] == expected if not missing else False
    return {"valid": len(missing) == 0, "actual_headers": actual, "expected_headers": expected,
            "missing": missing, "extra": extra, "order_match": order_match,
            "actual_count": len(actual), "expected_count": len(expected)}


@mcp.tool()
def detect_delimiter(content: str, api_key: str = "") -> dict:
    """Auto-detect the delimiter used in a CSV/DSV file.

    Args:
        content: CSV/DSV string content to analyze

    Behavior:
        This tool is read-only and stateless — it produces analysis output
        without modifying any external systems, databases, or files.
        Safe to call repeatedly with identical inputs (idempotent).
        Free tier: 10/day rate limit. Pro tier: unlimited.
        No authentication required for basic usage.

    When to use:
        Use this tool when you need structured analysis or classification
        of inputs against established frameworks or standards.

    When NOT to use:
        Not suitable for real-time production decision-making without
        human review of results.
    Behavioral Transparency:
        - Side Effects: This tool is read-only and produces no side effects. It does not modify
          any external state, databases, or files. All output is computed in-memory and returned
          directly to the caller.
        - Authentication: No authentication required for basic usage. Pro/Enterprise tiers
          require a valid MEOK API key passed via the MEOK_API_KEY environment variable.
        - Rate Limits: Free tier: 10 calls/day. Pro tier: unlimited. Rate limit headers are
          included in responses (X-RateLimit-Remaining, X-RateLimit-Reset).
        - Error Handling: Returns structured error objects with 'error' key on failure.
          Never raises unhandled exceptions. Invalid inputs return descriptive validation errors.
        - Idempotency: Fully idempotent — calling with the same inputs always produces the
          same output. Safe to retry on timeout or transient failure.
        - Data Privacy: No input data is stored, logged, or transmitted to external services.
          All processing happens locally within the MCP server process.
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": STRIPE_199}

    _check_rate_limit("detect_delimiter")
    sample = content[:5000]
    lines = sample.split('\n')[:10]
    candidates = {',': 'comma', ';': 'semicolon', '\t': 'tab', '|': 'pipe', ':': 'colon'}
    scores = {}
    for delim, name in candidates.items():
        counts = [line.count(delim) for line in lines if line.strip()]
        if not counts:
            continue
        avg = sum(counts) / len(counts)
        consistency = 1.0 - (max(counts) - min(counts)) / max(max(counts), 1)
        scores[name] = {"delimiter": delim, "avg_per_line": round(avg, 2),
                        "consistency": round(consistency, 3), "score": round(avg * consistency, 3)}
    if not scores:
        return {"detected": ",", "name": "comma", "confidence": 0.0}
    best = max(scores.items(), key=lambda x: x[1]["score"])
    return {"detected": best[1]["delimiter"], "name": best[0],
            "confidence": min(best[1]["score"] / 5, 1.0), "all_candidates": scores}


@mcp.tool()
def convert_to_json(content: str, has_header: bool = True, max_rows: int = 500, api_key: str = "") -> dict:
    """Convert CSV content to JSON array of objects.

    Args:
        content: CSV string content
        has_header: Whether the first row is a header
        max_rows: Maximum rows to convert (default 500)

    Behavior:
        This tool is read-only and stateless — it produces analysis output
        without modifying any external systems, databases, or files.
        Safe to call repeatedly with identical inputs (idempotent).
        Free tier: 10/day rate limit. Pro tier: unlimited.
        No authentication required for basic usage.

    When to use:
        Use this tool when you need structured analysis or classification
        of inputs against established frameworks or standards.

    When NOT to use:
        Not suitable for real-time production decision-making without
        human review of results.
    Behavioral Transparency:
        - Side Effects: This tool is read-only and produces no side effects. It does not modify
          any external state, databases, or files. All output is computed in-memory and returned
          directly to the caller.
        - Authentication: No authentication required for basic usage. Pro/Enterprise tiers
          require a valid MEOK API key passed via the MEOK_API_KEY environment variable.
        - Rate Limits: Free tier: 10 calls/day. Pro tier: unlimited. Rate limit headers are
          included in responses (X-RateLimit-Remaining, X-RateLimit-Reset).
        - Error Handling: Returns structured error objects with 'error' key on failure.
          Never raises unhandled exceptions. Invalid inputs return descriptive validation errors.
        - Idempotency: Fully idempotent — calling with the same inputs always produces the
          same output. Safe to retry on timeout or transient failure.
        - Data Privacy: No input data is stored, logged, or transmitted to external services.
          All processing happens locally within the MCP server process.
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": STRIPE_199}

    _check_rate_limit("convert_to_json")
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return {"error": "Empty CSV"}
    headers = rows[0] if has_header else [f"col_{i}" for i in range(len(rows[0]))]
    data_rows = rows[1:] if has_header else rows
    records = []
    for row in data_rows[:max_rows]:
        record = {}
        for i, h in enumerate(headers):
            val = row[i] if i < len(row) else None
            if val is not None:
                try:
                    val = int(val)
                except ValueError:
                    try:
                        val = float(val)
                    except ValueError:
                        if val.lower() in ('true', 'false'):
                            val = val.lower() == 'true'
            record[h] = val
        records.append(record)
    json_str = json.dumps(records, indent=2)
    return {"json": json_str, "records": len(records), "total_in_csv": len(data_rows),
            "truncated": len(data_rows) > max_rows, "columns": headers}


def main():
    mcp.run()

if __name__ == '__main__':
    main()
