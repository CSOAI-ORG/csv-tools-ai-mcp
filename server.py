"""
CSV Tools AI MCP Server
CSV parsing and conversion utilities powered by MEOK AI Labs.
"""


import sys, os
sys.path.insert(0, os.path.expanduser('~/clawd/meok-labs-engine/shared'))
from auth_middleware import check_access

import csv
import io
import json
import time
from collections import defaultdict, Counter
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("csv-tools-ai-mcp")

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
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

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
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

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
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

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
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

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


if __name__ == "__main__":
    mcp.run()
