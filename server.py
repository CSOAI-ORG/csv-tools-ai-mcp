"""
CSV Tools AI MCP Server
CSV parsing and conversion tools powered by MEOK AI Labs.
"""

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
def parse_csv(content: str, has_header: bool = True, max_rows: int = 100) -> dict:
    """Parse CSV content and return structured data with statistics.

    Args:
        content: CSV string content
        has_header: Whether first row is a header
        max_rows: Maximum rows to return (default 100)
    """
    _check_rate_limit("parse_csv")
    try:
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            return {"error": "Empty CSV"}
        headers = rows[0] if has_header else [f"col_{i}" for i in range(len(rows[0]))]
        data_rows = rows[1:] if has_header else rows
        records = []
        for row in data_rows[:max_rows]:
            record = {}
            for i, val in enumerate(row):
                key = headers[i] if i < len(headers) else f"col_{i}"
                record[key] = val
            records.append(record)
        col_stats = {}
        for i, h in enumerate(headers):
            vals = [r[i] for r in data_rows if i < len(r)]
            numeric = []
            for v in vals:
                try:
                    numeric.append(float(v))
                except (ValueError, TypeError):
                    pass
            col_stats[h] = {"total": len(vals), "empty": vals.count(""),
                           "numeric_count": len(numeric), "unique": len(set(vals))}
        return {"headers": headers, "rows": records, "total_rows": len(data_rows),
                "returned_rows": len(records), "columns": len(headers), "column_stats": col_stats}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def validate_headers(content: str, expected_headers: list[str], strict: bool = False) -> dict:
    """Validate CSV headers against expected column names.

    Args:
        content: CSV string content
        expected_headers: List of expected header names
        strict: If True, headers must match exactly (order and count)
    """
    _check_rate_limit("validate_headers")
    try:
        reader = csv.reader(io.StringIO(content))
        actual = next(reader, [])
        actual_lower = [h.strip().lower() for h in actual]
        expected_lower = [h.strip().lower() for h in expected_headers]
        missing = [h for h in expected_headers if h.strip().lower() not in actual_lower]
        extra = [h for h in actual if h.strip().lower() not in expected_lower]
        if strict:
            valid = actual_lower == expected_lower
        else:
            valid = len(missing) == 0
        return {"valid": valid, "actual_headers": actual, "expected_headers": expected_headers,
                "missing": missing, "extra": extra, "strict_mode": strict}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def detect_delimiter(content: str) -> dict:
    """Auto-detect the delimiter used in a CSV/DSV file.

    Args:
        content: File content to analyze
    """
    _check_rate_limit("detect_delimiter")
    delimiters = {',': 'comma', '\t': 'tab', ';': 'semicolon', '|': 'pipe', ':': 'colon'}
    sample = content[:5000]
    lines = sample.strip().split('\n')[:10]
    scores = {}
    for delim, name in delimiters.items():
        counts = [line.count(delim) for line in lines if line.strip()]
        if not counts:
            continue
        avg = sum(counts) / len(counts)
        consistency = 1 - (max(counts) - min(counts)) / max(max(counts), 1)
        scores[name] = {"delimiter": delim, "avg_per_line": round(avg, 2),
                        "consistency": round(consistency, 3), "score": round(avg * consistency, 3)}
    if not scores:
        return {"detected": None, "error": "No delimiters found"}
    best = max(scores.items(), key=lambda x: x[1]["score"])
    return {"detected": best[1]["delimiter"], "detected_name": best[0],
            "all_scores": scores, "lines_analyzed": len(lines)}


@mcp.tool()
def convert_to_json(content: str, has_header: bool = True, max_rows: int = 500) -> dict:
    """Convert CSV content to JSON array format.

    Args:
        content: CSV string content
        has_header: Whether first row is a header
        max_rows: Maximum rows to convert
    """
    _check_rate_limit("convert_to_json")
    try:
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            return {"error": "Empty CSV"}
        headers = rows[0] if has_header else [f"col_{i}" for i in range(len(rows[0]))]
        data_rows = rows[1:] if has_header else rows
        records = []
        for row in data_rows[:max_rows]:
            record = {}
            for i, val in enumerate(row):
                key = headers[i] if i < len(headers) else f"col_{i}"
                # Auto-type detection
                if val.lower() in ('true', 'false'):
                    record[key] = val.lower() == 'true'
                elif val == '':
                    record[key] = None
                else:
                    try:
                        record[key] = int(val)
                    except ValueError:
                        try:
                            record[key] = float(val)
                        except ValueError:
                            record[key] = val
            records.append(record)
        json_str = json.dumps(records, indent=2)
        return {"json": json_str, "records": len(records), "total_available": len(data_rows),
                "columns": headers}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run()
