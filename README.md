# CSV Tools AI MCP Server

> By [MEOK AI Labs](https://meok.ai) — CSV parsing, conversion, and data extraction utilities

## Installation

```bash
pip install csv-tools-ai-mcp
```

## Usage

```bash
python server.py
```

## Tools

### `parse_csv`
Parse CSV content and return structured data with column statistics.

**Parameters:**
- `content` (str): CSV string content
- `has_header` (bool): Whether the first row is a header (default True)
- `max_rows` (int): Maximum rows to return (default 100)

Additional tools for CSV conversion, filtering, and data transformation are available. See `server.py` for the full tool catalog.

## Authentication

Free tier: 50 calls/day. Upgrade at [meok.ai/pricing](https://meok.ai/pricing) for unlimited access.

## License

MIT — MEOK AI Labs
