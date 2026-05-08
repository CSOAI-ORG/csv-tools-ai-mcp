<div align="center">

# Csv Tools Ai MCP

**CSV Tools AI MCP Server**

[![PyPI](https://img.shields.io/pypi/v/meok-csv-tools-ai-mcp)](https://pypi.org/project/meok-csv-tools-ai-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![MEOK AI Labs](https://img.shields.io/badge/MEOK_AI_Labs-MCP_Server-purple)](https://meok.ai)

</div>

## Overview

CSV Tools AI MCP Server
CSV parsing and conversion utilities powered by MEOK AI Labs.

## Tools

| Tool | Description |
|------|-------------|
| `parse_csv` | Parse CSV content and return structured data with statistics. |
| `validate_headers` | Validate that CSV headers match expected column names. |
| `detect_delimiter` | Auto-detect the delimiter used in a CSV/DSV file. |
| `convert_to_json` | Convert CSV content to JSON array of objects. |

## Installation

```bash
pip install meok-csv-tools-ai-mcp
```

## Usage with Claude Desktop

Add to your Claude Desktop MCP config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "csv-tools-ai": {
      "command": "python",
      "args": ["-m", "meok_csv_tools_ai_mcp.server"]
    }
  }
}
```

## Usage with FastMCP

```python
from mcp.server.fastmcp import FastMCP

# This server exposes 4 tool(s) via MCP
# See server.py for full implementation
```

## License

MIT © [MEOK AI Labs](https://meok.ai)
