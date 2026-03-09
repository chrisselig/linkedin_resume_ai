# Setup Token Tracking Command

Set up automated token usage tracking for this project using a Claude Code Stop hook.

## Process

### STEP 1 — Check for existing setup
- Check if `.claude/hooks/log_token_usage.py` already exists. If it does, tell the user and stop.
- Check if `.claude/settings.json` already exists. If it does, read it before proceeding so you can merge the new hook rather than overwrite existing settings.

### STEP 2 — Add token_usage.csv to .gitignore
- Read the existing `.gitignore`
- If `token_usage.csv` is not already present, add it under a `# Token usage tracking` comment at the top of the file
- If there is no `.gitignore`, create one with just that entry

### STEP 3 — Create the hook script
Create `.claude/hooks/log_token_usage.py` with the following implementation:

```python
#!/usr/bin/env python3
"""
Claude Code Stop hook — appends per-turn token usage to token_usage.csv.

Receives a JSON payload via stdin with:
  transcript_path: path to the session's .jsonl transcript file
  cwd: working directory of the Claude Code session

Sums input_tokens + cache_creation_input_tokens + cache_read_input_tokens
and output_tokens across all assistant messages in the most recent turn,
then appends one row to token_usage.csv in the project root.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_transcript(path: str) -> list[dict]:
    messages = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return messages


def extract_last_turn(messages: list[dict]) -> tuple[str, int, int]:
    last_user_idx = -1
    for i, msg in enumerate(messages):
        if msg.get("type") == "user" and not msg.get("isMeta", False):
            last_user_idx = i

    if last_user_idx == -1:
        return "", 0, 0

    command_summary = ""
    user_msg = messages[last_user_idx]
    content = user_msg.get("message", {}).get("content", "")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                command_summary = block.get("text", "")[:100].replace("\n", " ").strip()
                break
    elif isinstance(content, str):
        command_summary = content[:100].replace("\n", " ").strip()

    total_input = 0
    total_output = 0
    for msg in messages[last_user_idx + 1 :]:
        if msg.get("type") != "assistant":
            continue
        usage = msg.get("message", {}).get("usage", {})
        if not usage:
            continue
        total_input += (
            usage.get("input_tokens", 0)
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        total_output += usage.get("output_tokens", 0)

    return command_summary, total_input, total_output


def append_to_csv(csv_path: Path, command_summary: str, input_tokens: int, output_tokens: int) -> None:
    file_exists = csv_path.exists()
    now = datetime.now(timezone.utc)

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp",
                "date",
                "command_summary",
                "input_tokens",
                "output_tokens",
                "total_tokens",
            ])
        writer.writerow([
            now.isoformat(),
            now.strftime("%Y-%m-%d"),
            command_summary,
            input_tokens,
            output_tokens,
            input_tokens + output_tokens,
        ])


def main() -> None:
    raw = sys.stdin.read().strip()
    if not raw:
        return
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return

    transcript_path = payload.get("transcript_path", "")
    cwd = payload.get("cwd", os.getcwd())

    if not transcript_path or not os.path.exists(transcript_path):
        return

    messages = parse_transcript(transcript_path)
    if not messages:
        return

    command_summary, input_tokens, output_tokens = extract_last_turn(messages)
    if input_tokens == 0 and output_tokens == 0:
        return

    csv_path = Path(cwd) / "token_usage.csv"
    append_to_csv(csv_path, command_summary, input_tokens, output_tokens)


if __name__ == "__main__":
    main()
```

Make the file executable with `chmod +x .claude/hooks/log_token_usage.py`.

### STEP 4 — Register the hook in .claude/settings.json
- If `.claude/settings.json` does not exist, create it with:
```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 .claude/hooks/log_token_usage.py"
          }
        ]
      }
    ]
  }
}
```
- If `.claude/settings.json` already exists, merge the Stop hook into the existing hooks configuration without removing any existing hooks.

### STEP 5 — Smoke test
Find the most recent `.jsonl` transcript file for this project under `~/.claude/projects/`. Run:
```bash
echo '{"transcript_path":"<path>","cwd":"<cwd>"}' | python3 .claude/hooks/log_token_usage.py
```
Confirm the script exits without error and that a row was appended to `token_usage.csv`. Show the last 3 rows of the CSV to the user.

### STEP 6 — Commit
- Create a feature branch: `feature_token_tracking`
- Stage `.gitignore`, `.claude/hooks/log_token_usage.py`, `.claude/settings.json`
- Commit with message: `Add automated token usage tracking via Claude Code Stop hook`
- Push to origin

## What Gets Created
| File | Purpose |
|------|---------|
| `.claude/hooks/log_token_usage.py` | Stop hook that reads the transcript and writes to CSV |
| `.claude/settings.json` | Registers the hook on the Stop event |
| `token_usage.csv` | Local-only CSV (git-ignored); one row per Claude response |

## CSV Schema
`timestamp, date, command_summary, input_tokens, output_tokens, total_tokens`

Input tokens include cache_creation and cache_read tokens. The CSV is never committed — it stays local to each developer's machine.
