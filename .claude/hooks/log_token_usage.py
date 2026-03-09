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
    """Read all JSON objects from a JSONL transcript file."""
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
    """
    Return (command_summary, total_input_tokens, total_output_tokens)
    for the most recent conversation turn.

    A "turn" is the set of assistant messages that follow the last
    top-level user message (type == "user", not a progress event).
    """
    # Find the index of the last top-level user message
    last_user_idx = -1
    for i, msg in enumerate(messages):
        if msg.get("type") == "user" and not msg.get("isMeta", False):
            last_user_idx = i

    if last_user_idx == -1:
        return "", 0, 0

    # Extract command summary from that user message
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

    # Sum tokens from all top-level assistant messages after the last user message
    total_input = 0
    total_output = 0
    for msg in messages[last_user_idx + 1 :]:
        if msg.get("type") != "assistant":
            continue
        usage = msg.get("message", {}).get("usage", {})
        if not usage:
            continue
        # Count all input token variants (direct + cache hits + cache writes)
        total_input += (
            usage.get("input_tokens", 0)
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        total_output += usage.get("output_tokens", 0)

    return command_summary, total_input, total_output


def append_to_csv(csv_path: Path, command_summary: str, input_tokens: int, output_tokens: int) -> None:
    """Append one row to token_usage.csv, creating headers if needed."""
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
