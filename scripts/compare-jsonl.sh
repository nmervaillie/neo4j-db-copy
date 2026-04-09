#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <left.jsonl|jsonl.gz> <right.jsonl|jsonl.gz>" >&2
  exit 2
fi

read_jsonl() {
  case "$1" in
    *.gz) gzip -dc -- "$1" ;;
    *) cat -- "$1" ;;
  esac
}

normalize() {
  jq -S -c '
    if .recordType == "node" then
      {
        recordType: "node",
        labels: (.labels | sort),
        properties: .properties
      }
    elif .recordType == "relationship" then
      {
        recordType: "relationship",
        type: .type,
        properties: .properties
      }
    else
      error("Unsupported recordType: \(.recordType)")
    end
  '
}

if diff -u \
  <(read_jsonl "$1" | sed "/^[[:space:]]*$/d" | normalize | LC_ALL=C sort) \
  <(read_jsonl "$2" | sed "/^[[:space:]]*$/d" | normalize | LC_ALL=C sort); then
  echo "JSONL content matches."
else
  echo "JSONL content differs." >&2
  exit 1
fi
