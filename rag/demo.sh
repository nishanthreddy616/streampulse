#!/usr/bin/env bash

set -euo pipefail

echo "StreamPulse governed metrics assistant"
echo "--------------------------------------"

echo
echo "Northstar latest metrics:"
python rag/ask_metrics.py title-northstar

echo
echo "Afterglow latest metrics:"
python rag/ask_metrics.py title-afterglow
