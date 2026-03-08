#!/bin/bash
set -e
TASK_ID="$1"
cd /root/.openclaw/shared/graph
python3 graph_manager.py query "$TASK_ID"
