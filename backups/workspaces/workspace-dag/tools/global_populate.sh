#!/bin/bash
set -e
SNAPSHOT_PATH="$1"
cd /root/.openclaw/shared/graph/scripts
python3 global_populator.py "$SNAPSHOT_PATH"
