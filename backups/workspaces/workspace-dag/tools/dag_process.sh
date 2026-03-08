#!/bin/bash
set -e
BRIEF_KEY="${1:-briefs/sample.json}"
SNAPSHOT_DIR="../snapshots"
cd /root/.openclaw/workspace-brief-dag-integrator
python3 process_briefs.py --brief_key "$BRIEF_KEY"
