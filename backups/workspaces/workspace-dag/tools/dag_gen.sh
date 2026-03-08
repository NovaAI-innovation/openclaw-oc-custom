#!/bin/bash
set -e
cd /root/.openclaw/workspace-brief-dag-integrator
source venv/bin/activate
python3 dag_builder.py
