#!/bin/bash
set -e
cd /root/.openclaw/workspace-brief-dag-integrator
source venv/bin/activate
./tools/brief_gen.sh "Launch campaign for product X"  # Hardcoded sample
./tools/plan_gen.sh "orgs/org_agency/artifacts/briefs/2026/03/brief_01JNZ9A0Q2V3X4Y5Z6A7B8C9D/brief.json"
./tools/dag_gen.sh "orgs/org_agency/artifacts/plans/2026/03/plan_01JNZ9PLAN1234567890AB/plan.json"
./tools/run_init.sh "orgs/org_agency/artifacts/dags/2026/03/dag_01JNZ9DAG1234567890AB/dag.json"
./tools/graph_merge.sh "orgs/org_agency/artifacts/dags/2026/03/dag_01JNZ9DAG1234567890AB/dag.json"
echo "Full chain complete: Artifacts generated, graph merged"
