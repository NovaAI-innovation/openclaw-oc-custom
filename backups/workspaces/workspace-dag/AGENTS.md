# Brief DAG Integrator Procedures

You are responsible for integrating project briefs from R2 into OpenClaw workflows.

## Core Workflow
1. Retrieve all JSON briefs from R2 bucket "openclaw" using exec tool for Python code.
2. If no briefs, create and upload a sample brief.
3. For each brief, parse tasks, build DAG with NetworkX, validate acyclic, topological sort.
4. Construct SnapshotV1 dict per schema.
5. Save snapshot to memory via exec CLI.
6. Schedule each task as adhoc via exec CLI.

## R2 Access
Use exec python3 -c "code" with:
- import os, json, uuid, networkx as nx, subprocess
- from /root/cloudflare_r2 import get_r2_client, upload_brief, download_brief
- access_key = os.environ.get(CLOUDFLARE_R2_ACCESS_KEY)
- secret_key = os.environ.get(CLOUDFLARE_R2_SECRET_KEY)
- account_id = os.environ.get(CLOUDFLARE_ACCOUNT_ID)
- bucket = os.environ.get(R2_BUCKET_NAME, openclaw)
- If None, exec openclaw secrets get CLOUDFLARE_R2_ACCESS_KEY etc. to set env.
- client = get_r2_client(access_key, secret_key, account_id)
- paginator = client.get_paginator(list_objects_v2)
- briefs = []
- for page in paginator.paginate(Bucket=bucket, Prefix=briefs/):
  for obj in page.get(Contents, []):
    if obj[Key].endswith(.json):
      content = download_brief(obj[Key], bucket, access_key, secret_key, account_id)
      briefs.append((obj[Key], json.loads(content)))
- If len(briefs)==0: sample = {"project": "Sample", "tasks": [{"id": "t1", "description": "Sample Task 1", "dependencies": []}, {"id": "t2", "description": "Sample Task 2", "dependencies": ["t1"]} ]}; upload_brief(sample, briefs/sample.json, bucket, access_key, secret_key, account_id); briefs = [(briefs/sample.json, sample)]

## DAG Building
For each brief:
- tasks = brief[tasks]
- G = nx.DiGraph()
- for t in tasks: G.add_node(t[id])
- for t in tasks: for dep in t.get(dependencies, []): G.add_edge(dep, t[id])
- if not nx.is_directed_acyclic_graph(G): error
- sorted_tasks = list(nx.topological_sort(G))
- ordered_tasks = [next(t for t in tasks if t[id]==node) for node in sorted_tasks]

## SnapshotV1 Schema
snapshot = {
  "deselect_chat": False,
  "context": {},
  "contexts": [],
  "tasks": [{"id": t[id], "description": t[description], "dependencies": t.get(dependencies, []), "status": "pending"} for t in ordered_tasks],
  "logs": [],
  "log_guid": str(uuid.uuid4()),
  "log_version": 1,
  "log_progress": 0,
  "log_progress_active": False,
  "paused": False,
  "notifications": [],
  "notifications_guid": str(uuid.uuid4()),
  "notifications_version": 1
}
- import json; snapshot_json = json.dumps(snapshot)
- subprocess.run([NO_COLOR=1, openclaw, memory, save, --text, snapshot_json, --json], check=True)

## Task Scheduling
For each task in snapshot[tasks]:
- subprocess.run([NO_COLOR=1, openclaw, scheduler:create_adhoc_task, --name, ftask_{task[id]}, --system-prompt, Task executor, --prompt, fExecute: {task[description]}, --dedicated-context, true, --yes], check=True)

## Governance
- Maintain clean boundaries: Separate R2, DAG, snapshot, memory, scheduler.
- Preserve SnapshotV1 schema exactly.
- No hidden coupling; explicit exec for all external ops.
- Validate DAG before proceeding.
- Log errors via print for observability.
## OpenClaw Secrets Retrieval in Python\n\nFor credential propagation in agent scripts (e.g., cloudflare_r2.py), use subprocess to fetch secrets securely:\n\n\n\nThis ensures secrets are fetched at runtime without hardcoding or env exposure.
## OpenClaw Secrets Retrieval in Python

For credential propagation in agent scripts (e.g., cloudflare_r2.py), use subprocess to fetch secrets securely:

```python
import subprocess

account_id = subprocess.check_output(['openclaw', 'secrets', 'get', 'CLOUDFLARE_ACCOUNT_ID']).decode().strip()
access_key = subprocess.check_output(['openclaw', 'secrets', 'get', 'CLOUDFLARE_R2_ACCESS_KEY']).decode().strip()
secret_key = subprocess.check_output(['openclaw', 'secrets', 'get', 'CLOUDFLARE_R2_SECRET_KEY']).decode().strip()

# Use these in boto3 client initialization, e.g.,
s3_client = boto3.client('s3',
    endpoint_url='https://' + account_id + '.r2.cloudflarestorage.com',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
```

This ensures secrets are fetched at runtime without hardcoding or env exposure.
