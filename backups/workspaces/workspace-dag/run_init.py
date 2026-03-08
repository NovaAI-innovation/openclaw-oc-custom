import json
import ulid
import hashlib
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

class RunInitializer:
    def __init__(self, bucket="openclaw", account_id="554cefcc58bccd23ab124a9c06582d06", access_key="972d38bf8ca554615f49ed091b1b8851", secret_key="93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1" ):
        self.s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com")
        self.bucket = bucket
        self.org_id = "org_agency"
        self.project_id = "project_01JNZ8ZZZZZZZZZZZZZZZZZZ"
        self.client_id = "client_01JNZ8YYYYYYYYYYYYYYYYYY"
        self.domain_id = "domain_marketing_advertising"

    def load_dag(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            print(f"Error loading DAG: {e}")
            return None

    def init_run(self, dag_envelope):
        now = datetime.now(timezone.utc).isoformat().replace(+00:00, Z)
        run_ulid = ulid.ULID()
        run_id = f"run_{str(run_ulid)}"

        run_payload = {
            "run_status": "ready",
            "root_dag_id": dag_envelope["artifact_id"],
            "orchestrator_id": "global_graph_manager",
            "started_at": now,
            "ended_at": None,
            "active_node_ids": [n["node_id"] for n in dag_envelope["dag"]["nodes"]],
            "completed_node_ids": [],
            "failed_node_ids": [],
            "blocked_node_ids": [],
            "event_stream_ref": {
                "store": "r2",
                "bucket": self.bucket,
                "key": f"orgs/{self.org_id}/artifacts/runs/2026/03/{run_id}/events.jsonl"
            },
            "metrics": {
                "total_nodes": len(dag_envelope["dag"]["nodes"]),
                "completed_nodes": 0,
                "failed_nodes": 0
            }
        }

        envelope = {
            "schema_version": "1.0.0",
            "artifact_type": "run",
            "artifact_id": run_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "created_at": now,
            "updated_at": now,
            "created_by": {"actor_type": "system", "actor_id": "global_graph_manager"},
            "source": {"source_type": "artifact", "source_ref": dag_envelope["artifact_id"]},
            "status": "ready",
            "version": {"artifact_version": 1, "parent_artifact_id": dag_envelope["artifact_id"], "parent_artifact_version": dag_envelope["version"]["artifact_version"], "content_hash": ""},
            "provenance": {"generated_from": [dag_envelope["artifact_id"]], "derived_artifacts": [], "artifact_refs": []},
            "visibility": {"scope": "project", "allowed_scopes": ["org", f"domain:{self.domain_id}", f"project:{self.project_id}"], "denied_scopes": []},
            "tags": ["run", "runtime"],
            "run": run_payload
        }

        payload_json = json.dumps(envelope["run"], sort_keys=True)
        hash_obj = hashlib.sha256(payload_json.encode())
        content_hash = "sha256:" + hash_obj.hexdigest()
        envelope["version"]["content_hash"] = content_hash

        year = now[:4]
        month = now[5:7]
        artifact_key = f"orgs/{self.org_id}/artifacts/runs/{year}/{month}/{run_id}/run.json"
        manifest_key = f"orgs/{self.org_id}/artifacts/runs/{year}/{month}/{run_id}/manifest.json"
        events_key = f"orgs/{self.org_id}/artifacts/runs/{year}/{month}/{run_id}/events.jsonl"

        self.s3.put_object(Bucket=self.bucket, Key=artifact_key, Body=json.dumps(envelope, indent=2).encode("utf-8"), ContentType="application/json")
        manifest = {
            "artifact_id": run_id,
            "artifact_type": "run",
            "schema_version": "1.0.0",
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "artifact_version": 1,
            "status": "ready",
            "created_at": now,
            "updated_at": now,
            "title": f"Run for {dag_envelope[dag][root_plan_id]}",
            "path_hint": artifact_key,
            "content_hash": content_hash,
            "derived_from": [dag_envelope["artifact_id"]],
            "tags": envelope["tags"]
        }
        self.s3.put_object(Bucket=self.bucket, Key=manifest_key, Body=json.dumps(manifest, indent=2).encode("utf-8"), ContentType="application/json")
        # Init empty events
        self.s3.put_object(Bucket=self.bucket, Key=events_key, Body="{}".encode("utf-8"), ContentType="application/json")

        index_key = f"orgs/{self.org_id}/indexes/artifact-registry.jsonl"
        index_line = json.dumps(manifest) + "\n"
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=index_key)
            current = obj["Body"].read().decode("utf-8")
            self.s3.put_object(Bucket=self.bucket, Key=index_key, Body=(current + index_line).encode("utf-8"))
        except ClientError:
            self.s3.put_object(Bucket=self.bucket, Key=index_key, Body=index_line.encode("utf-8"))

        print(f"Run initialized: {run_id}, total_nodes: {run_payload[metrics][total_nodes]}")
        return envelope

if __name__ == "__main__":
    initializer = RunInitializer()
    dag_key = "orgs/org_agency/artifacts/dags/2026/03/dag_01JNZ9DAG1234567890AB/dag.json"
    dag = initializer.load_dag(dag_key)
    if dag:
        run = initializer.init_run(dag)
        print(f"Run from DAG: {run[artifact_id]}")
    else:
        print("DAG not found, skip run init")
