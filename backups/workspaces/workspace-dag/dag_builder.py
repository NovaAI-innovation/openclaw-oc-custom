import json
import ulid
import hashlib
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
import networkx as nx

class DAGBuilder:
    def __init__(self, bucket="openclaw", account_id="554cefcc58bccd23ab124a9c06582d06", access_key="972d38bf8ca554615f49ed091b1b8851", secret_key="93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1" ):
        self.s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com")
        self.bucket = bucket
        self.org_id = "org_agency"
        self.project_id = "project_01JNZ8ZZZZZZZZZZZZZZZZZZ"
        self.client_id = "client_01JNZ8YYYYYYYYYYYYYYYYYY"
        self.domain_id = "domain_marketing_advertising"

    def load_plan(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            print(f"Error loading plan: {e}")
            return None

    def generate_dag(self, plan):
        now = datetime.now(timezone.utc).isoformat().replace(+00:00, Z)
        dag_ulid = ulid.ULID()
        dag_id = f"dag_{str(dag_ulid)}"

        dag = nx.DiGraph()
        nodes = []
        edges = []
        task_to_node = {}  # Map task_id to node_id

        for task in plan["plan"]["tasks"]:
            node_ulid = ulid.ULID()
            node_id = f"node_{str(node_ulid)}"
            task_to_node[task["task_id"]] = node_id
            node = {
                "node_id": node_id,
                "node_type": "task",
                "ref_id": task["task_id"],
                "domain_id": task["domain_id"],
                "label": task["title"],
                "status": task["status"],
                "priority": task["priority"],
                "required_capabilities": task["required_capabilities"],
                "input_artifact_refs": task["input_artifact_refs"],
                "output_artifact_specs": task["output_artifacts_expected"],
                "metadata": task["metadata"]
            }
            nodes.append(node)
            dag.add_node(node_id, **{k: v for k, v in node.items() if k != "node_id"})

            for dep_id in task["blocking_dependency_ids"]:
                if dep_id in task_to_node:
                    edge_ulid = ulid.ULID()
                    edge_id = f"edge_{str(edge_ulid)}"
                    edge = {
                        "edge_id": edge_id,
                        "from_node_id": task_to_node[dep_id],
                        "to_node_id": node_id,
                        "edge_type": "depends_on",
                        "required": True,
                        "metadata": {}
                    }
                    edges.append(edge)
                    dag.add_edge(task_to_node[dep_id], node_id)

        is_acyclic = nx.is_directed_acyclic_graph(dag)
        topo_order = list(nx.topological_sort(dag)) if is_acyclic else []
        validation = {"is_acyclic": is_acyclic, "validated_at": now, "validation_errors": [] if is_acyclic else ["Cycle detected"]}

        dag_payload = {
            "dag_type": "execution",
            "dag_status": "validated" if is_acyclic else "invalid",
            "root_plan_id": plan["artifact_id"],
            "graph_manager_id": "global_graph_manager",
            "validation": validation,
            "nodes": nodes,
            "edges": edges
        }

        envelope = {
            "schema_version": "1.0.0",
            "artifact_type": "dag",
            "artifact_id": dag_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "created_at": now,
            "updated_at": now,
            "created_by": {"actor_type": "agent", "actor_id": "graph_manager"},
            "source": {"source_type": "artifact", "source_ref": plan["artifact_id"]},
            "status": "ready" if is_acyclic else "invalid",
            "version": {"artifact_version": 1, "parent_artifact_id": plan["artifact_id"], "parent_artifact_version": plan["version"]["artifact_version"], "content_hash": ""},
            "provenance": {"generated_from": [plan["artifact_id"]], "derived_artifacts": [], "artifact_refs": []},
            "visibility": {"scope": "project", "allowed_scopes": ["org", f"domain:{self.domain_id}", f"project:{self.project_id}"], "denied_scopes": []},
            "tags": ["dag", "execution"],
            "dag": dag_payload
        }

        payload_json = json.dumps(envelope["dag"], sort_keys=True)
        hash_obj = hashlib.sha256(payload_json.encode())
        content_hash = "sha256:" + hash_obj.hexdigest()
        envelope["version"]["content_hash"] = content_hash

        year = now[:4]
        month = now[5:7]
        artifact_key = f"orgs/{self.org_id}/artifacts/dags/{year}/{month}/{dag_id}/dag.json"
        manifest_key = f"orgs/{self.org_id}/artifacts/dags/{year}/{month}/{dag_id}/manifest.json"

        self.s3.put_object(Bucket=self.bucket, Key=artifact_key, Body=json.dumps(envelope, indent=2).encode("utf-8"), ContentType="application/json")
        manifest = {
            "artifact_id": dag_id,
            "artifact_type": "dag",
            "schema_version": "1.0.0",
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "artifact_version": 1,
            "status": "ready" if is_acyclic else "invalid",
            "created_at": now,
            "updated_at": now,
            "title": f"DAG for {plan[plan][title]}",
            "path_hint": artifact_key,
            "content_hash": content_hash,
            "derived_from": [plan["artifact_id"]],
            "tags": envelope["tags"]
        }
        self.s3.put_object(Bucket=self.bucket, Key=manifest_key, Body=json.dumps(manifest, indent=2).encode("utf-8"), ContentType="application/json")

        index_key = f"orgs/{self.org_id}/indexes/artifact-registry.jsonl"
        index_line = json.dumps(manifest) + "\n"
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=index_key)
            current = obj["Body"].read().decode("utf-8")
            self.s3.put_object(Bucket=self.bucket, Key=index_key, Body=(current + index_line).encode("utf-8"))
        except ClientError:
            self.s3.put_object(Bucket=self.bucket, Key=index_key, Body=index_line.encode("utf-8"))

        return envelope

if __name__ == "__main__":
    builder = DAGBuilder()
    plan_key = "orgs/org_agency/artifacts/plans/2026/03/plan_01JNZ9PLAN1234567890AB/plan.json"
    plan = builder.load_plan(plan_key)
    if plan:
        dag = builder.generate_dag(plan)
        print(f"DAG from plan: {dag[artifact_id]}, nodes: {len(dag[dag][nodes])}, edges: {len(dag[dag][edges])}, acyclic: {dag[dag][validation][is_acyclic]}")
    else:
        print("Load failed, use hardcoded blueprint plan")
        sample_plan = {
            "artifact_id": "plan_01JNZ9PLAN1234567890AB",
            "plan": {
                "title": "Marketing launch execution plan",
                "tasks": [
                    {
                        "task_id": "task_01JNZ9TASK1",
                        "title": "Define campaign positioning",
                        "description": "Create initial positioning and messaging structure.",
                        "task_type": "analysis",
                        "domain_id": "domain_marketing_advertising",
                        "required_capabilities": ["cap_campaign_strategy", "cap_positioning"],
                        "priority": "high",
                        "status": "pending",
                        "blocking_dependency_ids": [],
                        "input_artifact_refs": [],
                        "output_artifacts_expected": [{"artifact_type": "positioning_doc", "name": "campaign-positioning"}],
                        "metadata": {}
                    },
                    {
                        "task_id": "task_01JNZ9TASK2",
                        "title": "Recommend channels and sequencing",
                        "description": "Determine channel mix and execution order.",
                        "task_type": "planning",
                        "domain_id": "domain_marketing_advertising",
                        "required_capabilities": ["cap_channel_planning"],
                        "priority": "high",
                        "status": "pending",
                        "blocking_dependency_ids": ["task_01JNZ9TASK1"],
                        "input_artifact_refs": [],
                        "output_artifacts_expected": [{"artifact_type": "channel_plan", "name": "channel-sequencing"}],
                        "metadata": {}
                    }
                ]
            }
        }
        dag = builder.generate_dag(sample_plan)
        print(f"Hardcoded DAG: {dag[artifact_id]}, nodes: {len(dag[dag][nodes])}, edges: {len(dag[dag][edges])}, acyclic: {dag[dag][validation][is_acyclic]}")
