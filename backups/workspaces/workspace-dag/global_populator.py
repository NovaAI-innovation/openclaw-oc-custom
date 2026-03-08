import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

class GraphPopulator:
    def __init__(self, bucket="openclaw", account_id="554cefcc58bccd23ab124a9c06582d06", access_key="972d38bf8ca554615f49ed091b1b8851", secret_key="93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1" ):
        self.s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com")
        self.bucket = bucket
        self.org_id = "org_agency"
        self.graph_key = f"orgs/{self.org_id}/artifacts/graphs/global-property-graph.json"
        self.history_key = f"orgs/{self.org_id}/artifacts/graphs/global-property-graph.history.jsonl"

    def load_global_graph(self):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.graph_key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError:
            # Init empty graph
            now = datetime.now(timezone.utc).isoformat().replace(+00:00, Z)
            return {
                "schema_version": "1.0.0",
                "artifact_type": "global_property_graph",
                "artifact_id": "graph_global",
                "org_id": self.org_id,
                "project_id": None,
                "client_id": None,
                "domain_id": None,
                "created_at": now,
                "updated_at": now,
                "created_by": {"actor_type": "system", "actor_id": "global_graph_manager"},
                "source": {"source_type": "system", "source_ref": "graph-init"},
                "status": "active",
                "version": {"artifact_version": 1, "parent_artifact_id": "graph_global", "parent_artifact_version": 0, "content_hash": ""},
                "provenance": {"generated_from": [], "derived_artifacts": [], "artifact_refs": []},
                "visibility": {"scope": "org", "allowed_scopes": ["org"], "denied_scopes": []},
                "tags": ["global", "graph"],
                "graph": {"graph_type": "property_graph", "graph_manager_id": "global_graph_manager", "nodes": [], "edges": []}
            }

    def merge_dag(self, dag_envelope):
        global_graph = self.load_global_graph()
        now = datetime.now(timezone.utc).isoformat().replace(+00:00, Z)
        version = global_graph["version"]["artifact_version"] + 1
        global_graph["version"]["artifact_version"] = version
        global_graph["version"]["parent_artifact_version"] = global_graph["version"]["artifact_version"] - 1
        global_graph["updated_at"] = now
        global_graph["created_by"]["actor_id"] = "global_graph_manager"

        graph = global_graph["graph"]
        nodes = graph["nodes"]
        edges = graph["edges"]
        added_nodes = []
        added_edges = []

        # Add brief node if not exists (from provenance)
        brief_id = dag_envelope["provenance"]["generated_from"][0] if dag_envelope["provenance"]["generated_from"] else "brief_placeholder"
        brief_node = next((n for n in nodes if n["node_id"] == brief_id), None)
        if not brief_node:
            brief_node = {
                "node_id": brief_id,
                "node_type": "brief",
                "domain_id": self.domain_id,
                "properties": {"title": "Launch campaign for product X", "status": "approved_for_planning"}
            }
            nodes.append(brief_node)
            added_nodes.append(brief_node)

        # Add plan node
        plan_id = dag_envelope["dag"]["root_plan_id"]
        plan_node = next((n for n in nodes if n["node_id"] == plan_id), None)
        if not plan_node:
            plan_node = {
                "node_id": plan_id,
                "node_type": "plan",
                "domain_id": self.domain_id,
                "properties": {"status": "planned"}
            }
            nodes.append(plan_node)
            added_nodes.append(plan_node)

            # Edge brief -> plan
            edge_ulid = ulid.ULID()
            edge_id = f"edge_{str(edge_ulid)}"
            edge = {
                "edge_id": edge_id,
                "from_node_id": brief_id,
                "to_node_id": plan_id,
                "edge_type": "derived_into",
                "properties": {}
            }
            edges.append(edge)
            added_edges.append(edge)

        # Add DAG nodes (tasks)
        for node in dag_envelope["dag"]["nodes"]:
            task_node = next((n for n in nodes if n["node_id"] == node["node_id"]), None)
            if not task_node:
                task_node = {
                    "node_id": node["node_id"],
                    "node_type": "task",
                    "domain_id": node["domain_id"],
                    "properties": {
                        "title": node["label"],
                        "status": node["status"],
                        "required_capabilities": node["required_capabilities"]
                    }
                }
                nodes.append(task_node)
                added_nodes.append(task_node)

                # Edge plan -> task
                edge_ulid = ulid.ULID()
                edge_id = f"edge_{str(edge_ulid)}"
                edge = {
                    "edge_id": edge_id,
                    "from_node_id": plan_id,
                    "to_node_id": node["node_id"],
                    "edge_type": "contains",
                    "properties": {}
                }
                edges.append(edge)
                added_edges.append(edge)

                # Edges task -> capabilities
                for cap in node["required_capabilities"]:
                    cap_node = next((n for n in nodes if n["node_id"] == cap), None)
                    if not cap_node:
                        cap_node = {
                            "node_id": cap,
                            "node_type": "capability",
                            "domain_id": self.domain_id,
                            "properties": {"label": cap.replace("cap_", "Cap ")}
                        }
                        nodes.append(cap_node)
                        added_nodes.append(cap_node)

                    edge_ulid = ulid.ULID()
                    edge_id = f"edge_{str(edge_ulid)}"
                    edge = {
                        "edge_id": edge_id,
                        "from_node_id": node["node_id"],
                        "to_node_id": cap,
                        "edge_type": "requires_capability",
                        "properties": {}
                    }
                    edges.append(edge)
                    added_edges.append(edge)

        # Add DAG edges (depends_on)
        for edge in dag_envelope["dag"]["edges"]:
            dag_edge = next((e for e in edges if e["edge_id"] == edge["edge_id"]), None)
            if not dag_edge:
                dag_edge = {
                    "edge_id": edge["edge_id"],
                    "from_node_id": edge["from_node_id"],
                    "to_node_id": edge["to_node_id"],
                    "edge_type": edge["edge_type"],
                    "properties": edge["metadata"]
                }
                edges.append(dag_edge)
                added_edges.append(dag_edge)

        # Update hash
        payload_json = json.dumps(global_graph["graph"], sort_keys=True)
        hash_obj = hashlib.sha256(payload_json.encode())
        global_graph["version"]["content_hash"] = "sha256:" + hash_obj.hexdigest()

        # Save global graph
        self.s3.put_object(Bucket=self.bucket, Key=self.graph_key, Body=json.dumps(global_graph, indent=2).encode("utf-8"), ContentType="application/json")

        # Append to history
        history_line = {
            "version": version,
            "updated_at": now,
            "added_nodes": [n["node_id"] for n in added_nodes],
            "added_edges": [e["edge_id"] for e in added_edges],
            "source": "dag_merge",
            "dag_id": dag_envelope["artifact_id"]
        }
        index_line = json.dumps(history_line) + "\n"
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.history_key)
            current = obj["Body"].read().decode("utf-8")
            self.s3.put_object(Bucket=self.bucket, Key=self.history_key, Body=(current + index_line).encode("utf-8"))
        except ClientError:
            self.s3.put_object(Bucket=self.bucket, Key=self.history_key, Body=index_line.encode("utf-8"))

        print(f"Global graph merged: Version {version}, added {len(added_nodes)} nodes, {len(added_edges)} edges")
        return global_graph

if __name__ == "__main__":
    populator = GraphPopulator()
    dag_key = "orgs/org_agency/artifacts/dags/2026/03/dag_01JNZ9DAG1234567890AB/dag.json"  # Adjust
    try:
        obj = populator.s3.get_object(Bucket=populator.bucket, Key=dag_key)
        dag_envelope = json.loads(obj["Body"].read().decode("utf-8"))
        merged = populator.merge_dag(dag_envelope)
    except ClientError:
        print("DAG not found, skip merge")
