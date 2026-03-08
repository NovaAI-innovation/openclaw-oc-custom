import json
import ulid
import hashlib
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

class ArtifactProcessor:
    def __init__(self, bucket="openclaw", account_id="554cefcc58bccd23ab124a9c06582d06", access_key="972d38bf8ca554615f49ed091b1b8851", secret_key="93136fb2f17da2aaa032463a48302ec983f64f5581f4773701560ee039de38a1" ):
        self.s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com")
        self.bucket = bucket
        self.org_id = "org_agency"
        self.project_id = "project_01JNZ8ZZZZZZZZZZZZZZZZZZ"
        self.client_id = "client_01JNZ8YYYYYYYYYYYYYYYYYY"
        self.domain_id = "domain_marketing_advertising"

    def load_brief(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ClientError as e:
            print(f"Error loading brief: {e}")
            return None

    def generate_plan(self, brief):
        now = datetime.utcnow().isoformat() + "Z"
        plan_ulid = ulid.ULID()
        plan_id = f"plan_{plan_ulid}"

        # Sample tasks from blueprint example
        tasks = [
            {
                "task_id": f"task_{ulid.ULID()}",
                "title": "Define campaign positioning",
                "description": "Create initial positioning and messaging structure.",
                "task_type": "analysis",
                "domain_id": self.domain_id,
                "required_capabilities": ["cap_campaign_strategy", "cap_positioning"],
                "priority": "high",
                "status": "pending",
                "dependency_ids": [],
                "blocking_dependency_ids": [],
                "optional_dependency_ids": [],
                "input_artifact_refs": [],
                "output_artifacts_expected": [{"artifact_type": "positioning_doc", "name": "campaign-positioning"}],
                "acceptance_criteria": ["Includes audience framing", "Includes positioning statement", "Includes core message themes"],
                "assignment": {"owner_type": "unassigned", "owner_id": None},
                "execution_policy": {"retry_limit": 1, "requires_approval_before_complete": True},
                "metadata": {}
            },
            {
                "task_id": f"task_{ulid.ULID()}",
                "title": "Recommend channels and sequencing",
                "description": "Determine channel mix and execution order.",
                "task_type": "planning",
                "domain_id": self.domain_id,
                "required_capabilities": ["cap_channel_planning"],
                "priority": "high",
                "status": "pending",
                "dependency_ids": [tasks[0]["task_id"]],  # Dep on first task
                "blocking_dependency_ids": [tasks[0]["task_id"]],
                "optional_dependency_ids": [],
                "input_artifact_refs": [],
                "output_artifacts_expected": [{"artifact_type": "channel_plan", "name": "channel-sequencing"}],
                "acceptance_criteria": ["Includes prioritized channels", "Includes rationale", "Includes sequence recommendation"],
                "assignment": {"owner_type": "unassigned", "owner_id": None},
                "execution_policy": {"retry_limit": 1, "requires_approval_before_complete": False},
                "metadata": {}
            }
        ]

        plan_payload = {
            "title": f"Execution plan for {brief[brief][title]}",
            "planning_status": "approved",
            "plan_summary": "Normalized task plan for campaign strategy and launch preparation.",
            "routing_hints": {"primary_domain": self.domain_id, "candidate_future_domains": ["design_brand", "web_development"], "cross_domain_potential": True},
            "required_capabilities": ["cap_campaign_strategy", "cap_channel_planning", "cap_asset_planning"],
            "tasks": tasks,
            "deliverables": [{"deliverable_id": f"deliverable_{ulid.ULID()}", "type": "strategy_brief", "title": "Campaign strategy brief", "source_task_ids": [t["task_id"] for t in tasks]}],
            "risks": [{"risk_id": f"risk_{ulid.ULID()}", "description": "Insufficient product information may weaken positioning quality.", "severity": "medium"}],
            "assumptions": ["Input materials are sufficient for first-pass strategy."]
        }

        envelope = {
            "schema_version": "1.0.0",
            "artifact_type": "plan",
            "artifact_id": plan_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "created_at": now,
            "updated_at": now,
            "created_by": {"actor_type": "agent", "actor_id": "brief_dag_integrator"},
            "source": {"source_type": "artifact", "source_ref": brief["artifact_id"]},
            "status": "planned",
            "version": {"artifact_version": 1, "parent_artifact_id": brief["artifact_id"], "parent_artifact_version": brief["version"]["artifact_version"], "content_hash": ""},
            "provenance": {"generated_from": [brief["artifact_id"]], "derived_artifacts": [], "artifact_refs": []},
            "visibility": {"scope": "project", "allowed_scopes": ["org", f"domain:{self.domain_id}", f"project:{self.project_id}"], "denied_scopes": []},
            "tags": ["plan", "marketing"],
            "plan": plan_payload
        }

        payload_json = json.dumps(envelope["plan"], sort_keys=True)
        hash_obj = hashlib.sha256(payload_json.encode())
        content_hash = "sha256:" + hash_obj.hexdigest()
        envelope["version"]["content_hash"] = content_hash

        year = now[:4]
        month = now[5:7]
        artifact_key = f"orgs/{self.org_id}/artifacts/plans/{year}/{month}/{plan_id}/plan.json"
        manifest_key = f"orgs/{self.org_id}/artifacts/plans/{year}/{month}/{plan_id}/manifest.json"

        # Upload
        self.s3.put_object(Bucket=self.bucket, Key=artifact_key, Body=json.dumps(envelope, indent=2).encode("utf-8"), ContentType="application/json")
        manifest = {
            "artifact_id": plan_id,
            "artifact_type": "plan",
            "schema_version": "1.0.0",
            "org_id": self.org_id,
            "project_id": self.project_id,
            "client_id": self.client_id,
            "domain_id": self.domain_id,
            "artifact_version": 1,
            "status": "planned",
            "created_at": now,
            "updated_at": now,
            "title": envelope["plan"]["title"],
            "path_hint": artifact_key,
            "content_hash": content_hash,
            "derived_from": [brief["artifact_id"]],
            "tags": envelope["tags"]
        }
        self.s3.put_object(Bucket=self.bucket, Key=manifest_key, Body=json.dumps(manifest, indent=2).encode("utf-8"), ContentType="application/json")

        # Append to index
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
    processor = ArtifactProcessor()
    brief_key = "orgs/org_agency/artifacts/briefs/2026/03/brief_01JNZ9A0Q2V3X4Y5Z6A7B8C9D/brief.json"
    brief = processor.load_brief(brief_key)
    if brief:
        plan = processor.generate_plan(brief)
        print(f"Plan generated: {plan[artifact_id]}")
    else:
        print("Failed to load brief")
