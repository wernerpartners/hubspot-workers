"""
One-time setup for the IAP pipeline on Claude Managed Agents. Run from the folder with pipeline.py:
  pip3 install anthropic
  export ANTHROPIC_API_KEY=sk-ant-...      # console key (creates the objects)
  export WORKER_ANTHROPIC_KEY=sk-ant-...   # key the worker uses for batches
  export HUBSPOT_TOKEN=pat-na2-...
  python3 setup_pipeline.py                # default: companies created in the last 24 hours
  RESEARCH_TAG="My tag" python3 setup_pipeline.py   # only companies with that Research Tag
  SCOPE=all python3 setup_pipeline.py      # SCOPE: all | <days> | list:<segment id>
Creates a deployment that runs hourly, paused, and fires one manual run. Unpause when happy.
"""
import os
from anthropic import Anthropic

client = Anthropic()
HERE = os.path.dirname(os.path.abspath(__file__))
SCOPE = os.getenv("SCOPE", "1")
RESEARCH_TAG = os.getenv("RESEARCH_TAG", "")
import time
RERUN_SINCE = os.getenv("RERUN_SINCE") or str(int(time.time() * 1000))   # companies matched after this are not redone
tag = (("tag-" + RESEARCH_TAG) if RESEARCH_TAG else SCOPE).replace(":", "-").replace(" ", "-").lower()

f = client.beta.files.upload(file=open(os.path.join(HERE, "pipeline.py"), "rb"), betas=["files-api-2025-04-14"])
print("file:", f.id)

env = client.beta.environments.create(
    name=f"hubspot-iap-pipeline-env",
    config={"type": "cloud", "packages": {"pip": ["anthropic", "requests", "beautifulsoup4", "phonenumbers"]},
            "networking": {"type": "unrestricted"}})
print("environment:", env.id)

vault = client.beta.vaults.create(display_name=f"hubspot-iap-pipeline-vault-{tag}")
for name, value, hosts in [("HUBSPOT_TOKEN", os.environ["HUBSPOT_TOKEN"], ["api.hubapi.com"]),
                           ("ANTHROPIC_API_KEY", os.environ["WORKER_ANTHROPIC_KEY"], ["api.anthropic.com"])]:
    client.beta.vaults.credentials.create(vault.id, display_name=name,
        auth={"type": "environment_variable", "secret_name": name, "secret_value": value,
              "networking": {"type": "limited", "allowed_hosts": hosts}})
print("vault:", vault.id)

cmd = (f"cd /mnt/session/uploads && SCOPE={SCOPE} RESEARCH_TAG='{RESEARCH_TAG}' RERUN_SINCE={RERUN_SINCE} STAGE2_MAX={os.getenv('STAGE2_MAX', '500')} "
       f"STAGE1_BATCH={os.getenv('STAGE1_BATCH', '5000')} nohup python pipeline.py > /tmp/pipeline.log 2>&1 & echo started")
agent = client.beta.agents.create(
    name=f"hubspot-iap-pipeline-runner-{tag}",
    model="claude-haiku-5-5",
    system=("You run a scheduled job and must not stop until it has finished. "
            f"Step 1: execute exactly: {cmd} "
            "Step 2: run: sleep 120; tail -n 3 /tmp/pipeline.log; pgrep -f pipeline.py || echo FINISHED "
            "Step 3: repeat step 2 until the output contains FINISHED. "
            "Then reply with the last 10 lines of /tmp/pipeline.log and stop. "
            "Do not read, edit, or rewrite pipeline.py. Do not install anything."),
    tools=[{"type": "agent_toolset_20260401"}])
print("agent:", agent.id)

depl = client.beta.deployments.create(
    name=f"hubspot-iap-pipeline-{tag}",
    agent=agent.id, environment_id=env.id, vault_ids=[vault.id],
    resources=[{"type": "file", "file_id": f.id, "mount_path": "/pipeline.py"}],
    initial_events=[{"type": "user.message", "content": [{"type": "text", "text": "Run the pipeline."}]}],
    schedule={"type": "cron", "expression": "0 * * * *", "timezone": "UTC"},
    budget={"type": "limit", "max_list_cost": {"amount": "100", "currency": "USD"}})
print("deployment:", depl.id)
client.beta.deployments.pause(depl.id)
run = client.beta.deployments.run(depl.id)
print("test run started, session:", run.session_id)
print("Watch: https://platform.claude.com/workspaces/default/deployments  — unpause there when happy.")
