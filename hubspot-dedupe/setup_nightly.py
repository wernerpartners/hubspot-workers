"""
One-time setup: nightly HubSpot dedupe on Claude Managed Agents, every day at 8 p.m. Costa Rica time.
Runs dedupe_companies.py, then z_strip_and_merge.py (strip new z prefixes + merge contacts), no merge limit.
Run from ~/Documents/WP/hubspot-dedupe:
  pip3 install anthropic
  export ANTHROPIC_API_KEY=sk-ant-...      # console key (creates the objects)
  export HUBSPOT_TOKEN=pat-na2-...
  python3 setup_nightly.py
The deployment is created PAUSED. Unpause it at https://platform.claude.com/workspaces/default/deployments
"""
import os
from anthropic import Anthropic

client = Anthropic()
HERE = os.path.dirname(os.path.abspath(__file__))
FILES = ["common.py", "dedupe_companies.py", "z_strip_and_merge.py", "nightly.py"]

uploads = []
for name in FILES:
    f = client.beta.files.upload(file=open(os.path.join(HERE, name), "rb"), betas=["files-api-2025-04-14"])
    uploads.append({"type": "file", "file_id": f.id, "mount_path": f"/{name}"})
    print("file:", name, f.id)

env = client.beta.environments.create(
    name="hubspot-dedupe-nightly-env",
    config={"type": "cloud", "packages": {"pip": ["requests"]}, "networking": {"type": "unrestricted"}})
print("environment:", env.id)

vault = client.beta.vaults.create(display_name="hubspot-dedupe-nightly-vault")
client.beta.vaults.credentials.create(vault.id, display_name="HUBSPOT_TOKEN",
    auth={"type": "environment_variable", "secret_name": "HUBSPOT_TOKEN", "secret_value": os.environ["HUBSPOT_TOKEN"],
          "networking": {"type": "limited", "allowed_hosts": ["api.hubapi.com"]}})
print("vault:", vault.id)

cmd = "cd /mnt/session/uploads && nohup python nightly.py > /tmp/nightly.log 2>&1 & echo started"
agent = client.beta.agents.create(
    name="hubspot-dedupe-nightly-runner",
    model="claude-haiku-5-5",
    system=("You run a scheduled job and must not stop until it has finished. "
            f"Step 1: execute exactly: {cmd} "
            "Step 2: run: sleep 300; tail -n 3 /tmp/nightly.log; pgrep -f nightly.py || echo FINISHED "
            "Step 3: repeat step 2 until the output contains FINISHED. "
            "Then reply with every line of /tmp/nightly.log that contains ===, merged, would merge, groups, "
            "z contacts or failed, and stop. Do not read, edit, or rewrite any .py file. Do not install anything."),
    tools=[{"type": "agent_toolset_20260401"}])
print("agent:", agent.id)

depl = client.beta.deployments.create(
    name="hubspot-dedupe-nightly",
    agent=agent.id, environment_id=env.id, vault_ids=[vault.id], resources=uploads,
    initial_events=[{"type": "user.message", "content": [{"type": "text", "text": "Run the nightly dedupe."}]}],
    schedule={"type": "cron", "expression": "0 20 * * *", "timezone": "America/Costa_Rica"},
    budget={"type": "limit", "max_list_cost": {"amount": "20", "currency": "USD"}})
print("deployment:", depl.id)
client.beta.deployments.pause(depl.id)
print("Created paused. Unpause at https://platform.claude.com/workspaces/default/deployments when the z strip is done.")
