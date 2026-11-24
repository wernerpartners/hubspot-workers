"""
One-time setup for Claude Managed Agents. Run from your laptop:
  pip install anthropic
  export ANTHROPIC_API_KEY=sk-ant-...      # your console key (creates the objects)
  export WORKER_ANTHROPIC_KEY=sk-ant-...   # key the worker uses for batches (can be the same)
  export HUBSPOT_TOKEN=...                 # HubSpot service key
  python setup_managed_agent.py
"""
import os
from anthropic import Anthropic

client = Anthropic()
HERE = os.path.dirname(os.path.abspath(__file__))

# 1. upload worker.py
f = client.beta.files.upload(file=open(os.path.join(HERE, "worker.py"), "rb"),
                             betas=["files-api-2025-04-14"])
print("file:", f.id)

# 2. environment: packages preinstalled, open network so it can crawl any website
env = client.beta.environments.create(
    name="hubspot-enrich-env",
    config={"type": "cloud",
            "packages": {"pip": ["anthropic", "requests", "beautifulsoup4"]},
            "networking": {"type": "unrestricted"}})
print("environment:", env.id)

# 3. vault: two secrets, each only substituted on its own API host
vault = client.beta.vaults.create(display_name="hubspot-enrich-vault")
for name, value, hosts in [
    ("HUBSPOT_TOKEN", os.environ["HUBSPOT_TOKEN"], ["api.hubapi.com"]),
    ("ANTHROPIC_API_KEY", os.environ["WORKER_ANTHROPIC_KEY"], ["api.anthropic.com"]),
]:
    client.beta.vaults.credentials.create(
        vault.id, display_name=name,
        auth={"type": "environment_variable", "secret_name": name, "secret_value": value,
              "networking": {"type": "limited", "allowed_hosts": hosts}})
print("vault:", vault.id)

# 4. agent: cheapest model, one job
agent = client.beta.agents.create(
    name="hubspot-enrich-runner",
    model="claude-haiku-5-5",
    system=("You run a scheduled job. Execute exactly: cd /mnt/session/uploads && python worker.py "
            "Do not read, edit, or rewrite worker.py. Do not install anything. "
            "When it finishes, reply with the last 5 lines of its output and stop."),
    tools=[{"type": "agent_toolset_20260401"}])
print("agent:", agent.id)

# 5. deployment: every 15 min, $2 cap per run on the runner (batch spend lands on your API bill)
depl = client.beta.deployments.create(
    name="hubspot-enrich-worker",
    agent=agent.id,
    environment_id=env.id,
    vault_ids=[vault.id],
    resources=[{"type": "file", "file_id": f.id, "mount_path": "/worker.py"}],
    initial_events=[{"type": "user.message",
                     "content": [{"type": "text", "text": "Run the worker."}]}],
    schedule={"type": "cron", "expression": "*/15 * * * *", "timezone": "UTC"},
    budget={"type": "limit", "max_list_cost": {"amount": "200", "currency": "USD"}})
print("deployment:", depl.id)

# 6. pause the schedule, fire one manual test run
client.beta.deployments.pause(depl.id)
run = client.beta.deployments.run(depl.id)
print("test run started, session:", run.session_id)
print("Watch it: https://platform.claude.com/workspaces/default/deployments")
print("When happy, unpause it there or run: client.beta.deployments.unpause('%s')" % depl.id)
