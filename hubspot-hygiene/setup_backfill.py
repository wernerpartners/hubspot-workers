"""
One-off run of backfill_activity_dates.py on Claude Managed Agents. From your laptop:
  export ANTHROPIC_API_KEY=sk-ant-...   # console key
  export HUBSPOT_TOKEN=...              # HubSpot service key
  DRY_RUN=true  python setup_backfill.py   # first: counts only, writes nothing
  DRY_RUN=false python setup_backfill.py   # then: the real write
Each call creates its own paused deployment and fires exactly one run. Archive them afterwards.
"""
import os
from anthropic import Anthropic

client = Anthropic()
HERE = os.path.dirname(os.path.abspath(__file__))
DRY = os.getenv("DRY_RUN", "true").lower() == "true"
tag = "dry" if DRY else "write"

f = client.beta.files.upload(file=open(os.path.join(HERE, "backfill_activity_dates.py"), "rb"),
                             betas=["files-api-2025-04-14"])
print("file:", f.id)

env = client.beta.environments.create(
    name=f"hubspot-activity-backfill-env-{tag}",
    config={"type": "cloud", "packages": {"pip": ["requests"]},
            "networking": {"type": "unrestricted"}})
print("environment:", env.id)

vault = client.beta.vaults.create(display_name=f"hubspot-activity-backfill-vault-{tag}")
client.beta.vaults.credentials.create(
    vault.id, display_name="HUBSPOT_TOKEN",
    auth={"type": "environment_variable", "secret_name": "HUBSPOT_TOKEN",
          "secret_value": os.environ["HUBSPOT_TOKEN"],
          "networking": {"type": "limited", "allowed_hosts": ["api.hubapi.com"]}})
print("vault:", vault.id)

# Shell commands are capped at ~5 minutes, and the script runs for hours, so it is
# started in the background with its output in a log file, then polled until done.
start = (f"cd /mnt/session/uploads && nohup env DRY_RUN={'true' if DRY else 'false'} "
         "python backfill_activity_dates.py > /tmp/backfill.log 2>&1 & echo started")
agent = client.beta.agents.create(
    name=f"hubspot-activity-backfill-{tag}",
    model="claude-haiku-5-5",
    system=("You run a one-off job and must not stop until it has finished. "
            f"Step 1: execute exactly: {start} "
            "Step 2: every 4 minutes run: sleep 240; tail -n 3 /tmp/backfill.log; "
            "pgrep -f backfill_activity_dates.py || echo FINISHED "
            "Step 3: repeat step 2 until the output contains FINISHED, however many times that takes. "
            "Then reply with the last 15 lines of /tmp/backfill.log and stop. "
            "Do not read, edit, or rewrite the script. Do not install anything."),
    tools=[{"type": "agent_toolset_20260401"}])
print("agent:", agent.id)

depl = client.beta.deployments.create(
    name=f"hubspot-activity-backfill-{tag}",
    agent=agent.id,
    environment_id=env.id,
    vault_ids=[vault.id],
    resources=[{"type": "file", "file_id": f.id, "mount_path": "/backfill_activity_dates.py"}],
    initial_events=[{"type": "user.message",
                     "content": [{"type": "text", "text": "Run the backfill."}]}],
    schedule={"type": "cron", "expression": "0 0 1 1 *", "timezone": "UTC"},
    budget={"type": "limit", "max_list_cost": {"amount": "20", "currency": "USD"}})
print("deployment:", depl.id)

client.beta.deployments.pause(depl.id)
run = client.beta.deployments.run(depl.id)
print("run started, session:", run.session_id)
print("Watch it: https://platform.claude.com/sessions")
