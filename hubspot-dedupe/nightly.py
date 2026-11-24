"""Nightly dedupe: companies first, then contacts (strip new z prefixes + merge). No merge limit.
Run by the Managed Agents deployment from setup_nightly.py. NIGHTLY_DRY_RUN=true reports without changing HubSpot."""
import os, subprocess, sys, time

dry = os.getenv("NIGHTLY_DRY_RUN", "false").lower()
env = dict(os.environ, DRY_RUN=dry, MAX_MERGES="1000000")
for name, script, extra in [("companies", "dedupe_companies.py", {}), ("contacts", "z_strip_and_merge.py", {"STEP": "all"})]:
    print(time.strftime("%H:%M:%S"), f"=== {name} dedupe start (DRY_RUN={dry})", flush=True)
    rc = subprocess.call([sys.executable, script], env=dict(env, **extra))
    print(time.strftime("%H:%M:%S"), f"=== {name} dedupe finished, exit code {rc}", flush=True)
print(time.strftime("%H:%M:%S"), "nightly done", flush=True)
