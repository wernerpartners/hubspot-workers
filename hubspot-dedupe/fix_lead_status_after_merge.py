"""Restore Lead Status lost in the company merges of Nov 3-4.

For each survivor listed in merged_companies.csv:
  - our merges = changes of hs_merged_object_ids since SINCE (default 2026-11-03)
  - skip if Lead Status changed after the last of those merges (2 minute grace for the merge itself)
  - previous status = the most recent Lead Status set before the merge that differs from today's value
  - restore it only if it ranks higher than today's value:
      Public Pool < Campaign Queue < New < Working < Closed Lead < Open Deal < Closed Deal
  - confidence "certain": that value is also the latest one set before the merge, so it can only have
    come from the merged-away company (the survivor's own latest value is today's value).
    confidence "check": the history can't tell which of the two companies it came from.
Writes lead_status_fix.csv.
DRY_RUN=true (default) writes nothing to HubSpot. DRY_RUN=false restores the "certain" rows;
add INCLUDE_CHECK=true to restore the "check" rows too.
Run from ~/Documents/WP/hubspot-dedupe (uses common.py). Env: HUBSPOT_TOKEN
"""
import csv, os, time
from datetime import datetime
from common import hs, log, write_csv

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
INCLUDE_CHECK = os.getenv("INCLUDE_CHECK", "false").lower() == "true"
SINCE = os.getenv("SINCE", "2026-11-03T00:00:00Z")
AFTER_GRACE_MS = 120_000      # a Lead Status entry within 2 minutes after the merge is the merge itself
BEFORE_GAP_MS = 60_000        # ignore entries in the minute before the merge
ORDER = ["", "public pool", "campaign queue", "new", "working", "closed lead", "open_deal", "closed deal"]

def ms(ts):
    try: return int(datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return 0

def rank(v):
    v = (v or "").strip().lower()
    return ORDER.index(v) if v in ORDER else -1

since_ms = ms(SINCE)
ids = list(dict.fromkeys(r["survivor_id"] for r in csv.DictReader(open("merged_companies.csv"))))
log(f"DRY_RUN: {DRY_RUN} | INCLUDE_CHECK: {INCLUDE_CHECK} | SINCE: {SINCE} | survivors: {len(ids):,}")

rows, updates = [], []
for n, cid in enumerate(ids, 1):
    if n % 200 == 0: log(f"{n:,} checked")
    time.sleep(0.11)
    r = hs("GET", f"/crm/v3/objects/companies/{cid}",
           params={"properties": "name,hs_lead_status", "propertiesWithHistory": "hs_lead_status,hs_merged_object_ids"})
    p = r.get("properties") or {}
    hist = r.get("propertiesWithHistory") or {}
    current = (p.get("hs_lead_status") or "").strip()
    link = f"https://app-na2.hubspot.com/contacts/243636394/record/0-2/{cid}"
    def add(action, previous="", confidence=""):
        rows.append([cid, p.get("name"), current, previous, confidence, action, link])

    merges = sorted(t for t in (ms(v.get("timestamp")) for v in hist.get("hs_merged_object_ids", [])) if t >= since_ms)
    if not merges: add("skip: no merge since " + SINCE); continue
    first, last = merges[0], merges[-1]
    ls = sorted(((ms(v.get("timestamp")), (v.get("value") or "").strip()) for v in hist.get("hs_lead_status", [])), reverse=True)
    if any(t > last + AFTER_GRACE_MS for t, _ in ls): add("skip: changed after merge"); continue
    pre = [(t, v) for t, v in ls if t < first - BEFORE_GAP_MS]
    prev = next((v for t, v in pre if v != current), "")
    if not prev: add("skip: no different status before merge"); continue
    if rank(prev) < 0: add("skip: unknown value", prev); continue
    confidence = "certain" if pre[0][1] == prev else "check"
    if rank(prev) <= rank(current): add("skip: not higher", prev, confidence); continue
    write = confidence == "certain" or INCLUDE_CHECK
    add("restore" if write else "restore after check", prev, confidence)
    if write: updates.append({"id": cid, "properties": {"hs_lead_status": prev}})

write_csv("lead_status_fix.csv", rows, ["company_id", "name", "lead_status_now", "lead_status_before_merge",
                                        "confidence", "action", "link"])
certain = sum(1 for x in rows if x[4] == "certain" and x[5].startswith("restore"))
check = sum(1 for x in rows if x[4] == "check" and x[5].startswith("restore"))
log(f"checked {len(rows):,} | restore certain: {certain:,} | restore check: {check:,} | "
    f"skipped changed after merge: {sum(1 for x in rows if x[5] == 'skip: changed after merge'):,}")

if DRY_RUN:
    log("DRY_RUN - nothing written. Review lead_status_fix.csv, then rerun with DRY_RUN=false")
else:
    done = 0
    for k in range(0, len(updates), 100):
        chunk = updates[k:k + 100]
        try:
            hs("POST", "/crm/v3/objects/companies/batch/update", json={"inputs": chunk}); done += len(chunk)
        except Exception as e:
            log("batch failed, retrying one by one:", str(e)[:200])
            for u in chunk:
                try: hs("PATCH", f"/crm/v3/objects/companies/{u['id']}", json={"properties": u["properties"]}); done += 1
                except Exception as e2: log("failed", u["id"], str(e2)[:200])
    log(f"Lead Status restored on {done:,} companies. done")
