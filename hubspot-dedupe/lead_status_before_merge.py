"""For every survivor in merged_companies.csv, read the Lead Status history and report the status
it had before the merge. HubSpot combines the property history of both records on merge, so values
that came from the merged-away company show up here too.
Writes lead_status_before_merge.csv with only the companies whose pre-merge status was
something other than Public Pool or Closed Lead (any value, from the survivor or the merged record).
Run from ~/Documents/WP/hubspot-dedupe. Env: HUBSPOT_TOKEN
"""
import csv, time
from datetime import datetime
from common import hs, log, write_csv

SKIP = {"public pool", "closed lead", ""}

def ms(ts):
    try: return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return 0

first_merge = {}
for r in csv.DictReader(open("merged_companies.csv")):
    t = ms(r["merge_time"])
    if r["survivor_id"] not in first_merge or t < first_merge[r["survivor_id"]][0]:
        first_merge[r["survivor_id"]] = (t, r["merge_time"], r["survivor_name"])
log(f"survivors to check: {len(first_merge):,}")

rows, n = [], 0
for cid, (mt, mts, name) in first_merge.items():
    n += 1
    h = hs("GET", f"/crm/v3/objects/companies/{cid}",
           params={"properties": "hs_lead_status", "propertiesWithHistory": "hs_lead_status"})
    current = (h.get("properties") or {}).get("hs_lead_status") or ""
    hist = (h.get("propertiesWithHistory") or {}).get("hs_lead_status", [])
    before = [v for v in hist if ms(v.get("timestamp", "")) < mt]          # newest first
    last_before = (before[0].get("value") or "") if before else ""
    values = []
    for v in before:
        val = (v.get("value") or "").strip()
        if val and val not in values: values.append(val)
    other = [v for v in values if v.lower() not in SKIP]
    if other or last_before.lower() not in SKIP:
        rows.append([cid, name, current, last_before, " | ".join(other), " | ".join(values), mts,
                     f"https://app-na2.hubspot.com/contacts/243636394/record/0-2/{cid}"])
    if n % 200 == 0: log(f"{n:,} checked")
    time.sleep(0.11)

write_csv("lead_status_before_merge.csv", rows,
          ["survivor_id", "name", "lead_status_now", "lead_status_just_before_merge",
           "other_statuses_before_merge", "all_statuses_before_merge", "first_merge_time", "link"])
changed = sum(1 for r in rows if r[2] != r[3])
log(f"survivors checked: {n:,} | had a non Public Pool / non Closed Lead status before merge: {len(rows):,} | status now different from just before merge: {changed:,}")
