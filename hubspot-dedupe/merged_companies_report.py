"""Count companies merged recently, from the property history of hs_merged_object_ids.
Run from ~/Documents/WP/hubspot-dedupe (uses common.py). Writes merged_companies.csv.
Env: HUBSPOT_TOKEN, DAYS (default 2)
"""
import os, time
from datetime import datetime, timezone, timedelta
from common import hs, log, write_csv

DAYS = float(os.getenv("DAYS", "2"))
since = datetime.now(timezone.utc) - timedelta(days=DAYS)
since_ms = int(since.timestamp() * 1000)

def merged_survivors():
    body = {"filterGroups": [{"filters": [{"propertyName": "hs_merged_object_ids", "operator": "HAS_PROPERTY"}]}],
            "properties": ["name", "hs_lead_status"], "limit": 200, "sorts": ["hs_object_id"]}
    after = None
    while True:
        if after: body["after"] = after
        res = hs("POST", "/crm/v3/objects/companies/search", json=body)
        for r in res.get("results", []): yield r
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break

rows, n = [], 0
for r in merged_survivors():
    n += 1
    cid = r["id"]
    h = hs("GET", f"/crm/v3/objects/companies/{cid}", params={"propertiesWithHistory": "hs_merged_object_ids"})
    hist = h.get("propertiesWithHistory", {}).get("hs_merged_object_ids", [])
    seen = set()
    for v in hist:                      # newest first; each entry is the full id list at that time
        ts = v.get("timestamp", "")
        try: ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception: continue
        ids = [x for x in (v.get("value") or "").split(";") if x]
        new = [x for x in ids if x not in seen]
        seen.update(ids)
        if ms >= since_ms and new:
            rows.append([cid, r["properties"].get("name"), r["properties"].get("hs_lead_status"), ";".join(new), ts,
                         f"https://app-na2.hubspot.com/contacts/243636394/record/0-2/{cid}"])
    if n % 200 == 0: log(f"{n:,} survivors checked")
    time.sleep(0.11)

write_csv("merged_companies.csv", rows, ["survivor_id", "survivor_name", "survivor_lead_status", "merged_ids", "merge_time", "link"])
log(f"survivors with merge history: {n:,} | merge events in the last {DAYS:g} days: {len(rows):,} | companies merged away: {sum(len(x[3].split(';')) for x in rows):,}")
