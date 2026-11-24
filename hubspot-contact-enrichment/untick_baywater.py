"""untick_baywater.py — remove the Baywater tick from companies stage 2 never confirmed.

Scope: companies with Baywater (325749324474) in enrichment_iap_campaigns AND enrichment_status != Done
(Pending, Unassigned or empty). Companies stage 2 confirmed (Done) are untouched.

For each: drop 325749324474 from enrichment_iap_campaigns, drop the Baywater name from enrichment_iap_matches,
clear iap_primary_campaign when it names Baywater. Nothing else changes.

  export HUBSPOT_TOKEN=...
  DRY_RUN=true python3 untick_baywater.py     # lists what would change, writes nothing
  python3 untick_baywater.py                  # writes
"""
import os, sys, time, requests

TOKEN = os.environ["HUBSPOT_TOKEN"]; DRY = os.getenv("DRY_RUN", "false").lower() == "true"
CAMP_ID = os.getenv("CAMPAIGN", "325749324474"); CAMP_NAME = os.getenv("CAMPAIGN_NAME", "Baywater")
HS = "https://api.hubapi.com"; V = "2026-09"; H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
S = requests.Session()

def hs(method, path, **kw):
    for a in range(6):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def search():
    groups = [
        {"filters": [{"propertyName": "enrichment_iap_campaigns", "operator": "CONTAINS_TOKEN", "value": CAMP_ID},
                     {"propertyName": "enrichment_status", "operator": "NEQ", "value": "Done"}]},
        {"filters": [{"propertyName": "enrichment_iap_campaigns", "operator": "CONTAINS_TOKEN", "value": CAMP_ID},
                     {"propertyName": "enrichment_status", "operator": "NOT_HAS_PROPERTY"}]},
    ]
    body = {"filterGroups": groups, "properties": ["name", "enrichment_iap_campaigns", "enrichment_iap_matches",
            "iap_primary_campaign", "enrichment_status"], "limit": 200}
    rows, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/companies/search", json=body)
        rows += res.get("results", []); after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    seen, out = set(), []
    for r in rows:
        if r["id"] not in seen: seen.add(r["id"]); out.append(r)
    return out

rows = search(); print(f"companies to untick: {len(rows)}")
updates = []
for r in rows:
    p = r["properties"]
    ids = [x for x in (p.get("enrichment_iap_campaigns") or "").split(";") if x and x != CAMP_ID]
    names = [x.strip() for x in (p.get("enrichment_iap_matches") or "").split(",") if x.strip() and CAMP_NAME.lower() not in x.lower()]
    props = {"enrichment_iap_campaigns": ";".join(ids), "enrichment_iap_matches": ", ".join(names)}
    prim = p.get("iap_primary_campaign") or ""
    if prim == CAMP_ID or CAMP_NAME.lower() in prim.lower(): props["iap_primary_campaign"] = ""
    updates.append({"id": r["id"], "properties": props})
    print(f"  {p.get('name')} | status={p.get('enrichment_status')} | {p.get('enrichment_iap_campaigns')} -> {props['enrichment_iap_campaigns'] or '(none)'}")
if DRY: print("DRY_RUN: nothing written"); sys.exit()
done = 0
for i in range(0, len(updates), 100):
    hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": updates[i:i+100]}); done += len(updates[i:i+100])
    print(f"written {done}/{len(updates)}")
print("done")
