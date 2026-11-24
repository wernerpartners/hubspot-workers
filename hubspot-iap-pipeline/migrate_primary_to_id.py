"""migrate_primary_to_id.py — one-off: iap_primary_campaign held the campaign NAME; switch it to the campaign record ID
and put the name in iap_primary_campaign_name. Names are matched to Campaigns (Telemarketing) records, ignoring case.
A value that is already an ID only gets its name filled. A name with no matching campaign falls back to the company's only ticked campaign (enrichment_iap_campaigns);
otherwise it is left as is and listed.

  source ../.env && python3 migrate_primary_to_id.py
"""
import os, time, requests
from collections import Counter
HS = "https://api.hubapi.com"; V = "2026-09"; S = requests.Session()

def hs(method, path, **kw):
    h = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
    for a in range(7):
        r = S.request(method, HS + path, headers=h, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        if r.status_code >= 400: print(r.status_code, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def search_all(obj, body):
    rows, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/{obj}/search", json=body)
        rows += res.get("results", []); after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return rows

camps = search_all("2-175770723", {"properties": ["campaign_name"], "limit": 100})
by_id = {c["id"]: (c["properties"].get("campaign_name") or "").strip() for c in camps}
by_name = {}
for cid, n in by_id.items(): by_name.setdefault(n.lower(), []).append(cid)
# names the campaigns had when the primary was written (both Akoya campaigns were renamed on Nov 18)
OLD_NAMES = {"akoya - alinabal": "198888772293", "akoya - health & wellness": "198892371671"}
for old, cid in OLD_NAMES.items(): by_name.setdefault(old, [cid])

rows = search_all("companies", {"filterGroups": [{"filters": [{"propertyName": "iap_primary_campaign", "operator": "HAS_PROPERTY"}]}],
                                "properties": ["iap_primary_campaign", "iap_primary_campaign_name", "enrichment_iap_campaigns"], "limit": 200})
print(f"companies with iap_primary_campaign: {len(rows)} | campaign records: {len(by_id)}")
upd, unmatched, done = [], Counter(), Counter()
for r in rows:
    v = (r["properties"].get("iap_primary_campaign") or "").strip()
    if v.isdigit() and v in by_id:
        if (r["properties"].get("iap_primary_campaign_name") or "") != by_id[v]:
            upd.append({"id": r["id"], "properties": {"iap_primary_campaign_name": by_id[v]}}); done["name filled"] += 1
        continue
    ids = by_name.get(v.lower(), [])
    if len(ids) != 1:                                      # fall back to the only ticked campaign, when there is exactly one
        ticked = [x for x in (r["properties"].get("enrichment_iap_campaigns") or "").split(";") if x.strip()]
        if len(ticked) == 1 and ticked[0] in by_id: ids = ticked
    if len(ids) == 1:
        upd.append({"id": r["id"], "properties": {"iap_primary_campaign": ids[0], "iap_primary_campaign_name": by_id[ids[0]]}}); done[by_id[ids[0]]] += 1
    else: unmatched[f"{v} ({'no campaign' if not ids else str(len(ids)) + ' campaigns'} with that name)"] += 1
for i in range(0, len(upd), 100): hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": upd[i:i+100]})
print(f"updated: {len(upd)}")
for k, n in done.most_common(): print(f"  {n:5d}  {k}")
if unmatched:
    print("left as is (no single campaign with that name):")
    for k, n in unmatched.most_common(): print(f"  {n:5d}  {k}")
