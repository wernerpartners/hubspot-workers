"""copy_retired.py — one-off: tick the Retired checkbox (retired) on every contact whose text property retired_contact is "true".

Only sets retired = true. Never unticks anything and never touches retired_contact.

  export HUBSPOT_TOKEN=...
  DRY_RUN=true python3 copy_retired.py     # counts and lists the first 20, writes nothing
  python3 copy_retired.py                  # writes
"""
import os, sys, time, requests

TOKEN = os.environ["HUBSPOT_TOKEN"]
DRY = os.getenv("DRY_RUN", "false").lower() == "true"
HS = "https://api.hubapi.com"; V = "2026-09"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
S = requests.Session()

def hs(method, path, **kw):
    for a in range(7):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        if r.status_code >= 400: print(r.status_code, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def todo():
    """Contacts with retired_contact = true and retired not true (false or empty). Search caps at 10,000 per query."""
    groups = [
        {"filters": [{"propertyName": "retired_contact", "operator": "EQ", "value": "true"},
                     {"propertyName": "retired", "operator": "NEQ", "value": "true"}]},
        {"filters": [{"propertyName": "retired_contact", "operator": "EQ", "value": "true"},
                     {"propertyName": "retired", "operator": "NOT_HAS_PROPERTY"}]},
    ]
    body = {"filterGroups": groups, "properties": ["firstname", "lastname", "retired", "retired_contact"], "limit": 200,
            "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]}
    rows, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/contacts/search", json=body)
        rows += res.get("results", [])
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    seen, out = set(), []
    for r in rows:
        if r["id"] not in seen: seen.add(r["id"]); out.append(r)
    return out

rows = todo()
print(f"contacts to tick Retired on: {len(rows)}")
for r in rows[:20]:
    p = r["properties"]; print(f"  {r['id']} {p.get('firstname') or ''} {p.get('lastname') or ''} | retired={p.get('retired')}")
if DRY: print("DRY_RUN: nothing written"); sys.exit()
done = 0
for i in range(0, len(rows), 100):
    chunk = [{"id": r["id"], "properties": {"retired": "true"}} for r in rows[i:i+100]]
    hs("POST", f"/crm/objects/{V}/contacts/batch/update", json={"inputs": chunk})
    done += len(chunk); print(f"written {done}/{len(rows)}")
print("done")
