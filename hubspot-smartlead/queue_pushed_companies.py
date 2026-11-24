"""queue_pushed_companies.py — one-off fix: companies whose contacts are already in a Smartlead campaign must not stay
in Public Pool (otherwise the push sends them a second contact).

For each Smartlead campaign below, it reads every lead from Smartlead itself, finds the HubSpot contact by email,
takes the contact's primary company, and moves that company from Public Pool to Campaign Queue.
Companies with any other Lead Status (Closed Lead, Working, ...) are NOT changed unless MOVE_ALL=true; they are listed in the CSV.
Leads with no HubSpot contact, or a contact with no company, are listed in the CSV too.

  cd ~/Documents/Werner\\ Partners/hubspot/hubspot-smartlead && source ../.env
  DRY_RUN=true python3 queue_pushed_companies.py     # nothing changed, CSV shows what would happen
  python3 queue_pushed_companies.py                  # moves Public Pool companies to Campaign Queue
Output: queue_pushed_companies_<date>.csv
"""
import os, csv, time, datetime, requests

HS = "https://api.hubapi.com"; V = "2026-09"
H = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
SL = "https://server.smartlead.ai/api/v1"; SL_KEY = os.environ["SMARTLEAD_API_KEY"]
CAMPAIGNS = {"4639837": "Akoya - Strategic - Alinabal", "4640051": "Akoya - Industry Theme - Health & Wellness"}
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
MOVE_ALL = os.getenv("MOVE_ALL", "false").lower() == "true"
S = requests.Session()

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    for a in range(7):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        if r.status_code >= 400: raise requests.HTTPError(f"HubSpot said {r.status_code}: {r.text[:300]}", response=r)
        return r.json() if r.text else {}
    raise requests.HTTPError(f"HubSpot busy after 7 tries ({r.status_code})")

def sl_leads(cid):
    out, offset = [], 0
    while True:
        for a in range(5):
            r = S.get(f"{SL}/campaigns/{cid}/leads", params={"api_key": SL_KEY, "offset": offset, "limit": 100}, timeout=60)
            if r.status_code == 429 or r.status_code >= 500: time.sleep(2 ** a); continue
            r.raise_for_status(); break
        j = r.json(); rows = j.get("data", j if isinstance(j, list) else [])
        for x in rows:
            lead = x.get("lead", x)
            e = (lead.get("email") or "").strip().lower()
            if e: out.append(e)
        if len(rows) < 100: return out
        offset += 100

def contacts_by_email(emails):
    found = {}
    emails = sorted(set(emails))
    for i in range(0, len(emails), 100):
        res = hs("POST", f"/crm/objects/{V}/contacts/search", json={"filterGroups": [{"filters": [
            {"propertyName": "email", "operator": "IN", "values": emails[i:i+100]}]}],
            "properties": ["email", "firstname", "lastname", "associatedcompanyid"], "limit": 200})
        for r in res.get("results", []):
            found[(r["properties"].get("email") or "").lower()] = r
    return found

today = datetime.date.today()
rows_out, moves = [], {}
for cid, cname in CAMPAIGNS.items():
    emails = sl_leads(cid)
    log(f"{cname}: {len(emails)} leads in Smartlead")
    contacts = contacts_by_email(emails)
    comp_ids = {c["properties"].get("associatedcompanyid") for c in contacts.values() if c["properties"].get("associatedcompanyid")}
    comps = {}
    ids = sorted(comp_ids)
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/companies/batch/read", json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": ["name", "hs_lead_status"]})
        for r in res.get("results", []): comps[r["id"]] = r["properties"]
    for e in emails:
        c = contacts.get(e)
        if not c:
            rows_out.append([cname, e, "", "", "", "", "no HubSpot contact with this email"]); continue
        comp = c["properties"].get("associatedcompanyid") or ""
        if not comp or comp not in comps:
            rows_out.append([cname, e, c["id"], comp, "", "", "contact has no primary company"]); continue
        st = comps[comp].get("hs_lead_status") or ""
        if st == "Campaign Queue": action = "already Campaign Queue"
        elif st == "Public Pool" or MOVE_ALL: action = "moved to Campaign Queue" if not DRY_RUN else "would move to Campaign Queue"; moves[comp] = True
        else: action = f"left as {st or '(empty)'} (not Public Pool)"
        rows_out.append([cname, e, c["id"], comp, comps[comp].get("name") or "", st, action])

if moves and not DRY_RUN:
    ids = sorted(moves)
    for i in range(0, len(ids), 100):
        hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": [{"id": x, "properties": {"hs_lead_status": "Campaign Queue"}} for x in ids[i:i+100]]})

out = f"queue_pushed_companies_{today}.csv"
with open(out, "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["smartlead_campaign", "lead_email", "contact_id", "company_id", "company_name", "lead_status_before", "action"])
    w.writerows(rows_out)
from collections import Counter
cnt = Counter(r[6] for r in rows_out)
log(("DRY RUN - nothing changed. " if DRY_RUN else "") + f"companies {'to move' if DRY_RUN else 'moved'} to Campaign Queue: {len(moves)}")
for k, v in cnt.most_common(): log(f"  {v:4d} leads: {k}")
log(f"details in {out}")
