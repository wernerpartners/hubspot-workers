"""Delete companies whose domain is a directory / social / shortener / site-builder domain
(the same blocked list as dedupe_companies.py) and that have NO emails, NO calls and NO deals,
neither on the company nor on any of its contacts.
Run from ~/Documents/WP/hubspot-dedupe (uses common.py and dedupe_companies.py).
Env: HUBSPOT_TOKEN
     DRY_RUN (default true)        report only -> directory_companies.csv
     DELETE_CONTACTS (default false) also delete the contacts of the deleted companies
Deleted records go to HubSpot's recycling bin for 90 days and can be restored there.
"""
import os, csv, time
from common import hs, log, write_csv, norm_domain
from dedupe_companies import DIRECTORY_DOMAINS, BUILDER_DOMAINS

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
DELETE_CONTACTS = os.getenv("DELETE_CONTACTS", "false").lower() == "true"
URL = "https://app-na2.hubspot.com/contacts/243636394/record/0-2/{}"

def search_domain(d):
    out, after = [], None
    while True:
        body = {"filterGroups": [{"filters": [{"propertyName": "domain", "operator": "CONTAINS_TOKEN", "value": d}]}],
                "properties": ["name", "domain"], "limit": 200}
        if after: body["after"] = after
        res = hs("POST", "/crm/v3/objects/companies/search", json=body)
        out += res.get("results", [])
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return out
        time.sleep(0.2)

def assoc(from_type, ids, to_type):
    m = {}
    for k in range(0, len(ids), 100):
        res = hs("POST", f"/crm/v4/associations/{from_type}/{to_type}/batch/read", json={"inputs": [{"id": str(i)} for i in ids[k:k + 100]]})
        for r in res.get("results", []):
            m[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to", [])]
    return m

blocked = DIRECTORY_DOMAINS | BUILDER_DOMAINS
companies = {}
for d in sorted(blocked):
    for r in search_domain(d):
        nd = norm_domain(r["properties"].get("domain"))
        if nd == d or (d in DIRECTORY_DOMAINS and nd.endswith("." + d)):
            companies[r["id"]] = r["properties"]
    time.sleep(0.2)
ids = list(companies)
log(f"companies with a blocked domain: {len(ids):,}")

c_contacts = assoc("companies", ids, "contacts")
contacts = list({x for v in c_contacts.values() for x in v})
act = {}
for kind in ["emails", "calls", "deals"]:
    for i, v in assoc("companies", ids, kind).items(): act.setdefault(i, {}).setdefault(kind, 0); act[i][kind] += len(v)
    ct = assoc("contacts", contacts, kind)
    for i in ids:
        n = sum(len(ct.get(c, [])) for c in c_contacts.get(i, []))
        act.setdefault(i, {}).setdefault(kind, 0); act[i][kind] += n

rows, to_delete, ct_delete = [], [], []
for i in ids:
    a = act.get(i, {})
    clean = not any(a.get(k) for k in ["emails", "calls", "deals"])
    rows.append([i, companies[i].get("name"), companies[i].get("domain"), len(c_contacts.get(i, [])),
                 a.get("emails", 0), a.get("calls", 0), a.get("deals", 0), "delete" if clean else "keep", URL.format(i)])
    if clean:
        to_delete.append(i); ct_delete += c_contacts.get(i, [])
write_csv("directory_companies.csv", rows, ["company_id", "name", "domain", "contacts", "emails", "calls", "deals", "action", "hubspot_link"])
log(f"to delete: {len(to_delete):,} companies ({len(ct_delete):,} contacts, {'deleted too' if DELETE_CONTACTS else 'kept'}) | keep: {len(ids) - len(to_delete):,}")

if DRY_RUN:
    log("DRY_RUN - nothing deleted. Review directory_companies.csv, then rerun with DRY_RUN=false"); raise SystemExit
for k in range(0, len(to_delete), 100):
    hs("POST", "/crm/v3/objects/companies/batch/archive", json={"inputs": [{"id": x} for x in to_delete[k:k + 100]]})
if DELETE_CONTACTS:
    for k in range(0, len(ct_delete), 100):
        hs("POST", "/crm/v3/objects/contacts/batch/archive", json={"inputs": [{"id": x} for x in ct_delete[k:k + 100]]})
log(f"deleted {len(to_delete):,} companies" + (f" and {len(ct_delete):,} contacts" if DELETE_CONTACTS else "") + ". done")
