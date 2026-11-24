"""contact-enrichment-worker — Enrichley Email Finder
Selects HubSpot contacts with no email, a first + last name, and an associated company with a domain,
asks Enrichley for the email, and writes it into the contact's Email property when status is "valid".
Found emails are already verified by Enrichley: no second verification call.
Outcome is recorded on the contact (email_finder_status, email_finder_date) so a contact is never looked up twice.

Env:
  HUBSPOT_TOKEN      contacts read+write, companies read
  ENRICHLEY_API_KEY  Enrichley key
  DRY_RUN            true (default) = call Enrichley and print, write nothing to HubSpot
  LIMIT              contacts per run (default 20)
  LIST_ID            optional HubSpot segment id to restrict to its members
Before first run create two contact properties:
  email_finder_status  dropdown: valid, not_found, error
  email_finder_date    date picker with time
"""
import os, re, sys, time, json, requests
from datetime import datetime, timezone

HS_TOKEN = os.environ["HUBSPOT_TOKEN"]; EN_KEY = os.environ["ENRICHLEY_API_KEY"]
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "20")); LIST_ID = os.getenv("LIST_ID")
HS = "https://api.hubapi.com"; V = "2026-09"
HSH = {"Authorization": f"Bearer {HS_TOKEN}", "Content-Type": "application/json"}
FINDER = "https://finder.enrichley.io/v1/find-email"
ENH = {"Authorization": f"Bearer {EN_KEY}", "Content-Type": "application/json", "Accept": "application/json"}
S = requests.Session()

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    """HubSpot call on the dated API; falls back to /crm/v3/ if the dated path is not there yet."""
    for attempt in range(6):
        try: r = S.request(method, HS + path, headers=HSH, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(2 ** attempt); continue
        if r.status_code == 404 and f"/crm/objects/{V}/" in path:
            path = path.replace(f"/crm/objects/{V}/", "/crm/v3/objects/"); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(2 ** attempt); continue
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def norm_domain(s):
    s = (s or "").strip().lower(); s = re.sub(r"^https?://", "", s); s = re.sub(r"^www\.", "", s)
    return s.split("/")[0].split("?")[0]

def candidates():
    """Contacts never looked up, with names, no email, and a company. Returns list of records."""
    filters = [{"propertyName": "email", "operator": "NOT_HAS_PROPERTY"},
               {"propertyName": "firstname", "operator": "HAS_PROPERTY"},
               {"propertyName": "lastname", "operator": "HAS_PROPERTY"},
               {"propertyName": "associatedcompanyid", "operator": "HAS_PROPERTY"},
               {"propertyName": "email_finder_status", "operator": "NOT_HAS_PROPERTY"}]
    if LIST_ID: filters.append({"propertyName": "hs_object_id", "operator": "IN", "values": list_member_ids()})
    out, after = [], None
    while len(out) < LIMIT:
        body = {"filterGroups": [{"filters": filters}], "limit": min(100, LIMIT - len(out)),
                "properties": ["firstname", "lastname", "associatedcompanyid", "hs_linkedin_url", "company"]}
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/contacts/search", json=body)
        out += res.get("results", [])
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    return out[:LIMIT]

def list_member_ids():
    ids, after = [], None
    while True:
        params = {"limit": 250}
        if after: params["after"] = after
        res = hs("GET", f"/crm/v3/lists/{LIST_ID}/memberships", params=params)
        ids += [r["recordId"] for r in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after or len(ids) >= 10000: break
    return ids[:10000]

def company_domains(ids):
    out = {}
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/companies/batch/read",
                 json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": ["domain", "website", "name"]})
        for r in res.get("results", []):
            p = r["properties"]; out[r["id"]] = norm_domain(p.get("domain") or p.get("website"))
    return out

def find(payload):
    """One finder call. Returns (status, email, credits_left). status: valid | not_found | error."""
    for attempt in range(5):
        r = S.post(FINDER, headers=ENH, json=payload, timeout=90)
        left = r.headers.get("X-Credits-Remaining")
        if r.status_code == 200:
            d = r.json(); return ("valid" if d.get("valid") and d.get("email") else "not_found"), d.get("email"), left
        if r.status_code == 429: time.sleep(int(r.headers.get("Retry-After", "5"))); continue
        if r.status_code in (503, 504): time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 402: log("Enrichley: out of credits, stopping"); sys.exit(2)
        if r.status_code == 401: log("Enrichley: bad API key"); sys.exit(2)
        log(f"Enrichley {r.status_code}: {r.text[:200]}"); return "error", None, left
    return "error", None, None

def write(cid, props):
    if DRY_RUN: return
    hs("PATCH", f"/crm/objects/{V}/contacts/{cid}", json={"properties": props})

def main():
    log("DRY_RUN:", DRY_RUN, "| LIMIT:", LIMIT, "| LIST_ID:", LIST_ID)
    recs = candidates(); log(f"candidates: {len(recs)}")
    doms = company_domains(list({r["properties"]["associatedcompanyid"] for r in recs}))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    found = nf = err = skipped = 0; left = None
    for r in recs:
        p = r["properties"]; cid = r["id"]
        first, last = (p.get("firstname") or "").strip(), (p.get("lastname") or "").strip()
        dom = doms.get(p.get("associatedcompanyid"), ""); li = (p.get("hs_linkedin_url") or "").strip()
        if not dom and not li:
            skipped += 1; continue  # nothing to search on; left untouched so it is retried once the company has a domain
        payload = {"full_name": f"{first} {last}", "first_name": first, "last_name": last}
        if dom: payload["domain"] = dom
        if li: payload["linkedin_profile_url"] = li
        status, email, left = find(payload)
        if status == "valid":
            found += 1; write(cid, {"email": email, "email_finder_status": "valid", "email_finder_date": now})
        elif status == "not_found":
            nf += 1; write(cid, {"email_finder_status": "not_found", "email_finder_date": now})
        else:
            err += 1; write(cid, {"email_finder_status": "error", "email_finder_date": now})
        log(f"{first} {last} @ {dom or li} -> {status} {email or ''}")
    log(f"done. found {found} | not found {nf} | errors {err} | skipped (no domain/linkedin) {skipped} | credits left {left}")

if __name__ == "__main__": main()
