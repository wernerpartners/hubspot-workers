"""migrate_enrichley_status.py — one-off: fill Enrichley status from the old enrichment_status sentence. No provider calls, no credits.

For every contact that has an email, an enrichment_status sentence and an EMPTY Enrichley status:
  valid   <- Enrichley validated the email, the Enrichley finder found / confirmed it, Findymail found it and Enrichley
             validated it, or the contact really replied ("email: replied, not checked")
  unknown <- sentence says "email unknown: enrichley" and no replacement was written
  invalid <- sentence says "email invalid: ..." (Enrichley, HubSpot bounce flag or Smartlead bounce) and no replacement was written
Also writes Enrichley checked email = the current email and invalid_primary_email (No for valid, Yes otherwise).
Contacts marked left company, without an email, or with any other sentence are left untouched and listed at the end.

  source ../.env && python3 migrate_enrichley_status.py
"""
import os, re, sys, time, requests
from collections import Counter

HS = "https://api.hubapi.com"; V = "2026-09"
S = requests.Session()

def hs(method, path, **kw):
    h = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
    for a in range(7):
        r = S.request(method, HS + path, headers=h, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        if r.status_code >= 400: print(r.status_code, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def decide(p):
    """-> "valid" | "invalid" | "unknown" | None (leave untouched)."""
    s = (p.get("enrichment_status") or "").lower()
    if not (p.get("email") or "").strip() or not s: return None
    if s.startswith("left company") or (p.get("left_company") or "").lower() == "true": return None
    dup = "replacement email already on contact" in s          # the replacement was NOT written: judge the old address
    if not dup and re.search(r"email (valid|replaced): [^;]*enrichley|email: (enrichley finder|findymail (name|linkedin) \+ enrichley)", s): return "valid"
    if "email valid: enrichley" in s or "email valid: confirmed by enrichley finder" in s: return "valid"
    if "email: replied, not checked" in s: return "valid"
    if "email unknown: enrichley" in s: return "unknown"
    if "email invalid:" in s: return "invalid"
    return None

def todo():
    body = {"filterGroups": [{"filters": [{"propertyName": "enrichment_status", "operator": "HAS_PROPERTY"},
                                          {"propertyName": "enrichley_status", "operator": "NOT_HAS_PROPERTY"},
                                          {"propertyName": "email", "operator": "HAS_PROPERTY"}]}],
            "properties": ["email", "enrichment_status", "left_company", "invalid_primary_email"], "limit": 200,
            "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]}
    rows, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/contacts/search", json=body)
        rows += res.get("results", []); after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    return rows

def main():
    rows = todo(); print(f"contacts with an email, a status sentence and no Enrichley status: {len(rows)}")
    inputs, skipped, n = [], Counter(), Counter()
    for r in rows:
        p = r["properties"]; d = decide(p)
        if not d: skipped[(p.get("enrichment_status") or "")[:70]] += 1; continue
        n[d] += 1
        inputs.append({"id": r["id"], "properties": {"enrichley_status": d, "enrichley_checked_email": p["email"].strip(),
                                                     "invalid_primary_email": "false" if d == "valid" else "true"}})
    for i in range(0, len(inputs), 100):
        hs("POST", f"/crm/objects/{V}/contacts/batch/update", json={"inputs": inputs[i:i+100]})
    print(f"written: valid {n['valid']}, invalid {n['invalid']}, unknown {n['unknown']} (total {len(inputs)})")
    print(f"left untouched: {sum(skipped.values())}")
    for k, v in skipped.most_common(15): print(f"  {v:4d}  {k}")

if __name__ == "__main__": main()
