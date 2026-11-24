"""
smartlead_status_worker.py — hourly: write each contact's Smartlead lead status into HubSpot (Last campaign lead status).

For every Smartlead campaign that is some contact's Last campaign ID (os_last_campaign_id, filled by Outbound Sync) or,
when Outbound Sync has not filled it, the last campaign in smartlead_campaign_lead_ids (written by push_to_smartlead.py):
  1. Reads the campaign's leads with Smartlead's Get Campaign Leads (GET /campaigns/{id}/leads, 100 per call):
     the first run reads every lead; later runs read only leads with activity or added since the previous run
     (event_time_gt and created_at_gt), minus a 2-hour overlap so nothing is missed between runs.
  2. Matches each lead to the HubSpot contacts with the same email AND that campaign as Last campaign ID.
  3. Writes the status to last_campaign_lead_status (STARTED, INPROGRESS, COMPLETED, PAUSED, STOPPED) when it changed.
     Smartlead counts finished sequences and replies as COMPLETED.
The time of the last successful run per campaign is kept in status_worker_state.json next to this script.

  source ../.env && python3 smartlead_status_worker.py          # FULL=true re-reads every lead of every campaign
Env: HUBSPOT_TOKEN, SMARTLEAD_API_KEY (required); FULL=false; DRY_RUN=false.
"""
import os, sys, json, time, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

HS = "https://api.hubapi.com"; V = "2026-09"; SL = "https://server.smartlead.ai/api/v1"
PROP = "last_campaign_lead_status"; STATUSES = {"STARTED", "INPROGRESS", "COMPLETED", "PAUSED", "STOPPED"}
FULL = os.getenv("FULL", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "status_worker_state.json")
OVERLAP = timedelta(hours=2)
S = requests.Session()
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    h = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
    for a in range(7):
        try: r = S.request(method, HS + path, headers=h, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(min(2 ** a, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 60)); continue
        if r.status_code >= 400: log("HubSpot", r.status_code, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    raise RuntimeError("HubSpot unreachable")

def sl_leads(camp_id, **filters):
    """All leads of a Smartlead campaign (optionally filtered), as a list of (email, status)."""
    out, offset = [], 0
    while True:
        params = {"api_key": os.environ["SMARTLEAD_API_KEY"], "offset": offset, "limit": 100, **filters}
        for a in range(6):
            r = S.get(f"{SL}/campaigns/{camp_id}/leads", params=params, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(5 * (a + 1), 60)); continue
            break
        if r.status_code == 404: log(f"   Smartlead campaign {camp_id} not found (deleted?), skipped"); return None
        if r.status_code != 200: raise RuntimeError(f"Smartlead {r.status_code}: {r.text[:200]}")
        data = r.json().get("data") or []
        for d in data:
            email = ((d.get("lead") or {}).get("email") or "").strip().lower()
            if email: out.append((email, (d.get("status") or "").upper()))
        if len(data) < 100: return out
        offset += 100

def contacts_by_campaign():
    """{Smartlead campaign id: {email: [(contact id, current status)]}}. Campaign = os_last_campaign_id (Outbound Sync);
    if empty, the last campaign in smartlead_campaign_lead_ids ("<campaign id>:<campaign lead id>;...", from the push).
    Searches page by record ID, so HubSpot's 10,000-result search limit is never reached."""
    groups = defaultdict(dict)
    for prop in ("os_last_campaign_id", "smartlead_campaign_lead_ids"):
        last_id = 0
        while True:
            filters = [{"propertyName": prop, "operator": "HAS_PROPERTY"}, {"propertyName": "hs_object_id", "operator": "GT", "value": str(last_id)}]
            if prop == "smartlead_campaign_lead_ids": filters.append({"propertyName": "os_last_campaign_id", "operator": "NOT_HAS_PROPERTY"})
            res = hs("POST", f"/crm/objects/{V}/contacts/search", json={"filterGroups": [{"filters": filters}],
                     "properties": ["email", "os_last_campaign_id", "smartlead_campaign_lead_ids", PROP], "limit": 200,
                     "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]})
            rows = res.get("results", [])
            for r in rows:
                p = r["properties"]; e = (p.get("email") or "").strip().lower(); last_id = max(last_id, int(r["id"]))
                cid = (p.get("os_last_campaign_id") or "").strip()
                if not cid:
                    items = [x for x in (p.get("smartlead_campaign_lead_ids") or "").split(";") if ":" in x]
                    cid = items[-1].split(":")[0].strip() if items else ""
                if e and cid.isdigit(): groups[cid].setdefault(e, []).append((r["id"], p.get(PROP) or ""))
            if len(rows) < 200: break
    return groups

def main():
    started = datetime.now(timezone.utc)
    try: state = {} if FULL else json.load(open(STATE))
    except (FileNotFoundError, json.JSONDecodeError): state = {}
    groups = contacts_by_campaign()
    log(f"FULL: {FULL or not state} | DRY_RUN: {DRY_RUN} | campaigns: {len(groups)} | contacts with a Last campaign ID: {sum(len(g) for g in groups.values())}")
    updates, n_read = [], 0
    for camp, emails in sorted(groups.items()):
        since = state.get(camp)
        try:
            if since:
                t = (datetime.fromisoformat(since) - OVERLAP).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                a = sl_leads(camp, event_time_gt=t); b = sl_leads(camp, created_at_gt=t)
                leads = None if a is None or b is None else a + b
            else:
                leads = sl_leads(camp)
        except Exception as e:
            log(f"   campaign {camp}: error, skipped ({str(e)[:150]})"); continue
        if leads is None: continue
        n_read += len(leads); changed = 0
        for email, status in leads:
            if status not in STATUSES: continue
            for hid, current in emails.get(email, []):
                if current != status: updates.append({"id": hid, "properties": {PROP: status}}); changed += 1
        state[camp] = started.isoformat()
        log(f"   campaign {camp}: {len(leads)} leads read, {changed} HubSpot contacts to update")
    seen, final = set(), []
    for u in reversed(updates):                    # one write per contact, the last one wins
        if u["id"] not in seen: seen.add(u["id"]); final.append(u)
    if not DRY_RUN:
        for i in range(0, len(final), 100): hs("POST", f"/crm/objects/{V}/contacts/batch/update", json={"inputs": final[i:i+100]})
        json.dump(state, open(STATE, "w"), indent=1)
    log(f"done. leads read {n_read} | contacts updated {len(final)} | {(datetime.now(timezone.utc) - started).seconds}s")

if __name__ == "__main__": main()
