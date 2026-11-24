"""
push_to_smartlead.py — daily: send ONE contact per company to the Smartlead campaign of each HubSpot campaign.

Run once a day. Per HubSpot campaign in CAMPAIGNS:
  1. The campaign must have Campaign Active = Yes, IAP Pipeline = Active and a Smartlead campaign ID
     (smartlead_campaign_id on the Campaigns (Telemarketing) record); otherwise it is skipped.
  2. Companies (always Company Record Type = Seller Prospect and Active/Inactive not Inactive and Is Buyer not true): iap_primary_campaign is this campaign AND Lead Status is
     Public Pool (new companies), or
     Lead Status is Campaign Queue and a contact of the company was already sent to this Smartlead campaign
     (so the next contact still goes the next day). Other lead statuses are never pushed.
     A company whose contact Smartlead accepts is moved from Public Pool to Campaign Queue.
     Company names are normalized and written back to HubSpot (Company name); the original goes to Legal name once.
  3. Per company, the eligible contacts are: Enrichley status = valid for the email now on the record,
     not Retired, not Left company, not already sent (or skipped) for this Smartlead campaign.
     The highest-ranked one is sent; the next one goes the next day. A company that got a contact for this
     campaign less than MIN_HOURS ago gets nothing in this run, so running twice a day never sends two.
  4. Ranking by job title: 1 owner / co-owner / founder / co-founder, 2 CEO / company president,
     3 COO / CFO, 4 other C-level, VP (incl. "vice president", "president of <department>"), general manager,
     5 director, 6 anything else or no title, 7 board member / chairman. Ties: oldest contact first.
  4b. If ANY contact of the company is already in ANOTHER Smartlead campaign whose status is ACTIVE or PAUSED, the whole
     company is skipped today (no other contact is tried). Drafted, stopped and completed campaigns do not block.
     Checked against Smartlead itself: every lead of every ACTIVE or PAUSED campaign is read once per run.
  5. Lead fields: first_name, last_name, email, company_name (normalized). Smartlead's own checks stay on
     (global block list, unsubscribes, duplicates in other campaigns, community bounce list).
  6. Confirmation: after each upload the campaign's leads are read back from Smartlead. A contact counts as SENT only
     when its email is found in the campaign. Then HubSpot gets: smartlead_pushed_campaign_ids + the campaign ID,
     smartlead_pushed_date, smartlead_lead_id (Smartlead's global lead ID) and smartlead_campaign_lead_ids
     (+ "<campaign id>:<campaign lead ID>"), and the company moves from Public Pool to Campaign Queue.
     Not found: the contact gets "skipped:<campaign id>" (so the next contact goes the next day), the company stays in
     Public Pool, and the reason (Smartlead's skip reason, or "not in the campaign after upload") goes to errors.csv.
  BACKFILL=true: instead of pushing, reads every lead already in each campaign's Smartlead campaign and writes the two
     ID properties on the matching HubSpot contacts. Contacts HubSpot marks as sent that are NOT in Smartlead are only
     listed in backfill_not_in_smartlead_<date>.csv, nothing is changed for them.

  source ../.env && CAMPAIGNS=198888772293,198892371671 python3 push_to_smartlead.py

Env: HUBSPOT_TOKEN, SMARTLEAD_API_KEY (required); CAMPAIGNS (HubSpot campaign record IDs, comma separated, required);
     MIN_HOURS=20; NORMALIZE_NAMES=true; DRY_RUN=false (true = nothing sent or written); BACKFILL=false.
"""
import os, re, sys, csv, json, time, requests
from datetime import datetime, timezone, timedelta

HS = "https://api.hubapi.com"; V = "2026-09"; CAMPAIGN_OBJECT = "2-175770723"
SL = "https://server.smartlead.ai/api/v1"
CAMPAIGNS = [c.strip() for c in os.getenv("CAMPAIGNS", "").split(",") if c.strip()]
MIN_HOURS = float(os.getenv("MIN_HOURS", "20"))
NORMALIZE = os.getenv("NORMALIZE_NAMES", "true").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
BACKFILL = os.getenv("BACKFILL", "false").lower() == "true"
HERE = os.path.dirname(os.path.abspath(__file__))
ERRORS_CSV = os.path.join(HERE, "errors.csv")
RUN_STARTED = time.strftime("%Y-%m-%d %H:%M:%S")
NEW_PROPS = [{"name": "smartlead_lead_id", "label": "Smartlead lead ID", "type": "string", "fieldType": "text", "groupName": "contactinformation",
              "description": "Smartlead global lead ID, written by push_to_smartlead.py when Smartlead confirms the lead is in a campaign."},
             {"name": "smartlead_campaign_lead_ids", "label": "Smartlead campaign lead IDs", "type": "string", "fieldType": "text", "groupName": "contactinformation",
              "description": "<Smartlead campaign ID>:<campaign lead ID> for every campaign the lead is confirmed in, separated by ;. Written by push_to_smartlead.py."}]
S = requests.Session()
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def record_error(step, cid, error, name=""):
    """One row in errors.csv: run start, time, step, contact, HubSpot link, reason. Never stops the run."""
    try:
        new = not os.path.exists(ERRORS_CSV)
        with open(ERRORS_CSV, "a", newline="") as fh:
            w = csv.writer(fh)
            if new: w.writerow(["run_started", "date_time", "step", "contact_id", "contact_name", "hubspot_link", "error"])
            w.writerow([RUN_STARTED, time.strftime("%Y-%m-%d %H:%M:%S"), step, cid or "", name or "",
                        f"https://app.hubspot.com/contacts/243636394/record/0-1/{cid}" if cid else "", re.sub(r"\s+", " ", str(error))[:1000]])
    except Exception: pass

# ------------------------------------------------------------------ HubSpot
def hs(method, path, **kw):
    h = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
    r = None
    for a in range(7):
        try: r = S.request(method, HS + path, headers=h, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(min(2 ** a, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 60)); continue
        if r.status_code >= 400: log("HubSpot", r.status_code, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    raise RuntimeError("HubSpot unreachable")

def search_all(obj, filters, props):
    body = {"filterGroups": [{"filters": filters}], "properties": props, "limit": 200}
    rows, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/{obj}/search", json=body)
        rows += res.get("results", []); after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return rows

def batch_read(obj, ids, props):
    out, ids = [], list(ids)
    for i in range(0, len(ids), 100):
        out += hs("POST", f"/crm/objects/{V}/{obj}/batch/read", json={"properties": props, "inputs": [{"id": x} for x in ids[i:i+100]]}).get("results", [])
    return out

def batch_update(obj, inputs):
    for i in range(0, len(inputs), 100):
        if not DRY_RUN: hs("POST", f"/crm/objects/{V}/{obj}/batch/update", json={"inputs": inputs[i:i+100]})

def contacts_of(company_ids):
    out, ids = {}, list(company_ids)
    for i in range(0, len(ids), 100):
        res = hs("POST", "/crm/v4/associations/companies/contacts/batch/read", json={"inputs": [{"id": c} for c in ids[i:i+100]]})
        for r in res.get("results", []): out[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to", [])]
    return out

# ------------------------------------------------------------------ rules
SUFFIX = r"(?:incorporated|inc|llc|l\.l\.c|corporation|corp|ltd|limited|lp|l\.p|llp|pllc|plc|pc|p\.c|co)"
def normalize_name(name):
    """'CCT Plastics, Inc. dba CCT Precision Machining' -> 'CCT Precision Machining'; 'Atlantic Precision Spring, Inc. (APS)'
    -> 'Atlantic Precision Spring'. Returns the input unchanged when cleaning would leave nothing."""
    n = (name or "").strip()
    if not n: return n
    m = re.search(r"\b(?:d/b/a|dba|d\.b\.a\.?)\s+(.+)$", n, re.I)
    if m: n = m.group(1)
    for _ in range(3):
        n = re.sub(r"\s*\([^()]*\)\s*$", "", n).strip()                                   # trailing (APS)
        n = re.sub(rf"[\s,]+{SUFFIX}\.?$", "", n, flags=re.I).strip(" ,.-")                # trailing Inc. / LLC / Corp ...
    n = re.sub(r"\s{2,}", " ", n).strip(" ,.-")
    return n if n and not re.fullmatch(SUFFIX + r"\.?", n, re.I) else (name or "").strip()

TIERS = [
    (1, r"\b(co-?owner|owner|co-?founder|founder|proprietor)\b"),
    (2, r"\b(ceo|chief executive)\b"),
    (3, r"\b(coo|cfo|chief operating|chief financial)\b"),
    (4, r"\b(c[a-z]o|chief\b|vice[\s-]*president|vp|svp|evp|avp|general manager|gm)\b"),
    (5, r"\bdirector\b"),
]
BOARD = r"\b(board|chairman|chairwoman|chair)\b"
def rank(title):
    t = (title or "").lower()
    if not t.strip(): return 6
    if re.search(r"\bpresident\b", t):
        vice = re.search(r"\b(vice|vp)\b", t) or re.search(r"\bpresident\s*(of|,|-|–)\s*(sales|marketing|operations|engineering|finance|business|division|region)", t)
        if not vice: return 2 if not re.search(TIERS[0][1], t) else 1
        if not re.search(TIERS[0][1] + "|" + TIERS[1][1] + "|" + TIERS[2][1], t): return 4      # department president = VP tier
    for tier, rx in TIERS:
        if re.search(rx, t): return tier
    return 7 if re.search(BOARD, t) else 6

def pushed_list(p): return [x for x in (p.get("smartlead_pushed_campaign_ids") or "").split(";") if x]

def eligible(p, sl_id):
    email = (p.get("email") or "").strip().lower()
    if not email: return False
    if (p.get("enrichley_status") or "") != "valid": return False
    if (p.get("enrichley_checked_email") or "").strip().lower() != email: return False
    if (p.get("retired") or "").lower() == "true" or (p.get("left_company") or "").lower() == "true": return False
    l = pushed_list(p)
    return sl_id not in l and f"skipped:{sl_id}" not in l

def recent(p, sl_id, cutoff):
    if sl_id not in pushed_list(p) and f"skipped:{sl_id}" not in pushed_list(p): return False
    d = p.get("smartlead_pushed_date") or ""
    try: return datetime.fromisoformat(d.replace("Z", "+00:00")) > cutoff
    except ValueError: return False

# ------------------------------------------------------------------ Smartlead
def smartlead_add(sl_id, leads):
    """Uploads leads (400 per request), asking for lead IDs (return_lead_ids).
    -> ({lowercased email: reason} for skipped leads, {lowercased email: global lead ID} for leads in this campaign).
    Tested 2026-11-24: the reply has emailToLeadIdMap = {newlyAddedLeads: {email: id}, existingLeads: {email: id},
    existingLeadsInOtherCampaigns: {email: id}}; the IDs are Smartlead's global lead IDs. With
    ignore_duplicate_leads_in_other_campaign = false (our setting) a lead already in another campaign IS added here."""
    skipped, ids = {}, {}
    for i in range(0, len(leads), 400):
        body = {"lead_list": leads[i:i+400], "settings": {"ignore_global_block_list": False, "ignore_unsubscribe_list": False,
                "ignore_duplicate_leads_in_other_campaign": False, "ignore_community_bounce_list": False, "return_lead_ids": True}}
        for a in range(6):
            r = S.post(f"{SL}/campaigns/{sl_id}/leads", params={"api_key": os.environ["SMARTLEAD_API_KEY"]}, json=body, timeout=120)
            if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(5 * (a + 1), 60)); continue
            break
        if r.status_code != 200: log(f"Smartlead {r.status_code}: {r.text[:300]}"); raise RuntimeError("Smartlead refused the request")
        d = r.json(); log(f"   Smartlead says: added {d.get('added_count')}, skipped {d.get('skipped_count')}")
        m = next((v for k, v in d.items() if k.lower() == "emailtoleadidmap" and isinstance(v, dict)), None)
        if m is None: record_error(f"Smartlead push {sl_id}", "", "upload reply had no emailToLeadIdMap; lead IDs taken from the read-back only")
        for group in ("newlyAddedLeads", "existingLeads", "existingLeadsInOtherCampaigns"):
            for e, lid in ((m or {}).get(group) or {}).items():
                ids[e.strip().lower()] = str(lid)
        for sk in d.get("skipped_leads") or []:
            e = (sk.get("email") or "").strip().lower() if isinstance(sk, dict) else ""
            reason = (sk.get("reason") or "") if isinstance(sk, dict) else str(sk)
            if e: skipped[e] = reason or "skipped by Smartlead"; continue
            txt = json.dumps(sk).lower()                       # unknown format: match by email anywhere in the entry
            for l in leads[i:i+400]:
                if l["email"].lower() in txt: skipped[l["email"].lower()] = reason or txt[:200]
    return skipped, ids

def campaign_leads(sl_id):
    """Every lead in a Smartlead campaign -> {lowercased email: (global lead ID, campaign lead ID)}."""
    out, offset = {}, 0
    while True:
        for a in range(6):
            r = S.get(f"{SL}/campaigns/{sl_id}/leads", params={"api_key": os.environ["SMARTLEAD_API_KEY"], "offset": offset, "limit": 100}, timeout=120)
            if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(5 * (a + 1), 60)); continue
            break
        if r.status_code != 200: raise RuntimeError(f"Smartlead {r.status_code} reading campaign leads: {r.text[:200]}")
        j = r.json(); rows = j.get("data", []) if isinstance(j, dict) else j
        for x in rows:
            lead = x.get("lead") if isinstance(x.get("lead"), dict) else x
            e = (lead.get("email") or "").strip().lower()
            if e: out[e] = (str(x.get("lead_id") or lead.get("id") or ""), str(x.get("campaign_lead_map_id") or ""))
        if len(rows) < 100: return out
        offset += 100

BUSY = None   # {lowercased email: {Smartlead campaign id: campaign name}} for every lead in an ACTIVE or PAUSED campaign
def busy_leads():
    """Reads once per run every lead of every Smartlead campaign whose status is ACTIVE or PAUSED."""
    global BUSY
    if BUSY is None:
        r = S.get(f"{SL}/campaigns/", params={"api_key": os.environ["SMARTLEAD_API_KEY"]}, timeout=120)
        if r.status_code != 200: raise RuntimeError(f"Smartlead {r.status_code} listing campaigns: {r.text[:200]}")
        rows = r.json(); rows = rows.get("data", rows) if isinstance(rows, dict) else rows
        live = {str(c.get("id")): c.get("name") or "" for c in rows if str(c.get("status") or "").upper() in ("ACTIVE", "PAUSED")}
        BUSY = {}
        for cid, cname in live.items():
            for e in campaign_leads(cid): BUSY.setdefault(e, {})[cid] = cname
        log(f"Smartlead campaigns ACTIVE or PAUSED: {len(live)}, with {len(BUSY)} leads in them")
    return BUSY

def other_live_campaign(emails, sl_id):
    """Name of another ACTIVE or PAUSED Smartlead campaign any of these emails is in, '' if none."""
    busy = busy_leads()
    for e in emails:
        for cid, cname in busy.get(e, {}).items():
            if cid != str(sl_id): return cname or cid
    return ""

def ensure_props():
    have = {p["name"] for p in hs("GET", "/crm/v3/properties/contacts").get("results", [])}
    for p in NEW_PROPS:
        if p["name"] not in have and not DRY_RUN: hs("POST", "/crm/v3/properties/contacts", json=p); log(f"created contact property {p['name']}")

def add_map(old, sl_id, map_id):
    items = [x for x in (old or "").split(";") if x and not x.startswith(f"{sl_id}:")]
    return ";".join(items + [f"{sl_id}:{map_id}"]) if map_id else ";".join(items)

def backfill(camp):
    """Writes Smartlead IDs on HubSpot contacts already in the campaign; lists contacts marked sent that are not there."""
    p = camp["properties"]; name = p.get("campaign_name") or camp["id"]; sl_id = (p.get("smartlead_campaign_id") or "").strip()
    if not re.fullmatch(r"\d+", sl_id): log(f"{name}: no Smartlead campaign ID, skipped"); return []
    found = campaign_leads(sl_id); log(f"{name}: {len(found)} leads in Smartlead campaign {sl_id}")
    emails = sorted(found); upd, matched = [], set()
    for i in range(0, len(emails), 100):
        res = hs("POST", f"/crm/objects/{V}/contacts/search", json={"filterGroups": [{"filters": [{"propertyName": "email", "operator": "IN", "values": emails[i:i+100]}]}],
                 "properties": ["email", "smartlead_campaign_lead_ids", "smartlead_pushed_campaign_ids"], "limit": 200})
        for r in res.get("results", []):
            e = (r["properties"].get("email") or "").lower(); lid, mid = found.get(e, ("", ""))
            if not lid and not mid: continue
            matched.add(r["id"])
            pushed = [x for x in (r["properties"].get("smartlead_pushed_campaign_ids") or "").split(";") if x and x != f"skipped:{sl_id}"]
            if sl_id not in pushed: pushed.append(sl_id)
            upd.append({"id": r["id"], "properties": {"smartlead_lead_id": lid, "smartlead_campaign_lead_ids": add_map(r["properties"].get("smartlead_campaign_lead_ids"), sl_id, mid),
                                                   "smartlead_pushed_campaign_ids": ";".join(pushed)}})
    batch_update("contacts", upd)
    marked = search_all("contacts", [{"propertyName": "smartlead_pushed_campaign_ids", "operator": "CONTAINS_TOKEN", "value": sl_id}], ["email", "firstname", "lastname", "smartlead_pushed_date"])
    missing = [[name, sl_id, r["id"], f"{r['properties'].get('firstname') or ''} {r['properties'].get('lastname') or ''}".strip(), r["properties"].get("email") or "",
                r["properties"].get("smartlead_pushed_date") or "", f"https://app.hubspot.com/contacts/243636394/record/0-1/{r['id']}"]
               for r in marked if r["id"] not in matched and (r["properties"].get("email") or "").lower() not in found]
    log(f"   IDs written on {len(upd)} contacts{' (dry run, nothing written)' if DRY_RUN else ''}; marked as sent in HubSpot but not in Smartlead: {len(missing)}")
    return missing

# ------------------------------------------------------------------ main
CPROPS = ["firstname", "lastname", "email", "jobtitle", "enrichley_status", "enrichley_checked_email", "retired", "left_company",
          "smartlead_pushed_campaign_ids", "smartlead_pushed_date", "smartlead_campaign_lead_ids"]

def run_campaign(camp):
    p = camp["properties"]; name = p.get("campaign_name") or camp["id"]; sl_id = (p.get("smartlead_campaign_id") or "").strip()
    if not re.fullmatch(r"\d+", sl_id): log(f"{name}: no Smartlead campaign ID, skipped"); return
    log(f"{name} -> Smartlead campaign {sl_id}")
    primary = [{"propertyName": "iap_primary_campaign", "operator": "EQ", "value": camp["id"]},
               {"propertyName": "company_record_type", "operator": "EQ", "value": "Seller Prospect"}]   # ALWAYS Seller Prospect
    pool = search_all("companies", primary + [{"propertyName": "hs_lead_status", "operator": "EQ", "value": "Public Pool"}], ["name", "legal_name", "active_inactive__c", "is_buyer"])
    queue = search_all("companies", primary + [{"propertyName": "hs_lead_status", "operator": "EQ", "value": "Campaign Queue"}], ["name", "legal_name", "active_inactive__c", "is_buyer"])
    ok = lambda c: (c["properties"].get("active_inactive__c") or "") != "Inactive" and str(c["properties"].get("is_buyer") or "").lower() != "true"
    inactive = sum(1 for c in pool + queue if not ok(c))
    pool, queue = [c for c in pool if ok(c)], [c for c in queue if ok(c)]   # ALWAYS: Inactive or Is Buyer companies are never pushed
    if inactive: log(f"   Inactive or Is Buyer companies left out: {inactive}")
    status = {c["id"]: "Public Pool" for c in pool}
    status.update({c["id"]: "Campaign Queue" for c in queue})
    comps = pool + queue
    log(f"   companies with this primary campaign: Public Pool {len(pool)}, Campaign Queue {len(queue)}")
    cname, renames = {}, []
    for c in comps:
        old = (c["properties"].get("name") or "").strip(); new = normalize_name(old) if NORMALIZE else old
        cname[c["id"]] = new
        if NORMALIZE and new and new != old:
            props = {"name": new}
            if not (c["properties"].get("legal_name") or "").strip(): props["legal_name"] = old
            renames.append({"id": c["id"], "properties": props})
    batch_update("companies", renames); log(f"   company names normalized: {len(renames)}")

    assoc = contacts_of(cname)
    rows = {r["id"]: r["properties"] for r in batch_read("contacts", {x for v in assoc.values() for x in v}, CPROPS)}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MIN_HOURS)
    pick, waiting, none, other_queue, busy = [], 0, 0, 0, 0
    for comp, cids in assoc.items():
        cs = [(cid, rows[cid]) for cid in cids if cid in rows]
        if status.get(comp) == "Campaign Queue" and not any(sl_id in pushed_list(cp) for _, cp in cs):
            other_queue += 1; continue                       # queued by something else, not by this campaign: leave it alone
        if any(recent(cp, sl_id, cutoff) for _, cp in cs): waiting += 1; continue
        el = [(rank(cp.get("jobtitle")), int(cid), cid, cp) for cid, cp in cs if eligible(cp, sl_id)]
        if not el: none += 1; continue
        other = other_live_campaign([(cp.get("email") or "").strip().lower() for _, cp in cs if (cp.get("email") or "").strip()], sl_id)
        if other:                                      # a contact of this company is in another ACTIVE/PAUSED campaign: skip the company
            busy += 1; log(f"   company {cname.get(comp) or comp}: a contact is already in Smartlead campaign '{other}', company skipped today"); continue
        el.sort(); _, _, cid, cp = el[0]; pick.append((comp, cid, cp))
    log(f"   sending today: {len(pick)} | company already got one in the last {MIN_HOURS:g}h: {waiting} | no contact left to send: {none + len(cname) - len(assoc)}"
        f" | in Campaign Queue but not from this campaign: {other_queue} | companies skipped (a contact is in another active or paused Smartlead campaign): {busy}")
    if not pick: return
    leads = [{"first_name": (cp.get("firstname") or "").strip(), "last_name": (cp.get("lastname") or "").strip(),
              "email": cp["email"].strip(), "company_name": cname.get(comp, "")} for comp, cid, cp in pick]
    if DRY_RUN:
        for l, (_, _, cp) in zip(leads[:15], pick[:15]): log("   (dry)", l["company_name"], "|", l["first_name"], l["last_name"], "|", cp.get("jobtitle") or "-")
        return
    skipped, upload_ids = smartlead_add(sl_id, leads)
    found = campaign_leads(sl_id)                       # confirmation: is each lead really in the campaign now?
    if any(cp["email"].strip().lower() not in found and cp["email"].strip().lower() not in skipped for _, _, cp in pick):
        time.sleep(20); found = campaign_leads(sl_id)    # give Smartlead a moment, then look once more
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"); upd, sent, failed = [], [], []
    for comp, cid, cp in pick:
        e = cp["email"].strip().lower()
        if e in found:
            lid, mid = found[e]; lid = upload_ids.get(e) or lid; sent.append(comp)   # global ID from the upload reply, campaign lead ID from the read-back
            upd.append({"id": cid, "properties": {"smartlead_pushed_campaign_ids": ";".join(pushed_list(cp) + [sl_id]), "smartlead_pushed_date": now,
                                                 "smartlead_lead_id": lid, "smartlead_campaign_lead_ids": add_map(cp.get("smartlead_campaign_lead_ids"), sl_id, mid)}})
        else:
            reason = skipped.get(e) or "not in the Smartlead campaign after upload (Smartlead did not list it as skipped)"
            failed.append(cid)
            upd.append({"id": cid, "properties": {"smartlead_pushed_campaign_ids": ";".join(pushed_list(cp) + [f"skipped:{sl_id}"]), "smartlead_pushed_date": now}})
            record_error(f"Smartlead push {sl_id}", cid, f"{e}: {reason}", f"{cp.get('firstname') or ''} {cp.get('lastname') or ''}".strip())
    batch_update("contacts", upd)
    queued = sorted({comp for comp in sent if status.get(comp) == "Public Pool"})
    batch_update("companies", [{"id": c, "properties": {"hs_lead_status": "Campaign Queue"}} for c in queued])
    log(f"   confirmed in Smartlead: {len(sent)} | not added: {len(failed)} (reasons in errors.csv, company stays in Public Pool) | "
        f"companies moved to Campaign Queue: {len(queued)}")

def main():
    if not CAMPAIGNS: log("CAMPAIGNS=<HubSpot campaign record IDs> is required"); sys.exit(2)
    log("DRY_RUN:", DRY_RUN, "| MIN_HOURS:", MIN_HOURS, "| NORMALIZE_NAMES:", NORMALIZE, "| BACKFILL:", BACKFILL)
    ensure_props()
    missing = []
    for c in batch_read(CAMPAIGN_OBJECT, CAMPAIGNS, ["campaign_name", "smartlead_campaign_id", "campaign_active", "iap_pipeline"]):
        cp, cn = c["properties"], c["properties"].get("campaign_name") or c["id"]
        if (cp.get("campaign_active") or "") != "true": log(f"{cn}: campaign is not active, skipped"); continue
        if (cp.get("iap_pipeline") or "") != "Active": log(f"{cn}: IAP Pipeline is not Active, skipped"); continue
        try:
            if BACKFILL: missing += backfill(c)
            else: run_campaign(c)
        except Exception as e:
            log(f"   error, campaign skipped: {type(e).__name__}: {str(e)[:200]}"); record_error(f"campaign {cn}", "", f"{type(e).__name__}: {e}")
    if BACKFILL:
        out = os.path.join(HERE, f"backfill_not_in_smartlead_{datetime.now():%Y-%m-%d}.csv")
        with open(out, "w", newline="") as fh:
            w = csv.writer(fh); w.writerow(["campaign", "smartlead_campaign_id", "contact_id", "name", "email", "pushed_date", "hubspot_link"]); w.writerows(missing)
        log(f"contacts marked as sent but not in Smartlead: {len(missing)} -> {out}")
    log("done")

if __name__ == "__main__": main()
