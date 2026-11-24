"""
enrich_contacts.py — contact waterfall for the contacts of the IAP-matched companies.

Scope:  every company in HubSpot whose Primary Campaign (iap_primary_campaign) is an active campaign
        (Campaign Active = Yes and IAP Pipeline = Active; only CAMPAIGN when it is set), AND Lead Status is
        Public Pool or Closed Lead (LEAD_STATUSES; Campaign Queue is skipped), AND Company Record Type = Seller Prospect,
        not Inactive, not Is Buyer.
        -> their associated contacts, minus retired = true and left_company = true, and only the MAX_PER_COMPANY (5)
        most senior per company (same ranking as push_to_smartlead.py: owner/founder, CEO/president, COO/CFO,
        other chiefs and VPs, directors, everyone else, board last).
        Errors are saved in errors.csv (run start, time, contact, HubSpot link, reason).
        A contact is processed when it was never enriched, was enriched more than REFRESH_DAYS (365) days ago,
        has an email Enrichley has not checked yet and has never really replied, has an Enrichley status that
        belongs to a different address, or has invalid_primary_email / invalid_primary_phone = true.
        Step 1 (web check) runs only for never / stale enriched contacts.

Per contact, in order:
  1. One Serper (Google) search for the person at the company, read by Claude Haiku. It answers three things:
       - still at the company? If the evidence says no -> left_company = true, enriched = true, stop.
       - current title -> written to jobtitle (web result wins over whatever is on the record).
       - LinkedIn URL -> written to hs_linkedin_url when found (web result wins; LinkedIn profiles are stale in this industry).
     Unknown (no evidence either way) = treated as still at the company; nothing is overwritten.
  2. Email.
     Enrichley's result is stored per address: enrichley_status (valid | invalid | unknown) + enrichley_checked_email
     (the address it refers to). A status only counts when enrichley_checked_email equals the current email; when
     they differ both are cleared and the new address is treated as never checked. An address with a status is
     never sent to Enrichley validation again until the contact is REFRESH_DAYS old. In this order:
       - enrichley_status = valid for this address -> skipped, no credit.
       - bounced (is_email_bounced, or Smartlead category with "bounce" / "wrong email" / "invalid email")
         -> invalid_primary_email = true, finder waterfall, no validation credit, enrichley_status left empty.
       - enrichley_status = invalid / unknown for this address -> not validated again; finder waterfall retried.
       - real reply (hs_sales_email_last_replied has a value, or os_last_reply_time has a value AND the Smartlead
         category is interested / not interested / wrong person) -> kept as valid, invalid_primary_email = false,
         no validation credit, enrichley_status left empty (Enrichley did not check it).
       - otherwise one Enrichley validation: ok / catch_all_validated -> valid, invalid_primary_email = false;
         invalid -> invalid; catch_all / unknown / anything else -> unknown; invalid and unknown set
         invalid_primary_email = true and run the finder waterfall. A failed call writes nothing (retried next run).
     No email at all -> the waterfall runs directly.
     Waterfall finds an Enrichley-valid email -> email written, enrichley_status = valid for it,
     invalid_primary_email = false. Nothing found -> invalid_primary_email = true.
       a. Enrichley Email Finder (name + domain + linkedin). Found = already verified, written as is.
       b. Findymail by name + domain  -> Enrichley validation -> written only if status is ok or catch_all_validated.
       c. Findymail by LinkedIn URL   -> Enrichley validation -> same rule. Skipped when the contact has no LinkedIn URL.
       d. CLAY_WEBHOOK_URL set: the contact is POSTed to the Clay table webhook (Clay runs its waterfall and writes
          Email back to HubSpot itself). Otherwise the email stays empty.
  3. Phone: runs when phone AND mobilephone are blank, OR invalid_primary_phone = true (set by the HubSpot workflow
     on call outcomes Left voicemail / No answer / Wrong number). Needs a LinkedIn URL.
     Findymail phone by LinkedIn URL (10 credits per number found) until PHONE_CREDIT_CAP credits are spent.
     A new number goes to mobilephone and clears invalid_primary_phone; the old number is kept in phone.
  4. enriched = true, enrichment_date, enrichment_status (one line saying what each step found).

Env:
  HUBSPOT_TOKEN, ENRICHLEY_API_KEY, FINDYMAIL_API_KEY, ANTHROPIC_API_KEY, SERPER_API_KEY   required
  CAMPAIGN=<campaign record id>              optional, e.g. 325749324474 (Baywater); empty = every qualifying company
  SEGMENT=<company segment ID>               optional: enrich the contacts of EVERY company in this HubSpot company segment
                                             instead of the campaign scope (no campaign / Lead Status / record type filter).
                                             Same steps as above. Use it with PHONE_CREDIT_CAP to also find phones.
  DRY_RUN=true        nothing is written to HubSpot, no paid call is made (providers are not called at all)
  LIMIT=1000          max contacts per run (default 1000)
  MAX_PER_COMPANY=5   contacts enriched per company, most senior first
  LEAD_STATUSES=Public Pool,Closed Lead   company Lead Status values that are enriched
  PHONE_CREDIT_CAP=0  Findymail phone credits allowed per run (0 = Findymail phone step off; 10 credits = 1 number)
  CLAY_WEBHOOK_URL=   optional Clay webhook source URL for step 2d
  SKIP_PHONE=true     skip step 3 entirely
  SERPER_API_KEY=     Google search via serper.dev ($1 per 1,000 queries)
  REFRESH_DAYS=365    re-enrich contacts whose enrichment_date is older than this
  MODEL=claude-haiku-5-5
  SETUP=true          create the missing contact properties, then exit
  WORKERS=10          contacts processed in parallel (1 = one at a time)
  PROGRESS_SECONDS=60 one progress line every this many seconds (counts so far + estimated time left)
  VERBOSE=true        also print the detail lines of every contact (off by default)
"""
import os, sys, re, json, csv, time, threading, requests
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
from datetime import datetime, timezone, timedelta

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
CAMPAIGN = os.getenv("CAMPAIGN", "").strip()
SEGMENT = os.getenv("SEGMENT", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
SETUP = os.getenv("SETUP", "false").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "1000"))
MAX_PER_COMPANY = int(os.getenv("MAX_PER_COMPANY", "5"))
LEAD_STATUSES = [x.strip() for x in os.getenv("LEAD_STATUSES", "Public Pool,Closed Lead").split(",") if x.strip()]
CAMPAIGN_OBJECT = "2-175770723"   # campaigns (telemarketing) custom object
SEARCH_PASS = 5000                # companies read per search pass; HubSpot search stops at 10,000 results
PHONE_CREDIT_CAP = int(os.getenv("PHONE_CREDIT_CAP", "0"))
CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL", "").strip()
SKIP_PHONE = os.getenv("SKIP_PHONE", "false").lower() == "true"
REFRESH_DAYS = int(os.getenv("REFRESH_DAYS", "365"))
WORKERS = max(1, int(os.getenv("WORKERS", "10")))
PROGRESS_SECONDS = max(5, int(os.getenv("PROGRESS_SECONDS", "60")))
VERBOSE = os.getenv("VERBOSE", "false").lower() == "true"
MODEL = os.getenv("MODEL", "claude-haiku-5-5")
SERPER = "https://google.serper.dev/search"

HS = "https://api.hubapi.com"; V = "2026-09"
HSH = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
ENRICHLEY_FINDER = "https://finder.enrichley.io/v1/find-email"
ENRICHLEY_VALIDATE = os.getenv("ENRICHLEY_VALIDATE_URL", "https://api.enrichley.io/api/v1/validate-single-email")
FINDYMAIL = "https://app.findymail.com/api"
LOCK = threading.Lock(); STOP = threading.Event(); _tl = threading.local()
class _Sessions:                      # one requests.Session per worker thread
    def __getattr__(self, k):
        s = getattr(_tl, "s", None)
        if s is None: s = _tl.s = requests.Session()
        return getattr(s, k)
S = _Sessions()

CONTACT_PROPS = ["firstname", "lastname", "email", "jobtitle", "hs_linkedin_url", "phone", "mobilephone",
                 "retired", "left_company", "enriched", "enrichment_date", "associatedcompanyid", "email_finder_status",
                 "is_email_bounced", "os_last_lead_category_name", "invalid_primary_email", "invalid_primary_phone",
                 "enrichley_status", "enrichley_checked_email", "hs_sales_email_last_replied", "os_last_reply_time", "enrichment_status"]
COMPANY_PROPS = ["name", "domain", "website", "linkedin_company_page", "active_inactive__c", "is_buyer"]
NEW_PROPS = [  # created by SETUP=true
    {"name": "enriched", "label": "Enriched", "type": "bool", "fieldType": "booleancheckbox", "groupName": "contactinformation",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}]},
    {"name": "enrichment_date", "label": "Enrichment Date", "type": "date", "fieldType": "date", "groupName": "contactinformation"},
    {"name": "enrichment_status", "label": "Enrichment Status", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
    {"name": "retired", "label": "retired", "type": "bool", "fieldType": "booleancheckbox", "groupName": "contactinformation",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}]},
    {"name": "left_company", "label": "left_company", "type": "bool", "fieldType": "booleancheckbox", "groupName": "contactinformation",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}]},
    {"name": "invalid_primary_email", "label": "invalid_primary_email", "type": "bool", "fieldType": "booleancheckbox", "groupName": "contactinformation",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}]},
    {"name": "invalid_primary_phone", "label": "invalid_primary_phone", "type": "bool", "fieldType": "booleancheckbox", "groupName": "contactinformation",
     "options": [{"label": "Yes", "value": "true"}, {"label": "No", "value": "false"}]},
    {"name": "enrichley_status", "label": "Enrichley status", "type": "enumeration", "fieldType": "select", "groupName": "contactinformation",
     "options": [{"label": "valid", "value": "valid"}, {"label": "invalid", "value": "invalid"}, {"label": "unknown", "value": "unknown"}]},
    {"name": "enrichley_checked_email", "label": "Enrichley checked email", "type": "string", "fieldType": "text", "groupName": "contactinformation"},
]
REAL_REPLY_CATEGORIES = ("interested", "not interested", "wrong person")   # Outbound Sync categories that count as a real reply
COST = {"searches": 0, "in_tokens": 0, "out_tokens": 0, "findymail_credits": 0, "enrichley_finder_found": 0, "enrichley_validations": 0}

ERRORS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "errors.csv")
RUN_STARTED = time.strftime("%Y-%m-%d %H:%M:%S")
def record_error(step, cid, error, name=""):
    """One row in errors.csv per error: run start, time, step, contact, HubSpot link, reason. Never stops the run."""
    try:
        with LOCK:
            new = not os.path.exists(ERRORS_CSV)
            with open(ERRORS_CSV, "a", newline="") as fh:
                w = csv.writer(fh)
                if new: w.writerow(["run_started", "date_time", "step", "contact_id", "contact_name", "hubspot_link", "error"])
                w.writerow([RUN_STARTED, time.strftime("%Y-%m-%d %H:%M:%S"), step, cid or "", name or "",
                            f"https://app.hubspot.com/contacts/243636394/record/0-1/{cid}" if cid else "", re.sub(r"\s+", " ", str(error))[:1000]])
    except Exception: pass

# seniority ranking, same as push_to_smartlead.py (1 = most senior, 7 = board)
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

def log(*a):
    """Outside the workers every line prints. Inside a worker the per-contact lines are dropped (VERBOSE=true keeps
    them, printed together when the contact is done); a line that stops the run always prints."""
    line = time.strftime("%H:%M:%S") + " " + " ".join(str(x) for x in a)
    buf = getattr(_tl, "buf", None)
    if buf is None: print(line, flush=True)
    elif VERBOSE: buf.append(line)
    elif any(k in line for k in ("stopping", "bad API key", "no credits")): print(line, flush=True)

N = {"li": 0, "email": 0, "phone": 0, "clay": 0, "left": 0, "invalid": 0, "phone_credits": 0, "done": 0,
     "chk_valid": 0, "chk_invalid": 0, "chk_unknown": 0, "chk_failed": 0, "notfound": 0, "errors": 0, "write_failed": 0, "retried": 0}
T = {}                                 # provider -> [calls, seconds]
def provider_failed():
    """Marks the contact being processed: a provider timed out or kept saying 'too many requests' after its retries."""
    _tl.err = True

def bump(k, n=1):
    with LOCK: N[k] += n
def timed(name):
    def deco(fn):
        def wrap(*a, **kw):
            t = time.time()
            try: return fn(*a, **kw)
            finally:
                with LOCK: c = T.setdefault(name, [0, 0.0]); c[0] += 1; c[1] += time.time() - t
        return wrap
    return deco

def cost(k, n=1):
    with LOCK: COST[k] += n

# ---------------------------------------------------------------- HubSpot
def hs(method, path, **kw):
    r = None
    for attempt in range(7):
        try: r = S.request(method, HS + path, headers=HSH, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code >= 400: raise requests.HTTPError(f"HubSpot said {r.status_code}: {r.text[:300]}", response=r)
        return r.json() if r.text else {}
    if r is None: raise requests.ConnectionError("HubSpot unreachable after 7 attempts")
    raise requests.HTTPError(f"HubSpot still busy or down after 7 tries ({r.status_code}): {r.text[:300]}", response=r)

def setup_properties():
    have = {p["name"] for p in hs("GET", "/crm/v3/properties/contacts").get("results", [])}
    for p in NEW_PROPS:
        if p["name"] in have: log("exists:", p["name"]); continue
        hs("POST", "/crm/v3/properties/contacts", json=p); log("created:", p["name"])
    missing = [p["name"] for p in NEW_PROPS if p["name"] not in have]
    log("setup done." if missing else "all properties already existed.")

def check_properties():
    have = {p["name"] for p in hs("GET", "/crm/v3/properties/contacts").get("results", [])}
    missing = [p["name"] for p in NEW_PROPS if p["name"] not in have]
    if missing: log("missing contact properties:", ", ".join(missing), "- run with SETUP=true first"); sys.exit(2)

def active_campaigns():
    """{campaign record id: name} for campaigns with Campaign Active = Yes and IAP Pipeline = Active."""
    out, after = {}, None
    while True:
        body = {"filterGroups": [{"filters": [{"propertyName": "campaign_active", "operator": "EQ", "value": "true"},
                                              {"propertyName": "iap_pipeline", "operator": "EQ", "value": "Active"}]}],
                "properties": ["campaign_name"], "limit": 200}
        if after: body["after"] = after
        res = hs("POST", f"/crm/v3/objects/{CAMPAIGN_OBJECT}/search", json=body)
        for r in res.get("results", []): out[str(r["id"])] = (r["properties"].get("campaign_name") or "").strip()
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return out

def campaign_companies():
    """Companies to enrich: primary campaign is an active campaign, Lead Status in LEAD_STATUSES, Seller Prospect,
    not Inactive, not Is Buyer. Read in passes of SEARCH_PASS, each starting after the last record ID of the one before."""
    camps = active_campaigns()
    if CAMPAIGN: camps = {k: v for k, v in camps.items() if k == CAMPAIGN}
    log(f"active campaigns ({len(camps)}): " + ("; ".join(sorted(camps.values())) or "none"))
    if not camps: return {}
    base = [{"propertyName": "iap_primary_campaign", "operator": "IN", "values": list(camps)},
            {"propertyName": "hs_lead_status", "operator": "IN", "values": LEAD_STATUSES},
            {"propertyName": "company_record_type", "operator": "EQ", "value": "Seller Prospect"}]
    rows, last = [], 0
    while True:
        got, after = 0, None
        while got < SEARCH_PASS:
            body = {"filterGroups": [{"filters": base + [{"propertyName": "hs_object_id", "operator": "GT", "value": str(last)}]}],
                    "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}], "properties": COMPANY_PROPS, "limit": 200}
            if after: body["after"] = after
            res = hs("POST", f"/crm/objects/{V}/companies/search", json=body)
            page = res.get("results", []); rows += page; got += len(page)
            after = res.get("paging", {}).get("next", {}).get("after")
            if not after or not page: break
        if not after or not rows: break
        last = max(int(r["id"]) for r in rows)
    out = {r["id"]: r["properties"] for r in rows
           if (r["properties"].get("active_inactive__c") or "") != "Inactive" and str(r["properties"].get("is_buyer") or "").lower() != "true"}
    if len(out) != len({r["id"] for r in rows}): log(f"left out {len({r['id'] for r in rows}) - len(out)} companies that are Inactive or Is Buyer")
    return out

def segment_companies():
    """Every company in the HubSpot company segment SEGMENT (GET /crm/v3/lists/{id}/memberships, 250 per page)."""
    info = hs("GET", f"/crm/v3/lists/{SEGMENT}").get("list", {})
    if str(info.get("objectTypeId")) != "0-2":
        log(f"segment {SEGMENT} is not a company segment (object type {info.get('objectTypeId')}), stopping"); sys.exit(2)
    log(f"segment {SEGMENT}: {info.get('name')} ({info.get('size')} companies according to HubSpot)")
    ids, after = [], None
    while True:
        q = "?limit=250" + (f"&after={after}" if after else "")
        res = hs("GET", f"/crm/v3/lists/{SEGMENT}/memberships{q}")
        ids += [str(r["recordId"]) for r in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    out = {}
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/companies/batch/read", json={"properties": COMPANY_PROPS, "inputs": [{"id": c} for c in ids[i:i+100]]})
        for r in res.get("results", []): out[str(r["id"])] = r["properties"]
    return out

def contacts_of(company_ids):
    """company id -> [contact ids] via the v4 associations batch read."""
    out = {}
    ids = list(company_ids)
    for i in range(0, len(ids), 100):
        res = hs("POST", "/crm/v4/associations/companies/contacts/batch/read", json={"inputs": [{"id": c} for c in ids[i:i+100]]})
        for r in res.get("results", []):
            out[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to", [])]
    return out

def read_contacts(ids):
    rows = []
    ids = list(ids)
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/contacts/batch/read", json={"properties": CONTACT_PROPS, "inputs": [{"id": c} for c in ids[i:i+100]]})
        rows += res.get("results", [])
    return rows

def write(cid, props, on_dup=None):
    log("   write" if not DRY_RUN else "   (dry) would write", json.dumps(props))
    if DRY_RUN or not props: return
    try:
        hs("PATCH", f"/crm/objects/{V}/contacts/{cid}", json={"properties": props}); return
    except requests.HTTPError as e:
        txt = e.response.text if e.response is not None else str(e)
        if "email" in props and re.search(r"already exists|existing id|conflict", txt, re.I):
            m = re.search(r"(?:Existing ID|ID)[:\s]+(\d{6,})", txt)
            dup = m.group(1) if m else "another contact"
            log(f"   email {props['email']} already belongs to contact {dup}: writing the rest without it")
            props = {k: v for k, v in props.items() if k not in ("email", "email_finder_status", "enrichley_status", "enrichley_checked_email")}
            props.update(on_dup or {})          # Enrichley result of the address that stays on the record
            props["invalid_primary_email"] = "true"
            props["enrichment_status"] = (props.get("enrichment_status", "") + f"; replacement email already on contact {dup} (duplicate)")[:250]
            try: hs("PATCH", f"/crm/objects/{V}/contacts/{cid}", json={"properties": props}); return
            except requests.HTTPError as e2: txt = e2.response.text if e2.response is not None else str(e2)
        bump("write_failed"); log(f"   HubSpot write failed, contact skipped: {txt[:300]}")
        record_error("HubSpot save", cid, f"HubSpot refused the update: {txt[:600]}")

# ---------------------------------------------------------------- providers
@timed("enrichley finder")
def enrichley_find(first, last, domain, linkedin):
    """Enrichley Email Finder. Returns (status, email): status valid | not_found | error. Found emails are already verified."""
    payload = {"full_name": f"{first} {last}".strip(), "first_name": first, "last_name": last}
    if domain: payload["domain"] = domain
    if linkedin: payload["linkedin_profile_url"] = linkedin
    h = {"Authorization": f"Bearer {os.environ['ENRICHLEY_API_KEY']}", "Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(5):
        try: r = S.post(ENRICHLEY_FINDER, headers=h, json=payload, timeout=90)
        except requests.RequestException as e:
            log(f"   network error ({type(e).__name__}), retrying"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 200:
            d = r.json()
            if d.get("valid") and d.get("email"): cost("enrichley_finder_found", 1); return "valid", d["email"]
            return "not_found", None
        if r.status_code == 429: bump("retried"); time.sleep(int(r.headers.get("Retry-After", "5"))); continue
        if r.status_code in (503, 504): time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 402: log("Enrichley: out of credits, stopping"); sys.exit(2)
        if r.status_code == 401: log("Enrichley: bad API key"); sys.exit(2)
        log(f"   Enrichley finder {r.status_code}: {r.text[:200]}"); provider_failed(); return "error", None
    provider_failed(); return "error", None

@timed("enrichley check")
def enrichley_validate(email):
    """Enrichley validation. Returns "valid" (ok / catch_all_validated), "invalid" (invalid),
    "unknown" (catch_all, unknown, anything else = not verified), or None when the check itself failed
    (network or API error): None is never recorded."""
    k = os.environ['ENRICHLEY_API_KEY']
    h = {"X-API-Key": k, "Authorization": f"Bearer {k}", "Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(4):
        try: r = S.post(ENRICHLEY_VALIDATE, headers=h, json={"email": email}, timeout=120)
        except requests.RequestException as e:
            log(f"   network error ({type(e).__name__}), retrying"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 200:
            cost("enrichley_validations", 1)
            d = r.json(); status = str(d.get("status") or d.get("result") or "").lower()
            if status in ("ok", "catch_all_validated") or (not status and d.get("valid") is True): res = "valid"
            elif status == "invalid": res = "invalid"
            else: res = "unknown"
            log(f"   Enrichley validate {email}: {status or d} -> {res}"); return res
        if r.status_code in (429, 503, 504): bump("retried"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 402: log("Enrichley: out of credits, stopping"); sys.exit(2)
        if r.status_code in (401, 403): log(f"Enrichley validate {r.status_code}: {r.text[:200]} - stopping so no email is mislabelled"); sys.exit(2)
        log(f"   Enrichley validate {r.status_code}: {r.text[:200]}"); provider_failed(); return None
    provider_failed(); return None

@timed("findymail")
def findymail(path, body):
    h = {"Authorization": f"Bearer {os.environ['FINDYMAIL_API_KEY']}", "Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(4):
        try: r = S.post(f"{FINDYMAIL}/{path}", headers=h, json=body, timeout=120)
        except requests.RequestException as e:
            log(f"   network error ({type(e).__name__}), retrying"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 200: return r.json()
        if r.status_code in (429, 503, 504): bump("retried"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 402: log("Findymail: out of credits, stopping"); sys.exit(2)
        if r.status_code == 401: log("Findymail: bad API key"); sys.exit(2)
        log(f"   Findymail {path} {r.status_code}: {r.text[:200]}"); provider_failed(); return {}
    provider_failed(); return {}

def findymail_email(name=None, domain=None, linkedin=None):
    d = findymail("search/linkedin", {"linkedin_url": linkedin}) if linkedin else findymail("search/name", {"name": name, "domain": domain})
    c = d.get("contact") or {}
    email = c.get("email") if isinstance(c, dict) else None
    if email: cost("findymail_credits", 1)
    return email

def findymail_phone(linkedin):
    d = findymail("search/phone", {"linkedin_url": linkedin})
    c = d.get("contact") or {}
    phones = c.get("phones") or c.get("phone_numbers") or ([c["phone"]] if c.get("phone") else [])
    if isinstance(phones, list) and phones:
        p = phones[0]; num = p.get("number") if isinstance(p, dict) else p
        if num: cost("findymail_credits", 10); return str(num)
    return None

# ---------------------------------------------------------------- Serper search + Claude read
def serper(q, num=10):
    """One Google query via serper.dev. Returns list of {title, link, snippet}."""
    h = {"X-API-KEY": os.environ["SERPER_API_KEY"], "Content-Type": "application/json"}
    for attempt in range(4):
        try: r = S.post(SERPER, headers=h, json={"q": q, "num": num}, timeout=30)
        except requests.RequestException as e:
            log(f"   network error ({type(e).__name__}), retrying"); time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 200:
            cost("searches", 1)
            return [{"title": o.get("title", ""), "link": o.get("link", ""), "snippet": o.get("snippet", "")}
                    for o in r.json().get("organic", [])]
        if r.status_code in (429, 503, 504): bump("retried"); time.sleep(3 * (attempt + 1)); continue
        if r.status_code in (401, 403): log("Serper: bad API key or no credits"); sys.exit(2)
        log(f"   Serper {r.status_code}: {r.text[:200]}"); provider_failed(); return []
    provider_failed(); return []

def claude_json(prompt):
    """Claude reads text already fetched; no tools. Returns the JSON object in its answer or {}."""
    import anthropic
    client = anthropic.Anthropic(timeout=90, max_retries=3)
    try: msg = client.messages.create(model=MODEL, max_tokens=400, messages=[{"role": "user", "content": prompt}])
    except Exception as e: log(f"   Claude error ({type(e).__name__}), skipped"); provider_failed(); return {}
    u = msg.usage; cost("in_tokens", u.input_tokens); cost("out_tokens", u.output_tokens)
    text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    m = re.search(r"\{.*\}", text, re.S)
    try: return json.loads(m.group(0)) if m else {}
    except json.JSONDecodeError: return {}

@timed("web check")
def verify_person(name, company, domain, title_on_record, linkedin_on_record):
    """One Serper query + one Haiku read. Returns dict: at_company (True|False|None), title, linkedin, evidence."""
    out = {"at_company": None, "title": None, "linkedin": None, "evidence": ""}
    short = re.sub(r"\s*\(.*?\)\s*$", "", company).strip()
    short = re.sub(r"\s+dba\s+.*$", "", short, flags=re.I).strip()
    short = re.sub(r"[,.]?\s*\b(LLC|L\.L\.C\.|Inc\.?|Incorporated|Corp\.?|Corporation|Co\.?|Company|Ltd\.?|LP)\.?$", "", short, flags=re.I).strip(" ,.") or company
    results = serper(f'"{name}" {short} {domain}')
    if not results: results = serper(f'"{name}" {short} site:linkedin.com/in')
    if not results: return out
    text = "\n".join(f"- {r['title']} | {r['link']} | {r['snippet']}" for r in results)
    d = claude_json(
        f'Person: "{name}". Company: "{company}" ({domain}). Title on file: "{title_on_record or "unknown"}". '
        f'LinkedIn on file: "{linkedin_on_record or "none"}".\n\nGoogle results:\n{text}\n\n'
        f'Answer only with JSON: {{"at_company": true|false|null, "title": "<current title or empty>", '
        f'"linkedin_url": "<linkedin.com/in/... or empty>", "evidence": "<the result line you relied on>"}}. '
        f'at_company is false ONLY if a result shows this exact person has left, retired, or now works elsewhere. '
        f'at_company is true ONLY if a result shows this exact person currently at this company. Otherwise null. '
        f'title is the current title at this company as a result states it, else empty. '
        f'linkedin_url only if a result shows this exact person name AND this company; else empty.')
    if d.get("at_company") is True: out["at_company"] = True
    elif d.get("at_company") is False: out["at_company"] = False
    t = (d.get("title") or "").strip()
    if t and len(t) <= 100: out["title"] = t
    u = (d.get("linkedin_url") or "").strip()
    if "linkedin.com/in/" in u: out["linkedin"] = u.split("?")[0].rstrip("/")
    out["evidence"] = str(d.get("evidence") or "")[:200]
    return out

def existing_email_invalid(p):
    """(invalid: bool, reason). HubSpot bounce flag or Smartlead bounce / wrong-email category. No provider call."""
    raw = p.get("os_last_lead_category_name") or ""
    cat = re.sub(r"[\s_\-]+", " ", raw.lower()).strip()     # "Wrong-Email" / "wrong_email" -> "wrong email"
    if (p.get("is_email_bounced") or "").lower() == "true": return True, "hubspot bounce flag"
    if "bounce" in cat or "wrong email" in cat or "invalid email" in cat: return True, f"smartlead category: {raw}"
    return False, ""

def real_reply(p):
    """(replied: bool, reason). A sequence reply (date only) counts. An Outbound Sync reply counts only with a
    real-reply category, so out of office and bounces never count."""
    if (p.get("hs_sales_email_last_replied") or "").strip(): return True, "sequence reply"
    cat = re.sub(r"[\s_\-]+", " ", (p.get("os_last_lead_category_name") or "").lower()).strip()
    if (p.get("os_last_reply_time") or "").strip() and cat in REAL_REPLY_CATEGORIES: return True, f"outbound sync: {cat}"
    return False, ""

def enrichley_state(p):
    """Enrichley status for the CURRENT email: counts only when enrichley_checked_email equals the email."""
    email = (p.get("email") or "").strip().lower(); checked = (p.get("enrichley_checked_email") or "").strip().lower()
    st = (p.get("enrichley_status") or "").strip().lower()
    if st in ("valid", "invalid", "unknown") and email and checked == email: return st
    if email and not st and not checked: return sentence_state(p)
    return ""

def sentence_state(p):
    """Enrichley result written only as a sentence in enrichment_status by the versions before enrichley_status
    existed: "valid" when Enrichley validated or found the email now on the record, "invalid" when it rejected it
    and no replacement was written (those versions did not separate invalid from unknown), else ""."""
    s = (p.get("enrichment_status") or "").lower()
    if not s or "replacement email already on contact" in s: return ""
    if re.search(r"email (valid|replaced): [^;]*enrichley|email: (enrichley finder|findymail (name|linkedin) \+ enrichley)", s): return "valid"
    if "email invalid: enrichley" in s: return "invalid"
    return ""

def enrichley_orphan(p):
    """True when an Enrichley status / checked address is stored but does not belong to the current email."""
    return bool((p.get("enrichley_status") or p.get("enrichley_checked_email")) and not enrichley_state(p))

# ---------------------------------------------------------------- main
def main():
    if SETUP: setup_properties(); return
    check_properties()
    log("DRY_RUN:", DRY_RUN, "| SEGMENT:", SEGMENT or "-", "| CAMPAIGN:", CAMPAIGN, "| LIMIT:", LIMIT, "| PHONE_CREDIT_CAP:", PHONE_CREDIT_CAP, "| CLAY:", bool(CLAY_WEBHOOK_URL), "| WORKERS:", WORKERS)
    if SEGMENT: companies = segment_companies(); log(f"companies to enrich (all companies in segment {SEGMENT}): {len(companies)}")
    else: companies = campaign_companies(); log(f"companies to enrich (active primary campaign, Lead Status {' or '.join(LEAD_STATUSES)}, Seller Prospect): {len(companies)}")
    assoc = contacts_of(companies); cids = {c for v in assoc.values() for c in v}
    log(f"associated contacts: {len(cids)}")
    rows = read_contacts(cids)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)).strftime("%Y-%m-%d")
    def stale(p):
        if (p.get("enriched") or "").lower() != "true": return True
        d = (p.get("enrichment_date") or "")[:10]
        return not d or d < cutoff          # never enriched, no date, or older than REFRESH_DAYS
    def needs_phone(p):
        """Phone step wanted: phone step is on, and the number is flagged bad or both phone fields are blank
        (unless an earlier run already found no number or had no LinkedIn URL)."""
        if SKIP_PHONE or PHONE_CREDIT_CAP <= 0: return False
        if (p.get("invalid_primary_phone") or "").lower() == "true": return True
        st = (p.get("enrichment_status") or "").lower()
        return not (p.get("phone") or p.get("mobilephone")) and "phone: not found" not in st and "phone: no linkedin" not in st
    def in_scope(p):
        if (p.get("retired") or "").lower() == "true": return False
        if (p.get("left_company") or "").lower() == "true": return False
        return full_scope(p) or needs_phone(p)
    def full_scope(p):
        if stale(p): return True
        if enrichley_orphan(p): return True                                         # status belongs to another address
        if not (p.get("enrichley_status") or "").strip() and enrichley_state(p) == "valid": return True   # copy the old sentence result into enrichley_status (no credit)
        if (p.get("email") or "").strip() and not enrichley_state(p) and not real_reply(p)[0]: return True   # never checked by Enrichley
        # invalid / not-found emails are NOT retried every day: they wait REFRESH_DAYS like any finished contact
        return False
    def needs_check(p):
        if not (p.get("email") or "").strip(): return False
        if not stale(p) and enrichley_state(p): return False
        return not existing_email_invalid(p)[0] and not real_reply(p)[0]
    by_id = {r["id"]: r for r in rows}
    alive = lambda p: (p.get("retired") or "").lower() != "true" and (p.get("left_company") or "").lower() != "true"
    top = set()                       # the MAX_PER_COMPANY most senior contacts of each company
    for comp, cs in assoc.items():
        cand = sorted((c for c in cs if c in by_id and alive(by_id[c]["properties"])),
                      key=lambda c: (rank(by_id[c]["properties"].get("jobtitle")), int(c)))
        top.update(cand[:MAX_PER_COMPANY])
    log(f"contacts kept (at most {MAX_PER_COMPANY} per company, most senior first): {len(top)} of {len(rows)}")
    rows = [r for r in rows if r["id"] in top]
    todo = [r for r in rows if in_scope(r["properties"])]
    log(f"to process: {len(todo)} (already done within {REFRESH_DAYS} days and nothing to retry: {len(rows) - len(todo)}); this run takes up to {LIMIT}")
    todo = todo[:LIMIT]
    log(f"existing emails that go to Enrichley validation in this run: up to {sum(1 for r in todo if full_scope(r['properties']) and needs_check(r['properties']))} (1 check each)")
    log(f"contacts that only need a phone (already enriched, email not touched): {sum(1 for r in todo if not full_scope(r['properties']))}")
    comp_of = {c: k for k, v in assoc.items() for c in v}
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = len(todo); t0 = time.time()
    def progress():
        with LOCK: n = dict(N); t = {k: v[1] / v[0] for k, v in T.items() if v[0]}
        mins = (time.time() - t0) / 60; left = f"{mins / n['done'] * (total - n['done']):.0f} min left" if n["done"] else "time left: not known yet"
        avg = ", ".join(f"{k} {v:.0f}s" for k, v in sorted(t.items())) or "-"
        return (f"{time.strftime('%H:%M:%S')} progress: {n['done']}/{total} contacts in {mins:.0f} min, {left} | "
                f"Enrichley checks: valid {n['chk_valid']}, invalid {n['chk_invalid']}, unknown {n['chk_unknown']}, failed {n['chk_failed']} | "
                f"emails found/replaced {n['email']}, not found {n['notfound']} | left company {n['left']} | phones {n['phone']} | "
                f"errors {n['errors'] + n['write_failed']} | provider retries {n['retried']} | avg per call: {avg}")
    def reserve_phone():              # 10 Findymail credits per lookup, never above PHONE_CREDIT_CAP across workers
        with LOCK:
            if N["phone_credits"] + 10 > PHONE_CREDIT_CAP: return False
            N["phone_credits"] += 10; return True

    RETRY = []                        # contacts where a provider timed out / said too many requests: tried again at the end
    def process(r, second=False):
        if STOP.is_set(): return
        _tl.buf = []; _tl.err = False
        try:
            p = r["properties"]; cid = r["id"]; co = companies.get(comp_of.get(cid) or p.get("associatedcompanyid") or "", {})
            first, last = (p.get("firstname") or "").strip(), (p.get("lastname") or "").strip()
            name = f"{first} {last}".strip(); company = co.get("name") or ""; domain = (co.get("domain") or "").lower().strip()
            linkedin = (p.get("hs_linkedin_url") or "").strip(); email = (p.get("email") or "").strip()
            log(f"{name} | {company} | {domain} | li={'yes' if linkedin else 'no'} | email={'yes' if email else 'no'}")
            upd = {}; status = []; on_dup = None
            phone_only = not full_scope(p)      # already enriched: only the phone step runs, email and web check are skipped
            if not name or not domain:
                write(cid, {"enriched": "true", "enrichment_date": now, "enrichment_status": "skipped: no name or company domain"}); return

            # 1. Still at the company? + title + LinkedIn (one web search)
            if not DRY_RUN and stale(p) and not phone_only:
                v = verify_person(name, company, domain, p.get("jobtitle"), linkedin)
                log(f"   web: at_company={v['at_company']} title={v['title'] or '-'} linkedin={v['linkedin'] or '-'}")
                if v["at_company"] is False:
                    write(cid, {"left_company": "true", "enriched": "true", "enrichment_date": now,
                                "enrichment_status": f"left company: {v['evidence']}"[:250]}); bump("left"); return
                status.append("at company" if v["at_company"] else "at company: unknown")
                if v["title"] and v["title"] != (p.get("jobtitle") or "").strip(): upd["jobtitle"] = v["title"]; status.append("title updated")
                if v["linkedin"] and v["linkedin"] != linkedin: linkedin = v["linkedin"]; upd["hs_linkedin_url"] = linkedin; bump("li"); status.append("linkedin updated")

            # 2. Email
            flag = (p.get("invalid_primary_email") or "").lower()
            est = "" if stale(p) else enrichley_state(p)      # Enrichley result for THIS address; checked again once REFRESH_DAYS old
            if enrichley_orphan(p):                           # stored status belongs to another address: clear it
                upd["enrichley_status"] = ""; upd["enrichley_checked_email"] = ""
            need_email = False
            if phone_only: pass
            elif not email:
                need_email = True
            elif est == "valid":
                status.append("email valid: enrichley (checked before)")
                if flag != "false": upd["invalid_primary_email"] = "false"
                if not (p.get("enrichley_status") or "").strip() and not stale(p):      # result came from the old sentence: store it properly
                    upd["enrichley_status"] = "valid"; upd["enrichley_checked_email"] = email
            else:
                bad, why = existing_email_invalid(p)
                replied, rwhy = real_reply(p)
                if bad:          # bounce or wrong-email tag: no credit spent on a dead address
                    log(f"   existing email {email}: INVALID ({why})")
                    upd["invalid_primary_email"] = "true"; status.append(f"email invalid: {why}"); need_email = True; bump("invalid")
                    upd["enrichley_status"] = "invalid"; upd["enrichley_checked_email"] = email      # never left empty: a bounced address counts as invalid
                elif est:        # invalid / unknown already recorded for this address: never validated twice
                    if flag != "true": upd["invalid_primary_email"] = "true"
                    status.append(f"email {est}: enrichley (checked before)"); need_email = True
                    if not (p.get("enrichley_status") or "").strip(): upd["enrichley_status"] = est; upd["enrichley_checked_email"] = email   # result came from the old sentence
                elif replied:    # a real reply proves the address: no credit spent
                    if flag != "false": upd["invalid_primary_email"] = "false"
                    status.append(f"email: replied, not checked ({rwhy})")
                    upd["enrichley_status"] = "valid"; upd["enrichley_checked_email"] = email        # a real reply counts as valid
                elif DRY_RUN:
                    status.append("email: would validate with enrichley")
                elif (res := enrichley_validate(email)) is None:
                    status.append("email: validation failed, unchanged"); bump("chk_failed")
                else:
                    upd["enrichley_status"] = res; upd["enrichley_checked_email"] = email; bump("chk_" + res)
                    if res == "valid":
                        upd["invalid_primary_email"] = "false"; upd["email_finder_status"] = "valid"; status.append("email valid: enrichley")
                    else:        # invalid or unknown
                        upd["invalid_primary_email"] = "true"; status.append(f"email {res}: enrichley"); need_email = True; bump("invalid")
            if need_email and not DRY_RUN:
                st, em = enrichley_find(first, last, domain, linkedin); log(f"   enrichley finder: {st} {em or ''}")
                src_ = None; found = None
                def same_company(e):
                    d = e.split("@")[-1].lower().removeprefix("www."); c = domain.removeprefix("www.")
                    return d == c or d.endswith("." + c) or c.endswith("." + d)
                if em and email and em.lower() == email.lower():      # finder independently confirms the current address
                    found = email; src_ = "confirmed by enrichley finder"
                elif em: found = em; src_ = "enrichley finder"
                if not found:
                    em = findymail_email(name=name, domain=domain); log(f"   findymail name: {em or 'not found'}")
                    if em and em.lower() != email.lower() and enrichley_validate(em) == "valid": found = em; src_ = "findymail name + enrichley"
                if not found and linkedin:
                    em = findymail_email(linkedin=linkedin); log(f"   findymail linkedin: {em or 'not found'}")
                    if em and not same_company(em):
                        status.append(f"linkedin email at another domain: {em} (may have left)")
                    elif em and em.lower() != email.lower() and enrichley_validate(em) == "valid": found = em; src_ = "findymail linkedin + enrichley"
                if found and found == email:
                    upd["email_finder_status"] = "valid"; upd["invalid_primary_email"] = "false"; status.append(f"email valid: {src_}")
                    upd["enrichley_status"] = "valid"; upd["enrichley_checked_email"] = email
                elif found:
                    on_dup = {k: upd[k] for k in ("enrichley_status", "enrichley_checked_email") if k in upd}   # result of the old address
                    upd["enrichley_status"] = "valid"; upd["enrichley_checked_email"] = found
                    email = found; upd["email"] = found; upd["email_finder_status"] = "valid"; upd["invalid_primary_email"] = "false"
                    bump("email"); status.append(f"email replaced: {src_}" if p.get("email") else f"email: {src_}")
                elif CLAY_WEBHOOK_URL:
                    S.post(CLAY_WEBHOOK_URL, json={"hubspot_contact_id": cid, "first_name": first, "last_name": last, "full_name": name,
                                                   "company": company, "domain": domain, "linkedin_url": linkedin}, timeout=30)
                    bump("clay"); status.append("email: sent to clay")
                else: status.append("email: no replacement found" if p.get("email") else "email: not found"); bump("notfound")
                if not found: upd["invalid_primary_email"] = "true"

            # 3. Phone
            phone_bad = (p.get("invalid_primary_phone") or "").lower() == "true"
            need_phone = phone_bad or not (p.get("phone") or p.get("mobilephone"))
            if not SKIP_PHONE and not DRY_RUN and need_phone:
                if not linkedin: status.append("phone: no linkedin, skipped")
                elif not reserve_phone(): status.append("phone: credit cap reached")
                else:
                    ph = findymail_phone(linkedin)
                    current = {re.sub(r"\D", "", p.get("phone") or ""), re.sub(r"\D", "", p.get("mobilephone") or "")}
                    if ph and re.sub(r"\D", "", ph) not in current:
                        upd["mobilephone"] = ph; bump("phone")
                        if phone_bad: upd["invalid_primary_phone"] = "false"
                        status.append("phone replaced: findymail" if phone_bad else "phone: findymail")
                    elif ph: status.append("phone: findymail returned the same number")
                    else: bump("phone_credits", -10); status.append("phone: not found")      # nothing found = no credits used
                    log(f"   phone: {ph or 'not found'}")

            if getattr(_tl, "err", False) and not DRY_RUN:   # a provider failed: save what was found, but do not mark it enriched
                status.append("provider timeout / too many requests: " + ("not done, picked up next run" if second else "tried again at the end of this run"))
                upd["enrichment_status"] = (((p.get("enrichment_status") or "") + "; ") if phone_only else "") + "; ".join(status)
                upd["enrichment_status"] = upd["enrichment_status"].strip("; ")[:250]
                write(cid, upd, on_dup)
                if second: record_error("enrichment", cid, "a provider timed out or said too many requests twice; contact left for the next run", name)
                else:
                    with LOCK: RETRY.append(r)
                return
            if phone_only:     # keep the earlier email result and enrichment date; add the phone result
                upd["enrichment_status"] = ((p.get("enrichment_status") or "") + "; " + ("; ".join(status) or "dry run")).strip("; ")[:250]
            else: upd.update({"enriched": "true", "enrichment_date": now, "enrichment_status": ("; ".join(status) or "dry run")[:250]})
            write(cid, upd, on_dup)

        except Exception as e:
            bump("errors"); log(f"   error on this contact ({type(e).__name__}: {str(e)[:200]}), skipped")
            record_error("enrichment", r.get("id"), f"{type(e).__name__}: {e}",
                         f"{(r['properties'].get('firstname') or '')} {(r['properties'].get('lastname') or '')}".strip())
        finally:
            buf, _tl.buf = _tl.buf, None
            with LOCK:
                N["done"] += 1
                if buf: print("\n".join(buf), flush=True)

    exit_code = 0; finished = threading.Event()
    def ticker():
        while not finished.wait(PROGRESS_SECONDS): print(progress(), flush=True)
    threading.Thread(target=ticker, daemon=True).start()
    ex = ThreadPoolExecutor(max_workers=WORKERS)
    futs = [ex.submit(process, r) for r in todo]
    try:
        for f in as_completed(futs):
            try: f.result()
            except CancelledError: pass
            except SystemExit as e:           # a provider is out of credits or a key is bad: stop everything
                STOP.set(); exit_code = e.code if isinstance(e.code, int) and e.code else 2
                for g in futs: g.cancel()
    except KeyboardInterrupt:
        STOP.set()
        for g in futs: g.cancel()
        log("interrupted: finishing the contacts already in progress, then stopping"); exit_code = 130
    ex.shutdown(wait=True)
    if RETRY and not STOP.is_set():
        log(f"second try for {len(RETRY)} contacts where a provider timed out or said too many requests (after a 60-second pause)")
        time.sleep(60); again = list(RETRY); total += len(again)
        with ThreadPoolExecutor(max_workers=max(1, WORKERS // 2)) as ex2:
            for f in as_completed([ex2.submit(process, r, True) for r in again]):
                try: f.result()
                except (CancelledError, SystemExit): STOP.set()
    finished.set(); print(progress(), flush=True)

    est = COST["searches"] * 0.001
    log(f"done. left company {N['left']} | linkedin {N['li']} | invalid emails {N['invalid']} | emails found/replaced {N['email']} | sent to clay {N['clay']} | phones {N['phone']}")
    log(f"cost: serper searches {COST['searches']} (= ${est:.2f}) + tokens in {COST['in_tokens']} out {COST['out_tokens']} | "
        f"Findymail credits {COST['findymail_credits']} | Enrichley finder found {COST['enrichley_finder_found']}, validations {COST['enrichley_validations']}")
    if exit_code: sys.exit(exit_code)

if __name__ == "__main__": main()
