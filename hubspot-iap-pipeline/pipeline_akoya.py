"""
pipeline_akoya.py — one-off variant of pipeline.py for the Akoya campaigns. pipeline.py is unchanged.

  Scope:    segment 908 members whose grata_research_tag is one of GRATA_TAGS (default "akoya - alinabal", "akoya - h&w"),
            optionally narrowed by LEAD_STATUSES and NO_LAST_CONTACT.
  IAPs:     only the campaigns in ONLY_IAPS (default the two Akoya campaigns 198888772293, 198892371671).
  Ticks:    ticks of every OTHER campaign on a company are kept; only the Akoya ticks, scores and reasons are replaced.
  Primary:  an existing non-Akoya iap_primary_campaign is kept; otherwise the Akoya pick (priority, then fewest matches).
  Contacts: when both stages are finished, contact-enrichment-worker/enrich_contacts.py runs once per Akoya campaign
            (RUN_CONTACTS=false skips it; CONTACT_LIMIT caps contacts per campaign, default 50; CONTACT_WORKER = path).

HubSpot IAP pipeline — two matching passes in one run.

  STAGE 1 (cheap, Haiku, Batch API): match companies in SCOPE against every active IAP using the
           description already on the record. Ticks enrichment_iap_campaigns.
  A company whose stage status is Failed is picked up again on the next run, in either stage.
  STAGE 2 (deep, Haiku, Batch API): companies STAGE 1 ticked, plus companies with no description (graded against
           every active IAP, since stage 1 had nothing to read). Crawls the website,
           rewrites enrichment_description + enrichment_keywords, grades EVERY active IAP 1-5 with a
           reason, REPLACES the tick set with every IAP scored KEEP_MIN (default 3) or more, writes enrichment_iap_scores and
           iap_primary_campaign: among the ticked IAPs, the best campaign_priority (1 beats 5) wins; a tie goes to the campaign
           with fewer matched companies in HubSpot at that moment (counts include the ticks made during this run).
           Also cleans the record: website is checked (loads + matches the company name/description);
           if dead or wrong, Claude web search finds the right one and website + domain are overwritten.
           LinkedIn Company Page is written only when the company's own site links to it or search
           confirms name + domain; if none is confirmed the existing value is left alone.
           Phone Number is filled only when blank, from the site (tel: links, contact page), checked
           with the phonenumbers library. Sets enriched = true when done.
           Headquarters: the real HQ country from the site is written to Country; if it is outside the US and
           Canada (HQ_ALLOWED), every campaign is unticked. HQ city and state from the site are written to City and
           State/Region only when those are blank.
  Stage 1 also sends city, state, country and number of employees so geography and size criteria are applied;
  a missing value never rules a company out. Companies with no description are matched from their website text
  (stored, or crawled during stage 1 for free); only dead sites are marked Done without a match.

RESEARCH_TAG=<value>  -> only companies whose Research Tags (research_tags__c) contain that value.
                         Implies RERUN=true (re-matches those companies even if already Done) unless RERUN=false.
SCOPE (used when RESEARCH_TAG is empty)
        list:<id>      -> members of a HubSpot segment (default list:908). Change the segment filters in HubSpot,
                          no redeploy needed; membership is read fresh every run.
        <days>, e.g. 1 -> companies created in the last <days> days AND with Research Tags filled
        all            -> every company (status-based, resumable, 5,000 per run)

Each run: PICKUP finished batches of both stages -> SUBMIT stage 2 for newly ticked -> SUBMIT stage 1.
Batches finish within ~1 hour, so with an hourly schedule a company flows stage 1 -> stage 2 in 2-3 runs.
Status properties (companies): iap_match_status / iap_match_batch_id (stage 1),
                               enrichment_status / enrichment_batch_id (stage 2).
Companies: enrichment_iap_scores (multi-line text), iap_primary_campaign (dropdown select with one option per campaign record id,
           options kept in sync by the pipeline like enrichment_iap_campaigns; if the property is still single-line text the
           campaign name is written instead), iap_match_date and
           enrichment_date (date picker with time), enriched (single checkbox) must exist. Dates are set when each stage finishes.
Campaigns: campaign_priority (radio select 1-5, 1 = highest) is optional; missing = 3.

Env: HUBSPOT_TOKEN, ANTHROPIC_API_KEY, RESEARCH_TAG, SCOPE, LEAD_STATUSES, NO_LAST_CONTACT, STAGE1_MODEL, STAGE2_MODEL, STAGE1_BATCH (5000),
     STAGE2_MAX (500 per run), MAX_TEXT (6000), DRY_RUN, RERUN=true (ignore Done status, re-match everything in scope)
"""
import subprocess
import os, sys, re, json, time, concurrent.futures as cf
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import phonenumbers
import anthropic

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
SCOPE = os.getenv("SCOPE", "list:908").strip().lower()   # default: HubSpot segment 908
# Optional narrowing of a list: scope (segment mode only). Empty = every segment member.
LEAD_STATUSES = [x.strip() for x in os.getenv("LEAD_STATUSES", "").split(",") if x.strip()]   # e.g. "Public Pool,Closed Lead"
NO_LAST_CONTACT = os.getenv("NO_LAST_CONTACT", "false").lower() == "true"                      # only companies with Last Contact Date empty
ONLY_IAPS = [x.strip() for x in os.getenv("ONLY_IAPS", "198888772293,198892371671").split(",") if x.strip()]
GRATA_TAGS = [x.strip().lower() for x in os.getenv("GRATA_TAGS", "akoya - alinabal,akoya - h&w").split(",") if x.strip()]
RUN_CONTACTS = os.getenv("RUN_CONTACTS", "true").lower() == "true"
CONTACT_LIMIT = os.getenv("CONTACT_LIMIT", "50")
CONTACT_WORKER = os.getenv("CONTACT_WORKER", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "hubspot-contact-enrichment", "enrich_contacts.py"))
ALL_IAPS = []   # every active IAP, used only to keep the campaign property options complete
RESEARCH_TAG = os.getenv("RESEARCH_TAG", "").strip()
TAG_PROP = "research_tags__c"
STAGE1_MODEL = os.getenv("STAGE1_MODEL", "claude-haiku-5-5")
STAGE2_MODEL = os.getenv("STAGE2_MODEL", "claude-haiku-5-5")   # Haiku for both stages to keep cost down
STAGE1_BATCH = int(os.getenv("STAGE1_BATCH", "5000"))
STAGE2_MAX = int(os.getenv("STAGE2_MAX", "100"))
MAX_TEXT = int(os.getenv("MAX_TEXT", "6000"))
STORE_TEXT = 60000
PACK = 25
CONCURRENCY = 8
WEB_SEARCH_TOOL = os.getenv("WEB_SEARCH_TOOL", "web_search_20250305")
WEB_SEARCH_MAX = int(os.getenv("WEB_SEARCH_MAX", "3"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
RERUN = os.getenv("RERUN", "true" if RESEARCH_TAG else "false").lower() == "true"
# RERUN re-matches a company only if it was not matched since this moment (ms). setup_pipeline.py fixes it at deploy
# time, so hourly runs do not redo the same companies; locally it defaults to the start of today (UTC).
RERUN_SINCE = os.getenv("RERUN_SINCE") or str(int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000))
CAMPAIGN_OBJ = "p243636394_campaigns__telemarketing_"
def prio(v):
    """campaign_priority: radio select 1-5, 1 = highest. Missing or unreadable = 3."""
    m = re.search(r"[1-5]", str(v or ""))
    return int(m.group()) if m else 3
KEEP_MIN = int(os.getenv("KEEP_MIN", "3"))   # stage 2 score needed to tick an IAP (3 = ticks 3s, 4s and 5s)
PRIMARY_IS_ENUM = False                       # set at start: True when iap_primary_campaign is a dropdown select (values = campaign ids)
MATCHED = {}                                  # campaign id -> companies with that campaign ticked, loaded per pickup, +1 per tick made

def pick_primary(keep, by_id):
    """Primary among the ticked IAPs: best campaign_priority first; tie -> fewer matched companies; residual tie -> name."""
    best = min(keep, key=lambda k: (by_id[k]["tier"], MATCHED.get(k, 0), by_id[k]["name"]))
    for k in keep: MATCHED[k] = MATCHED.get(k, 0) + 1
    return best                                    # campaign record ID; the name goes to iap_primary_campaign_name

def load_matched_counts(iaps):
    """How many companies have each active campaign ticked right now (HubSpot search total, one call per campaign)."""
    for i in iaps:
        try:
            res = hs("POST", f"/crm/objects/{V}/companies/search", json={"filterGroups": [{"filters": [
                {"propertyName": P["iap_ids"], "operator": "CONTAINS_TOKEN", "value": i["id"]}]}], "properties": ["name"], "limit": 1})
            MATCHED[i["id"]] = int(res.get("total", 0))
        except Exception as e:
            MATCHED.setdefault(i["id"], 0); log("matched count failed for", i["name"], e)

P = dict(desc="enrichment_description", kws="enrichment_keywords", text="enrichment_website_text",
         iap_ids="enrichment_iap_campaigns", iap_names="enrichment_iap_matches",
         scores="enrichment_iap_scores", primary="iap_primary_campaign", primary_name="iap_primary_campaign_name", s1d="iap_match_date", s2d="enrichment_date", enriched="enriched",
         s1="iap_match_status", s1b="iap_match_batch_id", s2="enrichment_status", s2b="enrichment_batch_id",
         reasons="iap_match_reasons")
GRATA_TAG = "grata_research_tag"
EFFORT = os.getenv("EFFORT", "low")   # Haiku 5.5 thinks by default and the thinking counts against max_tokens; low effort keeps it short   # stage 1 writes a verdict + reason for the IAPs named by this tag, matched or not
ST = dict(pending="Pending", submitted="Submitted", done="Done", failed="Failed")
SRC = ["enrichment_description", "description", "about_us", "company_description__c"]  # stage 1 text sources, first non-empty wins
S1_FACTS = ["city", "state", "country", "numberofemployees"]   # sent to stage 1 for geography / size criteria
S1_SITE = ["website", "domain", "enrichment_website_text"]      # used when a company has no description

HS = "https://api.hubapi.com"; V = "2026-09"
HSH = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
client = anthropic.Anthropic()
S = requests.Session()

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    for attempt in range(7):
        try: r = S.request(method, HS + path, headers=HSH, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** attempt, 60)); continue
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

CAMPAIGN_NAMES = {}                            # campaign record ID -> campaign name, every campaign record (active or not)
def load_campaign_names():
    body = {"properties": ["campaign_name"], "limit": 100}
    out, after = {}, None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/2-175770723/search", json=body)
        for r in res.get("results", []): out[str(r["id"])] = (r["properties"].get("campaign_name") or "").strip()
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return out

def add_primary_names(updates):
    """Whenever iap_primary_campaign (an ID) is written, write the campaign name next to it in iap_primary_campaign_name."""
    for u in updates:
        props = u.get("properties", {})
        if P["primary"] in props:
            v = str(props[P["primary"]] or "").strip()
            props[P["primary_name"]] = CAMPAIGN_NAMES.get(v, "" if (not v or v.isdigit()) else v)

def hs_update(updates):
    """Batch update; if HubSpot rejects a batch (400), retry record by record so one bad value cannot block the rest."""
    add_primary_names(updates)
    if DRY_RUN or not updates: return
    for i in range(0, len(updates), 100):
        chunk = updates[i:i+100]
        try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": chunk})
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != 400: raise
            log("hubspot rejected a batch:", e.response.text[:300], "- retrying one by one")
            for u in chunk:
                try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": [u]})
                except requests.HTTPError as e2:
                    if e2.response is None or e2.response.status_code != 400: raise
                    msg = e2.response.text[:200]
                    log("  skipped", u["id"], msg)
                    # drop the property HubSpot complains about and write the rest
                    bad = [k for k in u["properties"] if k in msg]
                    rest = {k: v for k, v in u["properties"].items() if k not in bad}
                    if bad and rest:
                        try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": [{"id": u["id"], "properties": rest}]})
                        except requests.HTTPError: log("  still rejected", u["id"])

def search(filters, props, limit, sorts=None):
    """Page a company search. filters = list of filter groups (OR between groups)."""
    body = {"filterGroups": filters, "properties": props, "limit": min(limit, 200)}
    if sorts: body["sorts"] = sorts
    rows, after = [], None
    while len(rows) < limit:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/companies/search", json=body)
        rows += res.get("results", [])
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    seen, out = set(), []
    for r in rows:   # HubSpot search paging can repeat a record when records change mid-search
        if r["id"] not in seen: seen.add(r["id"]); out.append(r)
    return out[:limit]

# ---------- IAPs ----------
def load_ownership_options():
    """Read the ownership dropdown options from HubSpot so we never write a value HubSpot would reject."""
    global OWN_VALID
    try:
        p = hs("GET", f"/crm/v3/properties/companies/{OWN_PROP}")
        OWN_VALID = {o["label"] for o in p.get("options", []) if not o.get("hidden")} | {o["value"] for o in p.get("options", [])}
        missing = [v for v in set(OWN_MAP.values()) if v not in OWN_VALID]
        if missing: log("ownership options missing in HubSpot (will not be written):", missing)
    except Exception as e:
        OWN_VALID = set(); log("could not read ownership options:", e)

def load_iaps():
    body = {"filterGroups": [{"filters": [{"propertyName": "campaign_active", "operator": "EQ", "value": "true"},
                                          {"propertyName": "iap_description", "operator": "HAS_PROPERTY"}]}],
            "properties": ["campaign_name", "iap_description", "iap_keywords", "campaign_priority"], "limit": 100}
    iaps, after = [], None
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/{CAMPAIGN_OBJ}/search", json=body)
        for r in res.get("results", []):
            p = r["properties"]
            if not (p.get("campaign_name") and p.get("iap_description")): continue
            kws = [k.strip().lower() for k in re.split(r"[,\n;]", p.get("iap_keywords") or "") if k.strip()]
            iaps.append({"id": str(r["id"]), "name": p["campaign_name"].strip(), "desc": p["iap_description"].strip(),
                         "kws": kws, "tier": prio(p.get("campaign_priority"))})
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    return iaps   # inactive campaigns are already excluded by campaign_active

def sync_campaign_property(iaps, prop_name=None):
    """Keep a company property's options = the active campaigns (value = campaign record id, label = campaign name).
    Used for enrichment_iap_campaigns (multi-checkbox) and, when it is a dropdown select, iap_primary_campaign."""
    global PRIMARY_IS_ENUM
    prop_name = prop_name or P["iap_ids"]
    prop = hs("GET", f"/crm/v3/properties/companies/{prop_name}")
    if prop_name == P["primary"]:
        PRIMARY_IS_ENUM = prop.get("type") == "enumeration"
        if not PRIMARY_IS_ENUM: return       # text field: holds the campaign record ID, no options to sync
    existing = {o["value"]: o for o in prop.get("options", [])}
    active = {i["id"]: i["name"] for i in iaps}
    options, changed = [], False
    for value, o in existing.items():
        label, hidden = active.get(value, o.get("label")), value not in active
        if o.get("label") != label or bool(o.get("hidden")) != hidden: changed = True
        options.append({"label": label, "value": value, "hidden": hidden})
    for cid, name in active.items():
        if cid not in existing: options.append({"label": name, "value": cid, "hidden": False}); changed = True
    for n, o in enumerate(options): o["displayOrder"] = n
    if changed and not DRY_RUN:
        hs("PATCH", f"/crm/v3/properties/companies/{prop_name}", json={"options": options})
        log(prop_name, "options updated:", len(active), "active options")

KEYWORD_FILTER = os.getenv("KEYWORD_FILTER", "false").lower() == "true"
LOOP = os.getenv("LOOP", "true").lower() == "true"          # keep running until both stages are finished
POLL_MINUTES = float(os.getenv("POLL_MINUTES", "10"))       # wait between checks while batches are processing
MAX_HOURS = float(os.getenv("MAX_HOURS", "12"))             # safety stop for LOOP
HQ_ALLOWED = {x.strip().lower() for x in os.getenv("HQ_ALLOWED", "US,CA").split(",") if x.strip()}  # headquarters must be here
OWN_PROP, PARENT_PROP = "ownership", "immediate_parent__c"
OWN_MAP = {"private": "Private", "private equity": "Private Equity", "public": "Public",
           "private sub": "Private Sub", "public sub": "Public Sub", "esop": "ESOP"}
OWN_VALID = set()   # filled from HubSpot at start
HOSTING = ("myftpupload.com", "iglouhost.com", "wixsite.com", "godaddysites.com", "squarespace.com",
           "weebly.com", "wordpress.com", "business.site", "yolasite.com", "webflow.io")

# Batch API prices per million tokens (half of standard) and web search per 1,000 searches. Haiku 5.5 standard:
# $0.10 in, $0.50 out, $0.01 cache read, $0.125 cache write (5 min). Checked Nov 2026 - update if the price list changes.
PRICES = {"haiku": dict(inp=0.05, out=0.25, cread=0.005, cwrite=0.0625), "sonnet": dict(inp=1.50, out=7.50, cread=0.15, cwrite=1.875)}
WEB_SEARCH_PRICE = 10.0 / 1000
RUN_COST = {"usd": 0.0}

def usage_add(acc, msg):
    """Accumulate token usage from one batch result message."""
    u = getattr(msg, "usage", None)
    if not u: return
    acc["inp"] += getattr(u, "input_tokens", 0) or 0
    acc["out"] += getattr(u, "output_tokens", 0) or 0
    acc["cread"] += getattr(u, "cache_read_input_tokens", 0) or 0
    acc["cwrite"] += getattr(u, "cache_creation_input_tokens", 0) or 0
    st = getattr(u, "server_tool_use", None)
    acc["search"] += (getattr(st, "web_search_requests", 0) or 0) if st else 0
    acc["n"] += 1

def usage_line(acc, model):
    pr = PRICES["sonnet" if "sonnet" in model else "haiku"]
    usd = (acc["inp"] * pr["inp"] + acc["out"] * pr["out"] + acc["cread"] * pr["cread"] + acc["cwrite"] * pr["cwrite"]) / 1e6 + acc["search"] * WEB_SEARCH_PRICE
    RUN_COST["usd"] += usd
    return (f"{acc['n']} answers, tokens in {acc['inp']:,} (cache read {acc['cread']:,}, cache write {acc['cwrite']:,}), out {acc['out']:,}, "
            f"web searches {acc['search']}, est. ${usd:.2f} (run total ${RUN_COST['usd']:.2f})")

def new_usage(): return {"inp": 0, "out": 0, "cread": 0, "cwrite": 0, "search": 0, "n": 0}

def parse_json_answer(raw):
    """Read the JSON object in a model answer; tolerate code fences and text around it."""
    raw = raw.replace("```json", "").replace("```", "")
    return json.loads(raw[raw.find("{"): raw.rfind("}") + 1])

def parse_stage1_answer(raw):
    """Stage 1 answers map company ID -> object. Parse whole; if broken, salvage each company object on its own."""
    try: return parse_json_answer(raw), 0
    except Exception: pass
    out, bad = {}, 0
    for m in re.finditer(r'"(\d{6,})"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})', raw):
        try: out[m.group(1)] = json.loads(m.group(2))
        except Exception: bad += 1
    if not out: raise ValueError("no company objects found")
    return out, bad

def tag_iaps(tag, iaps):
    """IAPs named by a Grata Research Tag such as 'baywater - add-on' (first word of the tag found in the campaign name)."""
    key = re.split(r"[-,;]", (tag or "").lower())[0].strip()
    return [i["id"] for i in iaps if key and key in i["name"].lower()]

def candidate_iaps(text, iaps):
    """By default every active IAP goes to the model, so matching comes from its reading of the
    description / website, not from keyword overlap. KEYWORD_FILTER=true restores the old pre-filter."""
    if not KEYWORD_FILTER: return list(iaps)
    t = text.lower()
    return [i for i in iaps if not i["kws"] or any(k in t for k in i["kws"])]

# ---------- scope ----------
def now_ms(): return str(int(time.time() * 1000))

def scope_filters():
    """Extra filters that define the scope. Returns a list of filters to AND into every stage-1 selection."""
    if RESEARCH_TAG: return [{"propertyName": TAG_PROP, "operator": "CONTAINS_TOKEN", "value": RESEARCH_TAG}]
    if SCOPE == "all": return []
    days = float(SCOPE)
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    return [{"propertyName": "createdate", "operator": "GTE", "value": str(since)},
            {"propertyName": TAG_PROP, "operator": "HAS_PROPERTY"}]   # Research Tags must be filled

def list_members(list_id):
    ids, after = [], None
    while True:
        params = {"limit": 250}
        if after: params["after"] = after
        try: res = hs("GET", f"/crm/lists/2026-09/{list_id}/memberships", params=params)
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != 404: raise
            res = hs("GET", f"/crm/v3/lists/{list_id}/memberships", params=params)
        ids += [str(r["recordId"]) for r in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    return list(dict.fromkeys(ids))

def _names():
    return {i["id"]: i["name"] for i in (ALL_IAPS or [])}

def merge_ids(existing, new_ids):
    """Keep ticks of campaigns outside ONLY_IAPS, replace the ONLY_IAPS ticks with new_ids."""
    other = [x for x in (existing or "").split(";") if x.strip() and x.strip() not in ONLY_IAPS]
    return sorted(set(x.strip() for x in other) | set(new_ids))

def other_score_lines(existing):
    """Score lines of campaigns outside ONLY_IAPS (lines look like '<campaign name>: <score> - <reason>')."""
    only_names = [n for k, n in _names().items() if k in ONLY_IAPS]
    return [l for l in (existing or "").split("\n") if l.strip() and not any(l.startswith(n + ":") for n in only_names)]

def is_only_name(name):
    """True when an iap_primary_campaign value (a campaign ID, or a name written before the switch to IDs) is an ONLY_IAPS campaign."""
    return bool(name) and (name in ONLY_IAPS or name in [n for k, n in _names().items() if k in ONLY_IAPS])

_SCOPE_CACHE = {}
def scope_ids():
    """Segment members, narrowed by LEAD_STATUSES (hs_lead_status) and NO_LAST_CONTACT (last_contact_date empty)."""
    ids = list_members(SCOPE[5:])
    if not LEAD_STATUSES and not NO_LAST_CONTACT and not GRATA_TAGS: return ids
    keep = []
    for r in batch_read(ids, ["hs_lead_status", "last_contact_date", "grata_research_tag"]):
        pr = r["properties"]
        if GRATA_TAGS and (pr.get("grata_research_tag") or "").strip().lower() not in GRATA_TAGS: continue
        if LEAD_STATUSES and (pr.get("hs_lead_status") or "") not in LEAD_STATUSES: continue
        if NO_LAST_CONTACT and pr.get("last_contact_date"): continue
        keep.append(str(r["id"]))
    if _SCOPE_CACHE.get("n") != (len(ids), len(keep)):
        _SCOPE_CACHE["n"] = (len(ids), len(keep))
        log(f"segment {SCOPE[5:]}: {len(ids)} members, {len(keep)} after filters (grata tag: {', '.join(GRATA_TAGS) or 'any'}; lead status: {', '.join(LEAD_STATUSES) or 'any'}; last contact empty: {NO_LAST_CONTACT})")
    return keep

def batch_read(ids, props):
    rows = []
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/companies/batch/read",
                 json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": props})
        rows += res.get("results", [])
    return rows

def list_rows(props):
    """Segment members that still need stage 1, using the same status rules as the search path."""
    ids = scope_ids()
    rows = batch_read(ids, props + [P["s1"], P["s1d"]])
    keep = []
    for r in rows:
        st, dt = r["properties"].get(P["s1"]), r["properties"].get(P["s1d"])
        if st == ST["submitted"]: continue
        if not RERUN and dt and st not in (ST["pending"], ST["failed"]): continue   # already matched by this pipeline (Failed is retried)
        if RERUN and dt:
            try:
                ms = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp() * 1000) if not dt.isdigit() else int(dt)
                if ms >= int(RERUN_SINCE): continue
            except ValueError: pass
        keep.append(r)
    keep.sort(key=lambda r: int(r["id"]))
    log(f"segment {SCOPE[5:]}: {len(ids)} members, {len(keep)} need stage 1")
    return keep[:STAGE1_BATCH]

# ---------- STAGE 1: cheap match ----------
def iap_block(iaps):
    return "\n\n".join(f"IAP ID {i['id']} ({i['name']}):\n{i['desc']}" for i in iaps)

def cached_system(text):
    """The part that is identical in every request of a batch: cached, so repeats cost ~10% of the input price."""
    return [{"type": "text", "text": text, "cache_control": {"type": "ephemeral"}}]

def s1_system(iaps):
    return f"""You match companies against acquisition profiles (IAPs) using only the company description given.

ACQUISITION PROFILES:
{iap_block(iaps)}

Return ONLY a JSON object mapping company ID to an object, e.g.
{{"123": {{"iaps": ["329011495616"], "reasons": {{"329011495616": "power-line contractor in TX, non-union stated"}},
          "description": ""}}, "456": {{"iaps": [], "reasons": {{}}, "description": ""}}}}
Include every company ID.
- "iaps": the IAP IDs the company matches. List an IAP only when the description clearly satisfies its core criteria
  (services, industry, geography, size, exclusions). Do not guess.
- "reasons": for each IAP ID listed after REASONS FOR on that company, one line (max 25 words) saying why it matches
  or the first criterion it fails. Give it whether or not the IAP is in "iaps". Empty object when REASONS FOR is none.
- "description": when the company is marked WRITE DESCRIPTION, 2-3 factual sentences on what it does, for whom and
  where, based on the text given. Otherwise "".
Use LOCATION and EMPLOYEES to apply each IAP's geography and size criteria. If a value is "unknown", do not
exclude the company for that reason; judge it on the description alone.
Criteria that a description cannot show (EBITDA, revenue, margins, union status unless stated, bonding, customer
concentration, anything tagged [Client]) are NEUTRAL: never exclude a company because one of them is "not stated" or
"unverified", and never give that as the failing criterion. Exclude on union status or a designation (MBE, WBE, DBE)
only when the IAP lists it as a hard exclusion AND the text states it (for example "IBEW signatory", "certified WBE").
When the IAP says to note it but not reject, mention it in the reason and do not exclude."""

def s1_user(companies, allowed_ids=None):
    comp_block = "\n\n".join(f"COMPANY {c['id']} — {c['name']}\nLOCATION: {c['loc'] or 'unknown'} | EMPLOYEES: {c['emp'] or 'unknown'}\n"
                             f"REASONS FOR: {', '.join(c['tag_iaps']) or 'none'}" + (" | WRITE DESCRIPTION" if c['need_desc'] else "") + "\n"
                             f"{c['text'][:1500]}" for c in companies)
    limit = f"Consider only these IAP IDs for these companies: {', '.join(allowed_ids)}\n\n" if allowed_ids else ""
    return f"{limit}COMPANIES:\n{comp_block}"


def s1_submit(iaps):
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        rows = list_rows(["name", GRATA_TAG] + SRC + S1_FACTS + S1_SITE)
    else:
        rows = None
    scope = scope_filters() if rows is None else []
    status_f = [{"propertyName": P["s1"], "operator": "NOT_HAS_PROPERTY"}] if not RERUN else []
    pending_f = [{"propertyName": P["s1"], "operator": "IN", "values": [ST["pending"], ST["failed"]]}]   # Failed is retried
    if not RERUN:
        groups = [{"filters": scope + pending_f}, {"filters": scope + status_f}]
    else:
        # re-match each company once per RERUN_SINCE: anything not matched since that moment
        notsub = [{"propertyName": P["s1"], "operator": "NEQ", "value": ST["submitted"]}]
        groups = [{"filters": scope + notsub + [{"propertyName": P["s1d"], "operator": "LT", "value": RERUN_SINCE}]},
                  {"filters": scope + notsub + [{"propertyName": P["s1d"], "operator": "NOT_HAS_PROPERTY"}]}]
    if rows is None:
        rows = search(groups, ["name", GRATA_TAG] + SRC + S1_FACTS + S1_SITE, STAGE1_BATCH, [{"propertyName": "hs_object_id", "direction": "ASCENDING"}])
    if not rows: log("stage 1: nothing to match in scope"); return
    # companies with no description: use stored website text, or crawl the site now (free, no model call)
    def site_text(r):
        p = r["properties"]
        if any(p.get(x) for x in SRC): return r["id"], None, False
        if p.get("enrichment_website_text"): return r["id"], p["enrichment_website_text"], False
        dom = clean_domain(p.get("domain") or p.get("website"))
        site = fetch_site(p.get("website"), dom) if (p.get("website") or dom) else None
        return r["id"], (site["text"] if site else None), bool(site)
    with cf.ThreadPoolExecutor(CONCURRENCY) as ex: got = list(ex.map(site_text, rows))
    from_site = {cid: t for cid, t, _ in got if t}
    store = [{"id": cid, "properties": {P["text"]: t[:STORE_TEXT]}} for cid, t, crawled in got if crawled and t]
    nodesc = sum(1 for r in rows if not any(r["properties"].get(x) for x in SRC))
    log(f"stage 1: {nodesc} with no description, {len(from_site)} matched from website text instead ({len(store)} crawled now)")
    skip, grouped = [], {}
    for r in rows:
        p = r["properties"]; text = next((p[s] for s in SRC if p.get(s)), None) or from_site.get(r["id"])
        cands = candidate_iaps(text, iaps) if text else []
        if not text:   # no description on record: skip stage 1, send to stage 2 to crawl the site and grade every IAP
            skip.append({"id": r["id"], "properties": {P["s1"]: ST["done"], P["s1d"]: now_ms(), P["s2"]: ST["pending"]}}); continue
        if not cands: skip.append({"id": r["id"], "properties": {P["s1"]: ST["done"], P["s1d"]: now_ms()}}); continue
        tagged = tag_iaps(p.get(GRATA_TAG), iaps)
        cands = cands + [i for i in iaps if i["id"] in tagged and i not in cands]   # always judge the tag's IAPs
        grouped.setdefault(tuple(sorted(i["id"] for i in cands)), []).append({"id": r["id"], "name": p.get("name") or "", "text": text,
            "loc": ", ".join(x for x in (p.get("city"), p.get("state"), p.get("country")) if x), "emp": p.get("numberofemployees") or "",
            "tag_iaps": tagged, "need_desc": not any(p.get(x) for x in SRC)})
    reqs, submitted = [], []
    for key, comps in grouped.items():
        sub = [i for i in iaps if i["id"] in key]
        for n in range(0, len(comps), PACK):
            chunk = comps[n:n+PACK]
            allowed = None if len(sub) == len(iaps) else [i["id"] for i in sub]
            reqs.append({"custom_id": f"g{len(reqs)}", "params": {"model": STAGE1_MODEL, "max_tokens": 600 * len(chunk) + 6000,
                         "output_config": {"effort": EFFORT}, "system": cached_system(s1_system(iaps)),
                         "messages": [{"role": "user", "content": s1_user(chunk, allowed)}]}})
            submitted += [c["id"] for c in chunk]
    nodesc = sum(1 for u in skip if u["properties"].get(P["s2"]) == ST["pending"])
    log(f"stage 1: pulled {len(rows)}, no description (sent to stage 2) {nodesc}, no-candidate {len(skip) - nodesc}, "
        f"sending {len(submitted)} in {len(reqs)} requests")
    if DRY_RUN or not reqs: hs_update(skip); return
    hs_update(store); hs_update(skip)
    bid = client.messages.batches.create(requests=reqs).id
    hs_update([{"id": c, "properties": {P["s1"]: ST["submitted"], P["s1b"]: bid}} for c in submitted])
    log("stage 1: submitted batch", bid)

def s1_pickup(iaps):
    by_id = {i["id"]: i["name"] for i in iaps}
    rows = search([{"filters": [{"propertyName": P["s1"], "operator": "EQ", "value": ST["submitted"]}]}],
                  [P["s1b"], P["iap_ids"], P["iap_names"], P["scores"], P["primary"]] + SRC, 100000)
    by_batch = {}
    for r in rows: by_batch.setdefault(r["properties"].get(P["s1b"]), {})[r["id"]] = r["properties"]
    by_batch.pop(None, None)
    ticked_now = []
    for bid, comps in by_batch.items():
        try: b = client.messages.batches.retrieve(bid)
        except anthropic.NotFoundError:
            log("stage 1: batch", bid, "not visible to this key, putting", len(comps), "companies back to Pending")
            hs_update([{"id": c, "properties": {P["s1"]: ST["pending"]}} for c in comps]); continue
        if b.processing_status != "ended": log("stage 1: batch", bid, "still", b.processing_status); continue
        results, req_errors, usage = {}, [], new_usage()
        for item in client.messages.batches.results(bid):
            if item.result.type != "succeeded":
                req_errors.append(f"{item.custom_id}: {item.result.type} {getattr(getattr(item.result, 'error', None), 'message', '')}"); continue
            msg = item.result.message; usage_add(usage, msg)
            raw = "\n".join(x.text for x in msg.content if x.type == "text")
            try:
                parsed, bad = parse_stage1_answer(raw); results.update(parsed)
                if bad: req_errors.append(f"{item.custom_id}: JSON partly broken, salvaged {len(parsed)} companies, lost {bad}, stop_reason={msg.stop_reason}")
            except Exception as e:
                req_errors.append(f"{item.custom_id}: unreadable JSON ({type(e).__name__}), stop_reason={msg.stop_reason}, {len(raw)} chars")
        for line in req_errors[:20]: log("  stage 1 request failed", line)
        log("stage 1: cost", usage_line(usage, STAGE1_MODEL))
        updates = []
        for cid, props in comps.items():
            if cid not in results: updates.append({"id": cid, "properties": {P["s1"]: ST["failed"]}}); continue
            res = results[cid]
            if isinstance(res, list): res = {"iaps": res}          # old answer format
            if not isinstance(res, dict): res = {"iaps": []}
            new_ids = [str(x) for x in (res.get("iaps") or []) if str(x) in by_id]
            merged = merge_ids(props.get(P["iap_ids"]), new_ids)
            names_all = _names()
            props_out = {P["iap_ids"]: ";".join(merged), P["iap_names"]: ", ".join(names_all.get(x, x) for x in merged),
                         P["s1"]: ST["done"], P["s1d"]: now_ms()}
            reasons = {str(k): str(v).strip() for k, v in (res.get("reasons") or {}).items() if str(k) in by_id and str(v).strip()}
            props_out[P["reasons"]] = "\n".join(f"{by_id[k]}: {'MATCH' if k in new_ids else 'NO'} - {v}" for k, v in reasons.items())[:65000]
            desc = (res.get("description") or "").strip()
            if desc and not any(props.get(x) for x in SRC): props_out[P["desc"]] = desc[:65000]   # no description on record: fill it
            if new_ids: props_out[P["s2"]] = ST["pending"]; ticked_now.append(cid)   # hand to stage 2
            else:   # no Akoya tick: drop stale Akoya grades, keep every other campaign's
                props_out[P["scores"]] = "\n".join(other_score_lines(props.get(P["scores"])))[:65000]
                if is_only_name(props.get(P["primary"])): props_out[P["primary"]] = ""
            updates.append({"id": cid, "properties": props_out})
        hs_update(updates)
        log(f"stage 1: batch {bid} written {len(updates)}, ticked {sum(1 for u in updates if u['properties'].get(P['s2']) == ST['pending'])}, "
            f"failed {sum(1 for u in updates if u['properties'].get(P['s1']) == ST['failed'])}, "
            f"reasons written {sum(1 for u in updates if u['properties'].get(P['reasons']))}, descriptions written {sum(1 for u in updates if P['desc'] in u['properties'])}")
    return ticked_now

# ---------- STAGE 2: deep enrichment + graded match ----------
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
LI_RE = re.compile(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/([A-Za-z0-9\-_%.]+)", re.I)
COUNTRY = {"united states": "US", "usa": "US", "us": "US", "canada": "CA", "united kingdom": "GB", "uk": "GB",
           "mexico": "MX", "australia": "AU", "germany": "DE", "france": "FR", "ireland": "IE", "costa rica": "CR"}

def norm_li(url):
    m = LI_RE.search(url or "")
    return f"https://www.linkedin.com/company/{m.group(1).strip('/').lower()}" if m else ""

def clean_domain(v):
    v = (v or "").strip().lower()
    if not v: return ""
    if "://" not in v: v = "http://" + v
    host = urlparse(v).netloc.split(":")[0]
    return host[4:] if host.startswith("www.") else host

US_NAMES = {"united states", "united states of america", "usa", "us", "u s", "u s a", "america"}
CA_NAMES = {"canada", "ca"}

def norm_country(v):
    t = re.sub(r"\s+", " ", re.sub(r"[^a-z ]", " ", (v or "").lower())).strip()
    if t in US_NAMES: return "us"
    if t in CA_NAMES: return "ca"
    return t

def canonical_country(v):
    return {"us": "United States", "ca": "Canada"}.get(norm_country(v), (v or "").strip())

def homepage_ok(url):
    u = urlparse(url if "://" in url else "https://" + url)
    host = u.netloc.lower()
    if not host or any(host.endswith(h) for h in HOSTING): return False
    return u.path in ("", "/") and not u.query

def get(url):
    try:
        r = requests.get(url, headers=UA, timeout=15, allow_redirects=True)
        if r.status_code < 400 and "text/html" in r.headers.get("content-type", ""): return r
    except requests.RequestException: pass
    return None

def page_bits(r):
    soup = BeautifulSoup(r.text, "html.parser")
    links = [urljoin(r.url, a.get("href", "")) for a in soup.find_all("a", href=True)]
    tels = [a["href"][4:] for a in soup.find_all("a", href=True) if a["href"].lower().startswith("tel:")]
    for t in soup(["script", "style", "noscript", "svg"]): t.decompose()
    text = re.sub(r"\s+", " ", soup.get_text(" ")).strip()
    return text, links, tels

def fetch_site(website, domain):
    """Returns dict(url, text, linkedin, tels), or None when the site does not load."""
    tries = []
    if website: tries.append(website if "://" in website else "https://" + website)
    if domain: tries += [f"https://{domain}", f"https://www.{domain}", f"http://{domain}"]
    for url in tries:
        r = get(url)
        if not r: continue
        text, links, tels = page_bits(r)
        if len(text) < 300: continue
        host = urlparse(r.url).netloc
        contact = next((l for l in links if re.search(r"contact", l, re.I) and urlparse(l).netloc == host), None)
        if contact:
            c = get(contact)
            if c:
                ct, cl, ctel = page_bits(c)
                text += " CONTACT PAGE: " + ct[:2000]; links += cl; tels += ctel
        lis = sorted({norm_li(l) for l in links if LI_RE.search(l)} - {""})
        return {"url": r.url, "text": text, "linkedin": lis, "tels": tels}
    return None

def pick_phone(tels, text, country):
    region = COUNTRY.get((country or "").strip().lower(), "US")
    cands = list(tels) + [m.raw_string for m in phonenumbers.PhoneNumberMatcher(text[:20000], region)]
    for raw in cands:
        try:
            n = phonenumbers.parse(raw, region)
            if phonenumbers.is_valid_number(n):
                return phonenumbers.format_number(n, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        except phonenumbers.NumberParseException: continue
    return ""

S2_RULES = """You clean and enrich a CRM company record, then grade its fit against acquisition profiles (IAPs).

STEP 1 - WEBSITE. Decide whether the website given really belongs to this company (its name, and its description when there is one).
A parked domain, a for-sale page, a directory listing, a different company or a dead site is NOT valid.
If it is not valid and web search is available, use it to find the company's own official website (if search is
not available, return "website_valid": false and "website": ""). Return ONLY a homepage on the
company's own domain (e.g. https://acme.com), never a page deep inside another site, a directory, a hosting
subdomain or a parent company's site. Only return one you are confident in.
If the company was ACQUIRED by or MERGED into another company, do NOT return the buyer's website: set
"acquired_by" to the buyer's name, set "website" to "" and keep "website_valid" as it is.
STEP 2 - LINKEDIN. If exactly one LinkedIn company link was found on a valid site, that is the answer.
Do not search for LinkedIn. If there is not exactly one link on the site, return "".
STEP 3 - OWNERSHIP. Classify who owns the company as exactly one of: Private, Private Equity, Public, Private Sub,
Public Sub, ESOP. Private = independent, privately held (family/founder). Private Equity = backed by a PE firm.
Public = listed on a stock exchange. Private Sub = subsidiary of a private company. Public Sub = subsidiary of a
public company. ESOP = employee owned. Use only the website text (About, News, footer); do not search for ownership.
Put the PE firm or parent company in "owner_name". If you cannot tell, return "" for both.
STEP 4 - HEADQUARTERS. Find the country of the company's OWN headquarters (not its parent's) from the website
(footer, Contact, About) or the search evidence. Do not rely on the country on record, it may be wrong. Return the
full English country name in "hq_country" (e.g. "United States", "Canada", "United Kingdom"), or "" if not shown.
Also return the headquarters city in "hq_city" and state/province in "hq_state" (full name, e.g. "Texas", "Ontario"),
only when the website or search evidence shows them; otherwise "".
STEP 5 - Describe the company and grade it ONLY against the IAP IDs named in the request, using the valid website
(or the search evidence for the corrected website).

Finish with ONLY a JSON object:
{"website_valid": true or false,
  "website": "official website URL (the one on record if valid, the corrected one if not, empty if none found)",
  "linkedin": "confirmed linkedin.com/company/ URL or empty",
  "linkedin_evidence": "site" or "",
  "acquired_by": "buyer name if acquired or merged, else empty",
  "ownership": "Private" or "Private Equity" or "Public" or "Private Sub" or "Public Sub" or "ESOP" or "",
  "owner_name": "PE firm or parent company, else empty",
  "hq_country": "country of the company's own headquarters, full English name, or empty",
  "hq_city": "headquarters city or empty",
  "hq_state": "headquarters state/province, full name, or empty",
  "description": "2-4 plain sentences: what the company actually does, who it serves, where it operates",
  "keywords": "10-20 comma-separated keywords: services, industry, niche, geography",
  "scores": {"<IAP ID>": {"score": 1-5, "reason": "one short line, ONLY for scores of 3 or more; otherwise empty"}} }
Score every IAP ID named. 5 = clearly fits every core criterion; 4 = fits, one minor gap; 3 = plausible but unproven;
2 = weak; 1 = does not fit or violates an exclusion. If no website or evidence was found, score every IAP 1.
SCORING RULE: grade only on what a website or public source can show (what the company does, its services, customers,
geography, business model, ownership type when stated, employee count). Criteria tagged [Client] or that cannot be checked
publicly (EBITDA, revenue, margins, revenue mix, customer concentration, union status, bonding, litigation, management depth)
are NEUTRAL: never lower a score because one of them is unverified, and never write "unverified" criteria as the reason for
a lower score. A company that fits every checkable criterion scores 5 even when every [Client] criterion is unknown."""

GRADE_RULES = """You grade a company's fit against acquisition profiles (IAPs) using only the material given.
Grade ONLY the IAP IDs named in the request.

Return ONLY a JSON object:
{"scores": {"<IAP ID>": {"score": 1-5, "reason": "one short line, ONLY for scores of 3 or more; otherwise empty"}} }
5 = clearly fits every core criterion; 4 = fits, one minor gap; 3 = plausible but unproven;
2 = weak; 1 = does not fit or violates an exclusion.
SCORING RULE: grade only on what a website or public source can show (what the company does, its services, customers,
geography, business model, ownership type when stated, employee count). Criteria tagged [Client] or that cannot be checked
publicly (EBITDA, revenue, margins, revenue mix, customer concentration, union status, bonding, litigation, management depth)
are NEUTRAL: never lower a score because one of them is unverified, and never write "unverified" criteria as the reason for
a lower score. A company that fits every checkable criterion scores 5 even when every [Client] criterion is unknown."""

def s2_system(iaps, grade_only=False):
    return f"{GRADE_RULES if grade_only else S2_RULES}\n\nACQUISITION PROFILES (IAPs):\n{iap_block(iaps)}"

def s2_user(c, cands):
    site = c["site"]
    if site:
        site_block = (f"WEBSITE LOADED: {site['url']}\nLINKEDIN LINKS FOUND ON THE SITE: {', '.join(site['linkedin']) or 'none'}\n"
                      f"WEBSITE TEXT:\n{site['text'][:MAX_TEXT]}")
    else:
        site_block = f"WEBSITE ON RECORD: {c['website'] or c['domain'] or 'none'} - it does NOT load (dead, blocked or missing)."
    return f"""COMPANY NAME: {c['name']}
CITY / STATE / COUNTRY ON RECORD: {c['loc'] or 'unknown'}
DESCRIPTION ON RECORD: {c['desc'] or 'none'}
LINKEDIN ON RECORD: {c['li'] or 'none'}
{site_block}

GRADE ONLY THESE IAP IDS: {', '.join(i['id'] for i in cands)}"""

def s2_grade_user(c, txt, cands):
    return f"""COMPANY NAME: {c['name']}
CITY / STATE / COUNTRY: {c['loc'] or 'unknown'}
DESCRIPTION: {c['desc'] or 'none'}
WEBSITE TEXT:
{txt[:MAX_TEXT]}

GRADE ONLY THESE IAP IDS: {', '.join(i['id'] for i in cands)}"""

S2_PROPS = ["name", "domain", "website", "phone", "linkedin_company_page", "city", "state", "country", P["iap_ids"]] + SRC

def s2_submit(iaps):
    props_needed = S2_PROPS + [P["s2"], P["s2b"], P["enriched"], P["text"]]
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        members = batch_read(scope_ids(), props_needed)
        rows = sorted([r for r in members if r["properties"].get(P["s2"]) in (ST["pending"], ST["failed"])], key=lambda r: int(r["id"]))[:STAGE2_MAX]
    else:
        rows = search([{"filters": [{"propertyName": P["s2"], "operator": "IN", "values": [ST["pending"], ST["failed"]]}]}],
                      props_needed, STAGE2_MAX, [{"propertyName": "hs_object_id", "direction": "ASCENDING"}])
    if not rows: log("stage 2: nothing pending"); return
    def crawl(r):
        p = r["properties"]
        domain = clean_domain(p.get("domain") or p.get("website"))
        return {"id": r["id"], "name": p.get("name") or domain, "domain": domain, "website": (p.get("website") or "").strip(),
                "phone": p.get("phone") or "", "li": p.get("linkedin_company_page") or "", "country": p.get("country") or "",
                "loc": ", ".join(x for x in (p.get("city"), p.get("state"), p.get("country")) if x),
                "desc": next((p[x] for x in SRC if p.get(x)), "")[:1500], "ticked": p.get(P["iap_ids"]) or "",
                "retry_search": p.get(P["s2b"]) == "search",
                "grade_only": (p.get(P["enriched"]) or "").lower() == "true", "stored": p.get(P["text"]) or "",
                "site": fetch_site(p.get("website"), domain)
                        if (p.get("website") or domain) and (p.get(P["enriched"]) or "").lower() != "true" else None}
    with cf.ThreadPoolExecutor(CONCURRENCY) as ex: crawled = list(ex.map(crawl, rows))
    reqs, updates, dead, graded_only, searched = [], [], 0, 0, 0
    by_id = {i["id"]: i for i in iaps}
    for c in crawled:
        cands = [by_id[x] for x in sorted(set(c["ticked"].split(";")) - {""}) if x in by_id]   # only what stage 1 ticked
        if not cands and not c["desc"] and not c["grade_only"]:
            cands = list(iaps)   # no description, so stage 1 could not match: grade every active IAP from the website
        if not cands:   # nothing ticked (or ticked IAP no longer active): nothing to confirm
            updates.append({"id": c["id"], "properties": {P["s2"]: ST["done"], P["s2d"]: now_ms(), P["s2b"]: ""}, "_final": True}); continue
        if c["grade_only"]:
            txt = c["stored"] or c["desc"] or c["name"]; graded_only += 1
        else:
            txt = c["site"]["text"] if c["site"] else (c["desc"] or c["name"])
            if not c["site"]: dead += 1
        if c["grade_only"]:   # already enriched: grade only, no website / LinkedIn / phone changes, no web search
            reqs.append({"custom_id": "g" + c["id"], "params": {"model": STAGE2_MODEL, "max_tokens": 4000 + 150 * len(cands),
                         "output_config": {"effort": EFFORT}, "system": cached_system(s2_system(iaps, grade_only=True)),
                         "messages": [{"role": "user", "content": s2_grade_user(c, txt, cands)}]}})
            updates.append({"id": c["id"], "properties": {}}); continue
        params = {"model": STAGE2_MODEL, "max_tokens": 8000 + 150 * len(cands), "output_config": {"effort": EFFORT},
                  "system": cached_system(s2_system(iaps)),
                  "messages": [{"role": "user", "content": s2_user(c, cands)}]}
        use_search = (not c["site"]) or c["retry_search"]   # search only when the site is dead or was judged wrong
        if use_search:
            params["tools"] = [{"type": WEB_SEARCH_TOOL, "name": "web_search", "max_uses": WEB_SEARCH_MAX}]; searched += 1
            params["max_tokens"] += 4000   # search results and the thinking between them also count
        reqs.append({"custom_id": ("s" if use_search else "e") + c["id"], "params": params})
        props = {}
        if c["site"]:
            props[P["text"]] = c["site"]["text"][:STORE_TEXT]
            if not c["phone"]:
                ph = pick_phone(c["site"]["tels"], c["site"]["text"], c["country"])
                if ph: props["phone"] = ph
        updates.append({"id": c["id"], "properties": props})
    log(f"stage 2: {len(reqs)} companies with {STAGE2_MODEL}: {graded_only} already enriched (grade only), {dead} with a dead or missing website, "
        f"{searched} with web search, {sum(1 for u in updates if 'phone' in u['properties'])} blank phones filled from the site")
    if DRY_RUN:
        if reqs: log("sample request:\n", reqs[0]["params"]["messages"][0]["content"][:2500])
        for u in updates[:50]:
            if "phone" in u["properties"]: log("  would fill phone", u["id"], u["properties"]["phone"])
        return
    final = [{"id": u["id"], "properties": u["properties"]} for u in updates if u.get("_final")]
    hs_update(final)
    updates = [u for u in updates if not u.get("_final")]
    if not reqs: log("stage 2: nothing to send"); return
    bid = client.messages.batches.create(requests=reqs).id
    for u in updates: u["properties"].update({P["s2"]: ST["submitted"], P["s2b"]: bid})
    hs_update(updates)
    log("stage 2: submitted batch", bid)

def s2_pickup(iaps):
    by_id = {i["id"]: i for i in iaps}
    load_matched_counts(iaps)
    rows = search([{"filters": [{"propertyName": P["s2"], "operator": "EQ", "value": ST["submitted"]}]}],
                  [P["s2b"], "website", "domain", "linkedin_company_page", OWN_PROP, PARENT_PROP, "country", "city", "state",
                   P["iap_ids"], P["scores"], P["primary"]], 100000)
    rec = {r["id"]: r["properties"] for r in rows}
    by_batch = {}
    for r in rows: by_batch.setdefault(r["properties"].get(P["s2b"]), []).append(r["id"])
    by_batch.pop(None, None)
    for bid, ids in by_batch.items():
        try: b = client.messages.batches.retrieve(bid)
        except anthropic.NotFoundError:
            log("stage 2: batch", bid, "not visible to this key, putting", len(ids), "companies back to Pending")
            hs_update([{"id": c, "properties": {P["s2"]: ST["pending"]}} for c in ids]); continue
        if b.processing_status != "ended": log("stage 2: batch", bid, "still", b.processing_status); continue
        updates, failed, fixed_sites, acquired, rejected, retry, outside, country_fixed = [], set(ids), [], [], [], [], [], []
        loc_filled, why, usage = [], {}, new_usage()
        for item in client.messages.batches.results(bid):
            raw_id = item.custom_id
            kind, cid = (raw_id[0], raw_id[1:]) if raw_id[:1] in ("g", "s", "e") else ("e", raw_id)
            grade_only = kind == "g"
            if item.result.type != "succeeded":
                why[cid] = f"{item.result.type} {getattr(getattr(item.result, 'error', None), 'message', '')}".strip(); continue
            msg = item.result.message; usage_add(usage, msg)
            texts = [x.text for x in msg.content if x.type == "text" and "{" in x.text]
            raw = texts[-1] if texts else ""
            if not raw: why[cid] = f"no JSON in answer, stop_reason={msg.stop_reason}, blocks={[x.type for x in msg.content]}"; continue
            raw = raw.replace("```json", "").replace("```", "")
            try:
                j = json.loads(raw[raw.find("{"): raw.rfind("}") + 1])
                scores = {k: v for k, v in (j.get("scores") or {}).items() if k in by_id}
                keep = [k for k, v in scores.items() if int(v.get("score", 0)) >= KEEP_MIN]
                prev = rec.get(cid, {})
                merged = merge_ids(prev.get(P["iap_ids"]), keep)
                old_primary = (prev.get(P["primary"]) or "").strip()
                if old_primary and not is_only_name(old_primary) and merged: primary = old_primary
                else: primary = pick_primary(keep, by_id) if keep else ""
                new_lines = [f"{by_id[k]['name']}: {v.get('score')}" + (f" - {v['reason']}" if v.get("reason") else "")
                             for k, v in sorted(scores.items(), key=lambda kv: -int(kv[1].get("score", 0)))]
                score_lines = "\n".join(new_lines + other_score_lines(prev.get(P["scores"])))
                names_all = _names()
                props = {P["desc"]: (j.get("description") or "")[:65000], P["kws"]: (j.get("keywords") or "")[:65000],
                    P["iap_ids"]: ";".join(merged), P["iap_names"]: ", ".join(names_all.get(k, k) for k in merged),
                    P["scores"]: score_lines[:65000], P["primary"]: primary, P["s2"]: ST["done"], P["s2d"]: now_ms(),
                    P["enriched"]: "true"}
                if grade_only:
                    props = {k: props[k] for k in (P["iap_ids"], P["iap_names"], P["scores"], P["primary"], P["s2"], P["s2d"])}
                    updates.append({"id": cid, "properties": props}); failed.discard(cid); continue
                old = rec.get(cid, {})
                if kind == "e" and j.get("website_valid") is False and not (j.get("acquired_by") or "").strip():
                    # site loaded but belongs to someone else: redo this company once with web search
                    updates.append({"id": cid, "properties": {P["s2"]: ST["pending"], P["s2b"]: "search"}})
                    retry.append(cid); failed.discard(cid); continue
                # headquarters: write the real country; outside the allowed countries -> untick everything
                hq = (j.get("hq_country") or "").strip()
                if hq:
                    if norm_country(hq) not in HQ_ALLOWED:
                        props[P["iap_ids"]] = ""; props[P["iap_names"]] = ""; props[P["primary"]] = ""; props[P["scores"]] = ""
                        outside.append((cid, hq))
                    if norm_country(hq) != norm_country(old.get("country")):
                        props["country"] = canonical_country(hq)
                        country_fixed.append((cid, old.get("country") or "", props["country"]))
                # city / state: fill only when blank on the record
                for key, prop in (("hq_city", "city"), ("hq_state", "state")):
                    val = (j.get(key) or "").strip()
                    if val and not (old.get(prop) or "").strip():
                        props[prop] = val[:250]; loc_filled.append((cid, prop, props[prop]))
                # website: overwrite only when the one on record is not valid and Claude found a different one
                new_site = (j.get("website") or "").strip()
                buyer = (j.get("acquired_by") or "").strip()
                if buyer:
                    new_site = ""                                  # acquired: keep the original website
                    if not old.get(PARENT_PROP): props[PARENT_PROP] = buyer[:250]
                    acquired.append((cid, buyer))
                if new_site and not homepage_ok(new_site): rejected.append((cid, new_site)); new_site = ""
                own = OWN_MAP.get((j.get("ownership") or "").strip().lower())
                if own and OWN_VALID and own not in OWN_VALID: own = None   # option renamed/removed in HubSpot
                if own and not old.get(OWN_PROP): props[OWN_PROP] = own
                owner = (j.get("owner_name") or "").strip()
                if owner and own in ("Private Equity", "Private Sub", "Public Sub") and not old.get(PARENT_PROP) and PARENT_PROP not in props:
                    props[PARENT_PROP] = owner[:250]
                if j.get("website_valid") is False and new_site and clean_domain(new_site) != clean_domain(old.get("website") or old.get("domain")):
                    u = urlparse(new_site if "://" in new_site else "https://" + new_site)
                    props["website"] = f"{u.scheme}://{u.netloc}"
                    props["domain"] = clean_domain(new_site)
                    fixed_sites.append((cid, old.get("website") or old.get("domain") or "", props["website"]))
                # linkedin: write only a confirmed company URL; if none confirmed, keep what is there
                li = norm_li(j.get("linkedin"))
                if li and j.get("linkedin_evidence") in ("site", "search") and li != norm_li(old.get("linkedin_company_page")):
                    props["linkedin_company_page"] = li
                updates.append({"id": cid, "properties": props})
                failed.discard(cid)
            except Exception as e: why[cid] = f"{type(e).__name__}: {e}"[:200] + f" (stop_reason={msg.stop_reason}, {len(raw)} chars)"
        for c in failed:
            if c not in why: why[c] = "no result returned for this company"
        updates += [{"id": c, "properties": {P["s2"]: ST["failed"]}} for c in failed]
        hs_update(updates)
        log(f"stage 2: batch {bid} written {len(updates) - len(failed)}, failed {len(failed)}, "
            f"websites corrected {len(fixed_sites)}, linkedin written {sum(1 for u in updates if 'linkedin_company_page' in u['properties'])}")
        for c in list(failed)[:50]: log(f"  stage 2 failed {c}: {why.get(c, '')}")
        log("stage 2: cost", usage_line(usage, STAGE2_MODEL))
        for cid, a, b in fixed_sites[:50]: log(f"  website fixed {cid}: {a or '(none)'} -> {b}")
        for cid, b in acquired[:50]: log(f"  acquired {cid}: by {b} (website kept)")
        for cid, u in rejected[:50]: log(f"  website not used {cid}: {u} (not a homepage on its own domain)")
        if retry: log(f"stage 2: {len(retry)} companies had a website that is not theirs, queued for a web-search pass")
        log(f"stage 2: headquarters outside US/Canada {len(outside)} (all campaigns unticked), country corrected {len(country_fixed)}")
        for cid, hq in outside[:50]: log(f"  outside {cid}: {hq}")
        for cid, a, b in country_fixed[:50]: log(f"  country {cid}: {a or '(blank)'} -> {b}")
        log(f"stage 2: blank city/state filled {len(loc_filled)}")
        for cid, prop, v in loc_filled[:50]: log(f"  {prop} {cid}: (blank) -> {v}")
        log(f"stage 2: ownership filled {sum(1 for u in updates if OWN_PROP in u['properties'])}, "
            f"parent filled {sum(1 for u in updates if PARENT_PROP in u['properties'])}")

def remaining():
    """What is still in flight: (stage-1 submitted, stage-2 submitted, segment members waiting).
    Segment mode reads the records directly, because HubSpot search lags a few seconds behind writes."""
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        s1 = s2 = waiting = 0
        for r in batch_read(scope_ids(), [P["s1"], P["s1d"], P["s2"]]):
            pr = r["properties"]
            if pr.get(P["s1"]) == ST["submitted"]: s1 += 1
            if pr.get(P["s2"]) == ST["submitted"]: s2 += 1
            if pr.get(P["s2"]) in (ST["pending"], ST["failed"]) or pr.get(P["s1"]) in (ST["pending"], ST["failed"]) \
               or (not pr.get(P["s1d"]) and pr.get(P["s1"]) != ST["submitted"]):
                waiting += 1
        return s1, s2, waiting
    time.sleep(15)
    s1 = search([{"filters": [{"propertyName": P["s1"], "operator": "EQ", "value": ST["submitted"]}]}], ["name"], 100000)
    s2 = search([{"filters": [{"propertyName": P["s2"], "operator": "EQ", "value": ST["submitted"]}]}], ["name"], 100000)
    return len(s1), len(s2), 0

def cycle(iaps):
    sync_campaign_property(ALL_IAPS)                 # every active campaign keeps its option
    sync_campaign_property(ALL_IAPS, P["primary"])
    s2_pickup(iaps)
    s1_pickup(iaps)
    s2_submit(iaps)
    s1_submit(iaps)

def run_contacts():
    if not RUN_CONTACTS: log("contacts: skipped (RUN_CONTACTS=false)"); return
    if DRY_RUN: log("contacts: skipped in DRY_RUN"); return
    worker = os.path.abspath(CONTACT_WORKER)
    if not os.path.exists(worker): log("contacts: enrich_contacts.py not found at", worker, "- set CONTACT_WORKER"); return
    for i in [x for x in ALL_IAPS if x["id"] in ONLY_IAPS]:
        log(f"contacts: running enrich_contacts.py for {i['name']} (LIMIT {CONTACT_LIMIT})")
        env = dict(os.environ, CAMPAIGN=i["id"], LIMIT=str(CONTACT_LIMIT))
        code = subprocess.call([sys.executable, worker], env=env, cwd=os.path.dirname(worker))
        log(f"contacts: {i['name']} finished with exit code {code}")
        if code == 2: log("contacts: stopping (missing setup, key or credits - see the lines above)"); break

if __name__ == "__main__":
    ALL_IAPS = load_iaps()
    CAMPAIGN_NAMES.update(load_campaign_names())
    iaps = [i for i in ALL_IAPS if i["id"] in ONLY_IAPS]
    log("matching only against:", ", ".join(i["name"] for i in iaps) or "(none found - check ONLY_IAPS and that the campaigns are active with an IAP)")
    log("RESEARCH_TAG:", RESEARCH_TAG or "(none)", "| SCOPE:", SCOPE if not RESEARCH_TAG else "(ignored)",
        "| LEAD_STATUSES:", ",".join(LEAD_STATUSES) or "(any)", "| NO_LAST_CONTACT:", NO_LAST_CONTACT, "| RERUN:", RERUN,
        "| active IAPs:", len(iaps), "| models:", STAGE1_MODEL, "/", STAGE2_MODEL, "| LOOP:", LOOP)
    if not iaps: log("no active IAP with iap_description - stopping"); sys.exit()
    load_ownership_options()
    start = time.time()
    while True:
        cycle(iaps)
        if not LOOP or DRY_RUN: break
        a1, a2, w = remaining()
        log(f"in flight: stage 1 batches {a1} companies, stage 2 batches {a2} companies, segment waiting {w}")
        if a1 == 0 and a2 == 0 and w == 0: log("everything finished"); run_contacts(); break
        if (time.time() - start) / 3600 > MAX_HOURS: log("MAX_HOURS reached, stopping; run again to continue"); break
        log(f"waiting {POLL_MINUTES:g} minutes for batches"); time.sleep(POLL_MINUTES * 60)
    log("done")
