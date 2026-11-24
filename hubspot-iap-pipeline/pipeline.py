"""
HubSpot IAP pipeline — two matching passes in one run.

  ALWAYS Batch API (half price). Websites are read on this computer, 8 at a time. Anthropic works through each batch
           job in parallel on its side; answers usually come back in under 1 hour (at most 24).

  STAGE 1 (cheap, Haiku, Batch API): match companies in SCOPE against every active IAP using the
           website summary written by stage 2 (enrichment_website_summary) when there is one, otherwise the
           description already on the record. Ticks enrichment_iap_campaigns.
  A company whose stage status is Failed is picked up again on the next run, in either stage.
  COMPANIES: only Company Record Type = Seller Prospect and Active/Inactive not Inactive and Is Buyer not true, ALWAYS, in both stages.
  CAMPAIGNS USED: Campaign Active = Yes, IAP Pipeline = Active, iap_description filled and matching its master doc.
  MASTER IAP: a campaign is used only when its iap_description matches iap_description_hash, the fingerprint the
           Campaign Config sheet sync writes when it pushes the campaign's IAP doc (the master). Campaigns edited in
           HubSpot, or never pushed from a doc, are skipped and listed in the log.
  IAP TRACKING: enrichment_iap_evaluated holds one line per IAP a company was checked against,
           "campaign ID - YYYY-MM-DD HH:MM UTC - stage 1|stage 2". A company already Done is checked again only against
           active IAPs that are not on its list, or whose IAP description changed after the date on its line; ticks and
           scores for the other IAPs are kept. Companies Done before this tracking existed (no list) are treated as
           checked against every IAP whose description last changed before their iap_match_date.
           Stage 2 grades only the ticked IAPs that stage 1 ticked since the last grading (line says "stage 1").
  STAGE 2 (deep, Haiku, Batch API): companies STAGE 1 ticked, plus companies with no description (graded against
           every active IAP, since stage 1 had nothing to read). Crawls the website,
           writes enrichment_description + enrichment_keywords only when the record has none, grades the IAPs stage 1 ticked 1-5 with a
           reason for scores 3-5 only (1-2 get no reason, to save cost), REPLACES the tick set with every IAP scored KEEP_MIN (default 3) or more.
           enrichment_iap_scores is append-only: each grading adds one line per IAP, "campaign ID - score - reason - YYYY-MM-DD",
           and older lines are never deleted.
           iap_primary_campaign: among the ticked IAPs, the highest score wins; a tie goes to the best campaign_priority
           (1 beats 5), then to the campaign with fewer matched companies in HubSpot at that moment (counts include the
           ticks made during this run).
           When the website is confirmed as the company's own, Company name is replaced by the name the company uses on
           its site, cleaned of Inc/LLC/Ltd and extras; the old name goes to legal_name (only if legal_name is empty).
           Industry: one of HubSpot's standard industry options, written when blank or not a standard option.
           Industry group (hs_industry_group): the specific niche, free text, written when blank.
           Number of Employees: written only when blank and the website states a number.
           enrichment_website_summary: structured bullet summary of the website (product/service lines, end markets,
           locations, certifications, ownership/history signals...); headings the site does not cover are left out.
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

Env: HUBSPOT_TOKEN, ANTHROPIC_API_KEY, RESEARCH_TAG, SCOPE, STAGE1_MODEL, STAGE2_MODEL, STAGE1_BATCH (15000),
     STAGE2_MAX (500 per run), MAX_TEXT (6000), DRY_RUN, RERUN=true (ignore Done status, re-match everything in scope)
"""
import os, sys, re, json, time, hashlib, csv, concurrent.futures as cf
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import phonenumbers
import anthropic

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
SCOPE = os.getenv("SCOPE", "list:908").strip().lower()   # default: HubSpot segment 908
RESEARCH_TAG = os.getenv("RESEARCH_TAG", "").strip()
TAG_PROP = "research_tags__c"
RECORD_TYPE = ("company_record_type", "Seller Prospect")   # ALWAYS: only Seller Prospect companies enter the pipeline (both stages)
ACTIVE_PROP = "active_inactive__c"                           # ALWAYS: companies set to Inactive never enter (empty or Active do)
BUYER_PROP = "is_buyer"                                      # ALWAYS: companies with Is Buyer = true never enter
def eligible_company(p):
    return (p.get(RECORD_TYPE[0]) == RECORD_TYPE[1] and (p.get(ACTIVE_PROP) or "") != "Inactive"
            and str(p.get(BUYER_PROP) or "").lower() != "true")
STAGE1_MODEL = os.getenv("STAGE1_MODEL", "claude-haiku-5-5")
STAGE2_MODEL = os.getenv("STAGE2_MODEL", "claude-haiku-5-5")   # Haiku for both stages to keep cost down
STAGE1_BATCH = int(os.getenv("STAGE1_BATCH", "15000"))   # companies sent to stage 1 per round
STAGE2_MAX = int(os.getenv("STAGE2_MAX", "2000"))      # companies sent to stage 2 per round (keeps one batch job well under Anthropic's 256 MB limit)
MAX_TEXT = int(os.getenv("MAX_TEXT", "6000"))
STORE_TEXT = 60000
PACK = 25
CONCURRENCY = 8
WEB_SEARCH_TOOL = os.getenv("WEB_SEARCH_TOOL", "web_search_20250305")
WEB_SEARCH_MAX = int(os.getenv("WEB_SEARCH_MAX", "2"))   # stage 2: at most 2 web searches per company ($10 per 1,000 searches)
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

def pick_primary(ticks, by_id, scores, added=()):
    """Primary among the ticked active IAPs: highest score first; tie -> best campaign_priority; tie -> fewer matched
    companies; residual tie -> name. scores: {campaign id: score}. added: ticks new in this run (counted in MATCHED)."""
    ticks = [k for k in ticks if k in by_id]
    for k in added: MATCHED[k] = MATCHED.get(k, 0) + 1
    if not ticks: return ""
    return min(ticks, key=lambda k: (-int(scores.get(k, 0) or 0), by_id[k]["tier"], MATCHED.get(k, 0), by_id[k]["name"]))

SUFFIX = r"(?:incorporated|inc|llc|l\.l\.c|corporation|corp|ltd|limited|lp|l\.p|llp|pllc|plc|pc|p\.c|co)"
def normalize_name(name):
    """Same cleaning as push_to_smartlead.py: 'CCT Plastics, Inc. dba CCT Precision Machining' -> 'CCT Precision Machining';
    'Atlantic Precision Spring, Inc. (APS)' -> 'Atlantic Precision Spring'. Returns the input unchanged when cleaning would leave nothing."""
    n = (name or "").strip()
    if not n: return n
    m = re.search(r"\b(?:d/b/a|dba|d\.b\.a\.?)\s+(.+)$", n, re.I)
    if m: n = m.group(1)
    for _ in range(3):
        n = re.sub(r"\s*\([^()]*\)\s*$", "", n).strip()
        n = re.sub(rf"[\s,]+{SUFFIX}\.?$", "", n, flags=re.I).strip(" ,.-")
    n = re.sub(r"\s{2,}", " ", n).strip(" ,.-")
    return n if n and not re.fullmatch(SUFFIX + r"\.?", n, re.I) else (name or "").strip()

IND_PROP, SUBIND_PROP, EMP_PROP = "industry", "hs_industry_group", "numberofemployees"
IND_STD = {}   # label -> value, HubSpot's standard industry options (values in CAPS_WITH_UNDERSCORES), loaded at start
def load_industry_options():
    """HubSpot's ~150 standard industry options. The imported custom options (lowercase duplicates, truncated labels) are skipped."""
    global IND_STD
    try:
        p = hs("GET", f"/crm/v3/properties/companies/{IND_PROP}")
        IND_STD = {o["label"]: o["value"] for o in p.get("options", []) if re.fullmatch(r"[A-Z][A-Z0-9_]*", o["value"]) and not o.get("hidden")}
        log(f"Setup: loaded HubSpot's Industry list ({len(IND_STD)} choices).")
    except Exception as e:
        IND_STD = {}; log("setup: could not read HubSpot's Industry dropdown choices, so Industry will not be filled this run:", e)

def score_lines(scores, by_id):
    """One line per graded IAP: 'campaign ID - score - reason - YYYY-MM-DD', highest score first."""
    day = time.strftime("%Y-%m-%d")
    out = []
    for k, v in sorted(scores.items(), key=lambda kv: -int(kv[1].get("score", 0))):
        reason = re.sub(r"\s+", " ", str(v.get("reason") or "")).strip() or ("no reason (scores 1-2 get none)" if int(v.get("score", 0) or 0) < 3 else "no reason given")
        out.append(f"{k} - {v.get('score')} - {reason} - {day}")
    return out

def append_scores(old, new_lines):
    """Append new lines to the existing text; when over HubSpot's 65,000-character limit, the oldest lines are dropped."""
    lines = [x for x in (old or "").split("\n") if x.strip()] + new_lines
    while lines and len("\n".join(lines)) > 65000: lines.pop(0)
    return "\n".join(lines)

EVAL_PROP = "enrichment_iap_evaluated"
EVAL_RE = re.compile(r"^(\d+)\s+-\s+(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}))?(?:\s*UTC)?(?:\s+-\s+(stage [12]))?\s*$")

def ms_of(v):
    """HubSpot datetime (ISO string or epoch ms) -> epoch ms; 0 when empty/unreadable."""
    v = str(v or "").strip()
    if not v: return 0
    if v.isdigit(): return int(v)
    try: return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError: return 0

def parse_eval(text):
    """{campaign id: (epoch ms of the check, 'stage 1' | 'stage 2' | '')}"""
    out = {}
    for line in str(text or "").split("\n"):
        m = EVAL_RE.match(line.strip())
        if not m: continue
        dt = datetime.strptime(m.group(2) + " " + (m.group(3) or "00:00"), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        out[m.group(1)] = (int(dt.timestamp() * 1000), m.group(4) or "")
    return out

def eval_text(old_text, ids, stage):
    """Add or refresh one line per campaign id checked now; lines for other campaigns are kept."""
    ids = set(ids)
    if not ids: return old_text or ""
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [l for l in str(old_text or "").split("\n") if l.strip() and not (EVAL_RE.match(l.strip()) and EVAL_RE.match(l.strip()).group(1) in ids)]
    lines += [f"{k} - {stamp} - {stage}" for k in sorted(ids)]
    return "\n".join(lines)[:65000]

def due_iaps(props, iaps):
    """Active IAPs this company still has to be checked against (see IAP TRACKING above)."""
    if RERUN: return list(iaps)
    ev = parse_eval(props.get(EVAL_PROP))
    if not ev:
        md = ms_of(props.get(P["s1d"]))
        if not md: return list(iaps)                                   # never matched: every active IAP
        return [i for i in iaps if i["changed_ms"] > md]               # matched before tracking existed: fallback on dates
    return [i for i in iaps if i["id"] not in ev or i["changed_ms"] > ev[i["id"]][0]]

def load_iap_change_dates(iaps):
    """changed_ms per IAP: when its iap_description last changed to a different text (HubSpot property history)."""
    for i in iaps: i["changed_ms"] = 0
    by = {i["id"]: i for i in iaps}
    ids = list(by)
    for n in range(0, len(ids), 50):
        try:
            res = hs("POST", f"/crm/v3/objects/{CAMPAIGN_OBJ}/batch/read",
                     json={"inputs": [{"id": x} for x in ids[n:n+50]], "properties": ["iap_description"],
                           "propertiesWithHistory": ["iap_description"]})
        except Exception as e:
            log("setup: could not read when each campaign's IAP text last changed, so companies are not re-checked for changed IAPs this run:", e); return
        for r in res.get("results", []):
            hist = sorted((r.get("propertiesWithHistory") or {}).get("iap_description") or [], key=lambda h: ms_of(h.get("timestamp")), reverse=True)
            changed = 0
            for k, h in enumerate(hist):                                # newest version whose text differs from the one before it
                older = hist[k + 1]["value"] if k + 1 < len(hist) else None
                if older is None or (h.get("value") or "").strip() != (older or "").strip():
                    changed = ms_of(h.get("timestamp")); break
            if str(r["id"]) in by: by[str(r["id"])]["changed_ms"] = changed

def latest_scores(text, by_id):
    """{campaign id: latest score} from enrichment_iap_scores (new 'ID - score - reason - date' lines, or old 'Name: score' lines)."""
    names = {i["name"]: k for k, i in by_id.items()}
    out = {}
    for line in str(text or "").split("\n"):
        line = line.strip()
        m = re.match(r"^(\d+)\s+-\s+([1-5])\s+-", line)
        if m: out[m.group(1)] = int(m.group(2)); continue
        m = re.match(r"^(.*?):\s*([1-5])(?:\s+-|$)", line)
        if m and m.group(1).strip() in names and names[m.group(1).strip()] not in out: out[names[m.group(1).strip()]] = int(m.group(2))
    return out

def load_matched_counts(iaps):
    """How many companies have each active campaign ticked right now (HubSpot search total, one call per campaign)."""
    for i in iaps:
        try:
            res = hs("POST", f"/crm/objects/{V}/companies/search", json={"filterGroups": [{"filters": [
                {"propertyName": P["iap_ids"], "operator": "CONTAINS_TOKEN", "value": i["id"]}]}], "properties": ["name"], "limit": 1})
            MATCHED[i["id"]] = int(res.get("total", 0))
        except Exception as e:
            MATCHED.setdefault(i["id"], 0); log("could not count the companies already matched to", i["name"], "- counted as 0:", e)

P = dict(desc="enrichment_description", kws="enrichment_keywords", text="enrichment_website_text",
         iap_ids="enrichment_iap_campaigns", iap_names="enrichment_iap_matches",
         scores="enrichment_iap_scores", primary="iap_primary_campaign", primary_name="iap_primary_campaign_name", s1d="iap_match_date", s2d="enrichment_date", enriched="enriched",
         s1="iap_match_status", s1b="iap_match_batch_id", s2="enrichment_status", s2b="enrichment_batch_id",
         reasons="iap_match_reasons", summary="enrichment_website_summary")
GRATA_TAG = "grata_research_tag"
EFFORT = os.getenv("EFFORT", "low")   # Haiku 5.5 thinks by default and the thinking counts against max_tokens; low effort keeps it short   # stage 1 writes a verdict + reason for the IAPs named by this tag, matched or not
ST = dict(pending="Pending", submitted="Submitted", done="Done", failed="Failed")
SRC = ["enrichment_description", "description", "about_us", "company_description__c"]  # description fields, first non-empty wins
S1_SRC = ["enrichment_website_summary"] + SRC   # stage 1 reads the stage 2 website summary first, then the description fields
S1_LIMIT = {"enrichment_website_summary": 4000}   # characters sent to stage 1 per source (default 1500)
S1_FACTS = ["city", "state", "country", "numberofemployees"]   # sent to stage 1 for geography / size criteria
S1_SITE = ["website", "domain", "enrichment_website_text"]      # used when a company has no description

HS = "https://api.hubapi.com"; V = "2026-09"
HSH = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
client = anthropic.Anthropic()
S = requests.Session()

ERRORS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "errors.csv")
RUN_STARTED = time.strftime("%Y-%m-%d %H:%M:%S")   # when this run started; written on every error row
CSV_HEADER = ["run_started", "date_time", "stage", "company_id", "company_name", "hubspot_link", "error"]
def record_error(stage, cid, error, name=""):
    """Adds one row to errors.csv: when, stage, company, HubSpot link, what went wrong. Never stops the run."""
    try:
        if os.path.exists(ERRORS_CSV):   # file from before run_started existed: keep it under another name
            with open(ERRORS_CSV, newline="") as fh: head = next(csv.reader(fh), [])
            if head != CSV_HEADER: os.rename(ERRORS_CSV, ERRORS_CSV.replace(".csv", f"-old-{time.strftime('%Y%m%d-%H%M%S')}.csv"))
        new = not os.path.exists(ERRORS_CSV)
        with open(ERRORS_CSV, "a", newline="") as fh:
            w = csv.writer(fh)
            if new: w.writerow(CSV_HEADER)
            w.writerow([RUN_STARTED, time.strftime("%Y-%m-%d %H:%M:%S"), stage, cid or "", name or "",
                        f"https://app.hubspot.com/contacts/243636394/record/0-2/{cid}" if cid else "", re.sub(r"\s+", " ", str(error))[:1000]])
    except Exception: pass

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    for attempt in range(7):
        try: r = S.request(method, HS + path, headers=HSH, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout): time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code >= 400:
            raise requests.HTTPError(f"HubSpot said {r.status_code}: {r.text[:300]}", response=r)
        return r.json() if r.text else {}
    raise requests.HTTPError(f"HubSpot still busy or down after 7 tries ({r.status_code}): {r.text[:300]}", response=r)

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

OPTS = {}   # dropdown property -> values HubSpot accepts (filled by sync_campaign_property)

def drop_unknown_options(updates):
    """Campaign IDs that are no longer options of the dropdown (e.g. a deleted campaign) are removed before saving,
    otherwise HubSpot refuses the whole company."""
    for u in updates:
        pr = u.get("properties", {})
        for prop, vals in OPTS.items():
            if prop not in pr or not pr[prop]: continue
            keep = [x for x in str(pr[prop]).split(";") if x in vals]
            if len(keep) != len(str(pr[prop]).split(";")): pr[prop] = ";".join(keep)

def add_primary_names(updates):
    """Whenever iap_primary_campaign (an ID) is written, write the campaign name next to it in iap_primary_campaign_name."""
    for u in updates:
        props = u.get("properties", {})
        if P["primary"] in props:
            v = str(props[P["primary"]] or "").strip()
            props[P["primary_name"]] = CAMPAIGN_NAMES.get(v, "" if (not v or v.isdigit()) else v)

def hs_update(updates):
    """Batch update; if HubSpot rejects a batch (400), retry record by record so one bad value cannot block the rest."""
    drop_unknown_options(updates)
    add_primary_names(updates)
    if DRY_RUN or not updates: return
    for i in range(0, len(updates), 100):
        chunk = updates[i:i+100]
        try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": chunk})
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != 400: raise
            log("HubSpot refused to save a group of companies:", e.response.text[:300], "- trying them one at a time")
            for u in chunk:
                try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": [u]})
                except requests.HTTPError as e2:
                    if e2.response is None or e2.response.status_code != 400: raise
                    full = e2.response.text; msg = full[:200]
                    log("  HubSpot refused company", u["id"], msg, "- saving its other fields without the refused one")
                    record_error("HubSpot save", u["id"], f"HubSpot refused: {msg}")
                    # drop the property HubSpot complains about and write the rest
                    bad = [k for k in u["properties"] if f'"{k}"' in full or f"'{k}'" in full]
                    rest = {k: v for k, v in u["properties"].items() if k not in bad}
                    if bad and rest:
                        try: hs("POST", f"/crm/objects/{V}/companies/batch/update", json={"inputs": [{"id": u["id"], "properties": rest}]})
                        except requests.HTTPError:
                            log("  HubSpot still refused company", u["id"], "- nothing saved for it")
                            record_error("HubSpot save", u["id"], "HubSpot refused it twice - nothing saved")

def search(filters, props, limit, sorts=None):
    """Page a company search, always by record ID (hs_object_id > last one seen).
    HubSpot's normal paging stops at 10,000 results (400 error), so it is not used. sorts is ignored (always ID order)."""
    rows, last = [], 0
    while len(rows) < limit:
        idf = {"propertyName": "hs_object_id", "operator": "GT", "value": str(last)}
        groups = [{**g, "filters": list(g.get("filters", [])) + [idf]} for g in filters] if filters else [{"filters": [idf]}]
        size = min(limit - len(rows), 200)
        res = hs("POST", f"/crm/objects/{V}/companies/search", json={"filterGroups": groups, "properties": props, "limit": size,
                 "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}]})
        got = res.get("results", [])
        rows += got
        if not got or not res.get("paging", {}).get("next"): break
        last = int(got[-1]["id"])
    seen, out = set(), []
    for r in rows:
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
        if missing: log("setup: these Ownership choices do not exist in HubSpot, so they will not be filled:", missing)
    except Exception as e:
        OWN_VALID = set(); log("setup: could not read HubSpot's Ownership choices, so Ownership will not be filled this run:", e)

def iap_hash(text):
    """SHA-256 hex of the IAP text, line endings normalised and trimmed (same as iapHash_ in campaign_config_sync.gs)."""
    t = re.sub(r"\r\n?", "\n", str(text or "")).strip()
    return hashlib.sha256(t.encode("utf-8")).hexdigest()

def load_iaps():
    body = {"filterGroups": [{"filters": [{"propertyName": "campaign_active", "operator": "EQ", "value": "true"},
                                          {"propertyName": "iap_pipeline", "operator": "EQ", "value": "Active"},
                                          {"propertyName": "iap_description", "operator": "HAS_PROPERTY"}]}],
            "properties": ["campaign_name", "iap_description", "iap_keywords", "campaign_priority", "iap_description_hash"], "limit": 100}
    iaps, after, not_master = [], None, []
    while True:
        if after: body["after"] = after
        res = hs("POST", f"/crm/objects/{V}/{CAMPAIGN_OBJ}/search", json=body)
        for r in res.get("results", []):
            p = r["properties"]
            if not (p.get("campaign_name") and p.get("iap_description")): continue
            if iap_hash(p["iap_description"]) != (p.get("iap_description_hash") or "").strip().lower():
                not_master.append(p["campaign_name"].strip()); continue   # edited in HubSpot or never pushed from its master doc
            kws = [k.strip().lower() for k in re.split(r"[,\n;]", p.get("iap_keywords") or "") if k.strip()]
            iaps.append({"id": str(r["id"]), "name": p["campaign_name"].strip(), "desc": p["iap_description"].strip(),
                         "kws": kws, "tier": prio(p.get("campaign_priority"))})
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    if not_master:
        log(f"Skipped {len(not_master)} campaign(s), IAP text differs from its doc: " + "; ".join(sorted(not_master)) + ". Fix: Config sheet > HubSpot Sync > Push every IAP doc to HubSpot")
    load_iap_change_dates(iaps)
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
    OPTS[prop_name] = {o["value"] for o in options}   # values HubSpot accepts for this property
    if changed and not DRY_RUN:
        hs("PATCH", f"/crm/v3/properties/companies/{prop_name}", json={"options": options})
        log(f"Setup: campaign dropdown {prop_name} updated ({len(active)} campaigns).")

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
    return f"about ${usd:.2f} (run total ${RUN_COST['usd']:.2f}); {acc['n']} answers, {acc['search']} web searches."

def new_usage(): return {"inp": 0, "out": 0, "cread": 0, "cwrite": 0, "search": 0, "n": 0}

BATCHES = {}   # Batch API jobs this run is waiting for: job id -> stage, companies, time sent
STATS = {}     # per campaign, this run: checked, stage 1 ticked, stage 2 graded, stage 2 kept

def stat(cid, key, n=1):
    STATS.setdefault(cid, {"checked": 0, "s1": 0, "graded": 0, "kept": 0})[key] += n

def s2_stat(scores, props):
    for k in scores: stat(k, "graded")
    for k in str(props.get(P["iap_ids"]) or "").split(";"):
        if k in scores: stat(k, "kept")

def send_requests(reqs, stage, companies=0):
    """Sends all of one stage's requests to Anthropic as one Batch API job (half price). Returns the job id."""
    bid = client.messages.batches.create(requests=reqs).id
    BATCHES[bid] = {"stage": stage, "companies": companies or len(reqs), "sent": time.time()}
    return bid

def progress(w, start):
    """Short status after each check: % done per stage, rough time left, cost so far."""
    left = []
    for st in (1, 2):
        jobs = [(bid, j) for bid, j in BATCHES.items() if j["stage"] == st]
        if not jobs:
            log(f"Stage {st}: nothing at Anthropic."); continue
        done = total = comps = 0; secs = 0
        for bid, j in jobs:
            try: rc = client.messages.batches.retrieve(bid).request_counts
            except Exception: continue
            d = rc.succeeded + rc.errored + rc.canceled + rc.expired
            done += d; total += d + rc.processing; comps += j["companies"]
            secs = max(secs, time.time() - j["sent"])
        pct = done / total if total else 0
        if pct >= 1: eta = "finished, saving at next check"
        elif pct > 0: eta = f"about {secs * (1 - pct) / pct / 60:.0f} min left"; left.append(secs * (1 - pct) / pct)
        else: eta = "no answers back yet (usually under 1 hour)"
        log(f"Stage {st}: {pct:.0%} done (about {int(pct * comps):,} of {comps:,} companies), {eta}.")
    if w: log(f"Not sent yet: {w:,} companies.")
    log(f"Run so far: {(time.time() - start) / 60:.0f} min, about ${RUN_COST['usd']:.2f}."
        + (f" At least {max(left) / 60:.0f} more min; stage 2 for new matches comes after stage 1." if left else ""))

def campaign_report(iaps):
    """Per campaign: what this run did, and the totals in HubSpot now."""
    log("Campaigns - this run: checked / stage 1 ticked / stage 2 kept of graded | HubSpot now: ticked / primary")
    load_matched_counts(iaps)
    for i in iaps:
        c = STATS.get(i["id"], {"checked": 0, "s1": 0, "graded": 0, "kept": 0})
        try:
            prim = f'{int(hs("POST", f"/crm/objects/{V}/companies/search", json={"filterGroups": [{"filters": [{"propertyName": P["primary"], "operator": "EQ", "value": i["id"]}]}], "properties": ["name"], "limit": 1}).get("total", 0)):,}'
        except Exception: prim = "?"
        log(f"  {i['name']}: {c['checked']:,} / {c['s1']:,} / {c['kept']:,} of {c['graded']:,} | {MATCHED.get(i['id'], 0):,} / {prim}")

def get_answers(bid, ids, stage, status_prop):
    """Claude's answers for one job, or None when they are not ready or cannot be read."""
    back = lambda: hs_update([{"id": c, "properties": {status_prop: ST["pending"]}} for c in ids])
    if str(bid).startswith("threads-"):   # left by the short-lived threads version
        log(f"stage {stage}: {len(ids)} companies were left half-way by an earlier run that stopped early. Putting them back in the queue.")
        back(); return None
    try:
        b = client.messages.batches.retrieve(bid)
        if bid not in BATCHES:   # sent by an earlier run
            BATCHES[bid] = {"stage": stage, "companies": len(ids), "sent": b.created_at.timestamp() if getattr(b, "created_at", None) else time.time()}
    except anthropic.NotFoundError:
        log(f"stage {stage}: Batch API job {bid} cannot be found with this API key. Putting its {len(ids)} companies back in the queue.")
        for c in ids: record_error(f"stage {stage}", c, f"Anthropic job {bid} not found with this API key - put back in the queue")
        back(); return None
    if b.processing_status != "ended":
        return None   # still running; progress() shows how far it is
    BATCHES.pop(bid, None)
    return client.messages.batches.results(bid)

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
    seller = [{"propertyName": RECORD_TYPE[0], "operator": "EQ", "value": RECORD_TYPE[1]}]
    if RESEARCH_TAG: return seller + [{"propertyName": TAG_PROP, "operator": "CONTAINS_TOKEN", "value": RESEARCH_TAG}]
    if SCOPE == "all": return seller
    days = float(SCOPE)
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    return seller + [{"propertyName": "createdate", "operator": "GTE", "value": str(since)},
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

def batch_read(ids, props):
    rows = []
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/objects/{V}/companies/batch/read",
                 json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": props})
        rows += res.get("results", [])
    return rows

def list_rows(props, iaps):
    """Segment members that still need stage 1: never matched, Pending/Failed, or Done with IAPs still to check."""
    ids = list_members(SCOPE[5:])
    rows = batch_read(ids, props + [P["s1"], P["s1d"], EVAL_PROP, P["iap_ids"], RECORD_TYPE[0], ACTIVE_PROP, BUYER_PROP])
    keep, recheck, not_seller = [], 0, 0
    for r in rows:
        if not eligible_company(r["properties"]): not_seller += 1; continue   # not a Seller Prospect, or Inactive: never enters
        st, dt = r["properties"].get(P["s1"]), r["properties"].get(P["s1d"])
        if st == ST["submitted"]: continue
        if not RERUN and dt and st not in (ST["pending"], ST["failed"]):   # already matched: only if some IAPs are still due
            if not due_iaps(r["properties"], iaps): continue
            recheck += 1
        if RERUN and dt:
            try:
                ms = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp() * 1000) if not dt.isdigit() else int(dt)
                if ms >= int(RERUN_SINCE): continue
            except ValueError: pass
        keep.append(r)
    keep.sort(key=lambda r: int(r["id"]))
    log(f"Segment {SCOPE[5:]}: {len(ids):,} companies, {not_seller} left out, {len(keep):,} to check in stage 1 ({recheck:,} only for new or changed campaigns).")
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
                             f"{c['text']}" for c in companies)
    limit = f"Consider only these IAP IDs for these companies: {', '.join(allowed_ids)}\n\n" if allowed_ids else ""
    return f"{limit}COMPANIES:\n{comp_block}"


def s1_submit(iaps):
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        rows = list_rows(["name", GRATA_TAG] + S1_SRC + S1_FACTS + S1_SITE, iaps)
    else:
        rows = None
    scope = scope_filters() if rows is None else []
    status_f = [{"propertyName": P["s1"], "operator": "NOT_HAS_PROPERTY"}] if not RERUN else []
    pending_f = [{"propertyName": P["s1"], "operator": "IN", "values": [ST["pending"], ST["failed"]]}]   # Failed is retried
    if not RERUN:
        groups = [{"filters": scope + pending_f}, {"filters": scope + status_f}]
        latest = max([i["changed_ms"] for i in iaps] + [0])
        if latest:   # Done companies matched before the newest IAP change; due_iaps() below decides per company
            groups.append({"filters": scope + [{"propertyName": P["s1"], "operator": "EQ", "value": ST["done"]},
                                               {"propertyName": P["s1d"], "operator": "LT", "value": str(latest)}]})
    else:
        # re-match each company once per RERUN_SINCE: anything not matched since that moment
        notsub = [{"propertyName": P["s1"], "operator": "NEQ", "value": ST["submitted"]}]
        groups = [{"filters": scope + notsub + [{"propertyName": P["s1d"], "operator": "LT", "value": RERUN_SINCE}]},
                  {"filters": scope + notsub + [{"propertyName": P["s1d"], "operator": "NOT_HAS_PROPERTY"}]}]
    if rows is None:
        rows = search(groups, ["name", GRATA_TAG, P["s1"], P["s1d"], EVAL_PROP, P["iap_ids"], RECORD_TYPE[0], ACTIVE_PROP, BUYER_PROP] + S1_SRC + S1_FACTS + S1_SITE,
                      STAGE1_BATCH, [{"propertyName": "hs_object_id", "direction": "ASCENDING"}])
        before = len(rows); rows = [r for r in rows if eligible_company(r["properties"])]
        if before != len(rows): log(f"Stage 1: {before - len(rows)} left out (Inactive or Is Buyer).")
    if not rows: log("Stage 1: nothing to check."); return
    # companies with no description: use stored website text, or crawl the site now (free, no model call)
    def site_text(r):
        p = r["properties"]
        if any(p.get(x) for x in S1_SRC): return r["id"], None, False
        if p.get("enrichment_website_text"): return r["id"], p["enrichment_website_text"], False
        dom = clean_domain(p.get("domain") or p.get("website"))
        site = fetch_site(p.get("website"), dom) if (p.get("website") or dom) else None
        return r["id"], (site["text"] if site else None), bool(site)
    with cf.ThreadPoolExecutor(CONCURRENCY) as ex: got = list(ex.map(site_text, rows))
    from_site = {cid: t for cid, t, _ in got if t}
    store = [{"id": cid, "properties": {P["text"]: t[:STORE_TEXT]}} for cid, t, crawled in got if crawled and t]
    nodesc = sum(1 for r in rows if not any(r["properties"].get(x) for x in S1_SRC))
    log(f"Stage 1: {nodesc:,} have no description; {len(from_site):,} use their website text instead.")
    skip, grouped = [], {}
    for r in rows:
        p = r["properties"]
        src = next((x for x in S1_SRC if p.get(x)), None)
        text = p[src][:S1_LIMIT.get(src, 1500)] if src else (from_site.get(r["id"]) or "")[:1500] or None
        due = due_iaps(p, iaps)
        if not due:   # nothing new to check: mark Done and move its match date so the next run does not pull it again
            skip.append({"id": r["id"], "properties": {P["s1"]: ST["done"], P["s1d"]: now_ms()}}); continue
        cands = candidate_iaps(text, due) if text else []
        if not text:   # no description on record: skip stage 1, send to stage 2 to crawl the site and grade the due IAPs
            skip.append({"id": r["id"], "properties": {P["s1"]: ST["done"], P["s1d"]: now_ms(), P["s2"]: ST["pending"]}}); continue
        if not cands:
            for i in due: stat(i["id"], "checked")
            skip.append({"id": r["id"], "properties": {P["s1"]: ST["done"], P["s1d"]: now_ms(),
                                                       EVAL_PROP: eval_text(p.get(EVAL_PROP), [i["id"] for i in due], "stage 1")}}); continue
        tagged = [x for x in tag_iaps(p.get(GRATA_TAG), iaps) if any(i["id"] == x for i in due)]
        cands = cands + [i for i in due if i["id"] in tagged and i not in cands]   # always judge the tag's IAPs
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
    log(f"Stage 1: {len(submitted):,} companies go to Claude ({len(reqs)} questions). {nodesc} have nothing to read, so they go straight to stage 2. {len(skip) - nodesc} need no check.")
    if DRY_RUN or not reqs: hs_update(skip); return
    hs_update(store); hs_update(skip)
    bid = send_requests(reqs, 1, len(submitted))
    hs_update([{"id": c, "properties": {P["s1"]: ST["submitted"], P["s1b"]: bid}} for c in submitted])
    log(f"Stage 1: sent to Anthropic ({len(submitted):,} companies, job {bid}).")

def s1_pickup(iaps):
    """Finds companies waiting for a stage 1 Batch API job and saves the answers of the finished jobs."""
    by_id = {i["id"]: i["name"] for i in iaps}
    rows = search([{"filters": [{"propertyName": P["s1"], "operator": "EQ", "value": ST["submitted"]}]}],
                  ["name", P["s1b"], P["iap_ids"], P["iap_names"], P["s1d"], EVAL_PROP] + SRC, 100000)
    by_batch = {}
    for r in rows: by_batch.setdefault(r["properties"].get(P["s1b"]), {})[r["id"]] = r["properties"]
    by_batch.pop(None, None)
    ticked_now = []
    for bid, comps in by_batch.items():
        items = get_answers(bid, list(comps), 1, P["s1"])
        if items is None: continue
        results, req_errors, usage = {}, [], new_usage()
        for item in items:
            if item.result.type != "succeeded":
                req_errors.append(f"{item.custom_id}: {item.result.type} {getattr(getattr(item.result, 'error', None), 'message', '')}"); continue
            msg = item.result.message; usage_add(usage, msg)
            raw = "\n".join(x.text for x in msg.content if x.type == "text")
            try:
                parsed, bad = parse_stage1_answer(raw); results.update(parsed)
                if bad: req_errors.append(f"{item.custom_id}: JSON partly broken, salvaged {len(parsed)} companies, lost {bad}, stop_reason={msg.stop_reason}")
            except Exception as e:
                req_errors.append(f"{item.custom_id}: unreadable JSON ({type(e).__name__}), stop_reason={msg.stop_reason}, {len(raw)} chars")
        for line in req_errors[:20]: log("  stage 1: one question to Claude went wrong:", line)
        log("stage 1 cost:", usage_line(usage, STAGE1_MODEL))
        updates = []
        for cid, props in comps.items():
            if cid not in results:
                record_error("stage 1", cid, "Claude's answer for this company was missing or unreadable - retried next run"
                             + (f" (request problems: {'; '.join(req_errors[:3])})" if req_errors else ""), props.get("name"))
                updates.append({"id": cid, "properties": {P["s1"]: ST["failed"]}}); continue
            res = results[cid]
            if isinstance(res, list): res = {"iaps": res}          # old answer format
            if not isinstance(res, dict): res = {"iaps": []}
            checked = {i["id"] for i in due_iaps(props, iaps)}          # the IAPs this company was sent with
            new_ids = [str(x) for x in (res.get("iaps") or []) if str(x) in by_id and str(x) in checked]
            old_ticks = set(str(props.get(P["iap_ids"]) or "").split(";")) - {""}
            ticks = (old_ticks - checked) | set(new_ids)                # ticks for IAPs not checked now are kept
            for k in checked: stat(k, "checked")
            for k in new_ids: stat(k, "s1")
            props_out = {P["iap_ids"]: ";".join(sorted(ticks)), P["iap_names"]: ", ".join(by_id[x] for x in sorted(ticks) if x in by_id),
                         P["s1"]: ST["done"], P["s1d"]: now_ms(), EVAL_PROP: eval_text(props.get(EVAL_PROP), checked, "stage 1")}
            reasons = {str(k): str(v).strip() for k, v in (res.get("reasons") or {}).items() if str(k) in by_id and str(v).strip()}
            props_out[P["reasons"]] = "\n".join(f"{by_id[k]}: {'MATCH' if k in new_ids else 'NO'} - {v}" for k, v in reasons.items())[:65000]
            desc = (res.get("description") or "").strip()
            if desc and not any(props.get(x) for x in SRC): props_out[P["desc"]] = desc[:65000]   # no description on record: fill it
            if new_ids: props_out[P["s2"]] = ST["pending"]; ticked_now.append(cid)   # hand to stage 2
            elif not ticks: props_out.update({P["primary"]: ""})   # nothing ticked: clear the primary (score history is kept)
            updates.append({"id": cid, "properties": props_out})
        hs_update(updates)
        log(f"Stage 1 saved: {len(updates):,} companies. {sum(1 for u in updates if u['properties'].get(P['s2']) == ST['pending']):,} matched and go to stage 2. "
            f"{sum(1 for u in updates if u['properties'].get(P['s1']) == ST['failed'])} failed (retried next run).")
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
STEP 5 - NAME, INDUSTRY, SIZE. Only from the company's own valid website:
"company_name": the name the company uses for itself on its site (logo, header, About, footer), without legal suffixes
(Inc, LLC, Ltd, Corp, Co), without a city, region or subsidiary note, and without a "dba" prefix (keep the name after "dba").
Return "" if the website is not valid.
"industry": exactly one label from the INDUSTRY LIST below, copied exactly, or "" if none fits.
"sub_industry": the specific niche in plain words, 2-6 words (e.g. "commercial HVAC service"), or "".
"employees": the number of employees ONLY when the website states it (e.g. "over 120 employees" -> 120); otherwise null.
Never estimate it.
STEP 6 - WEBSITE SUMMARY. From the company's own website (or the search evidence for the corrected website), write
"website_summary" as plain text with these headings, in this order, each followed by short "- " bullet lines:
Overview (1-2 sentences: what they do and how they make money), Product lines, Service lines, End markets (industries and
customer types), Notable customers (only customers the site names), Locations (HQ plus every office, plant or branch, with
state or province), Service area (local, regional, national or international, and where), Capabilities and assets
(processes, facilities, fleet, equipment), Certifications and licenses (e.g. ISO 9001, AS9100, UL), Business model signals
(manufacturer / distributor / service provider, recurring revenue such as maintenance contracts, B2B or B2C),
Ownership and history signals (year founded, family-owned, generation, leaders named, acquisitions, parent company or
private equity owner). Leave out any heading the evidence says nothing about. Never guess. Keep it short: at most 3 bullet
lines per heading, each under 15 words, and under 200 words in total. Return "" if there is no
valid website or evidence.
STEP 7 - Describe the company (only when DESCRIPTION ON RECORD is none) and grade it ONLY against the IAP IDs named in the request, using the valid website
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
  "description": "2-4 plain sentences: what the company actually does, who it serves, where it operates. EMPTY when DESCRIPTION ON RECORD is not none",
  "keywords": "10-20 comma-separated keywords: services, industry, niche, geography. EMPTY when KEYWORDS ON RECORD is not none",
  "company_name": "name as the company writes it on its own site, cleaned, or empty",
  "industry": "one label from the INDUSTRY LIST, or empty",
  "sub_industry": "specific niche, 2-6 words, or empty",
  "employees": number or null,
  "website_summary": "structured summary with the STEP 6 headings, or empty",
  "scores": {"<IAP ID>": {"score": 1-5, "reason": "one short line saying why, ONLY when the score is 3, 4 or 5; empty for 1 and 2"}} }
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
{"scores": {"<IAP ID>": {"score": 1-5, "reason": "one short line saying why, ONLY when the score is 3, 4 or 5; empty for 1 and 2"}} }
5 = clearly fits every core criterion; 4 = fits, one minor gap; 3 = plausible but unproven;
2 = weak; 1 = does not fit or violates an exclusion.
SCORING RULE: grade only on what a website or public source can show (what the company does, its services, customers,
geography, business model, ownership type when stated, employee count). Criteria tagged [Client] or that cannot be checked
publicly (EBITDA, revenue, margins, revenue mix, customer concentration, union status, bonding, litigation, management depth)
are NEUTRAL: never lower a score because one of them is unverified, and never write "unverified" criteria as the reason for
a lower score. A company that fits every checkable criterion scores 5 even when every [Client] criterion is unknown."""

def s2_system(iaps, grade_only=False):
    if grade_only: return f"{GRADE_RULES}\n\nACQUISITION PROFILES (IAPs):\n{iap_block(iaps)}"
    return f"{S2_RULES}\n\nINDUSTRY LIST:\n{'; '.join(sorted(IND_STD))}\n\nACQUISITION PROFILES (IAPs):\n{iap_block(iaps)}"

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
KEYWORDS ON RECORD: {'yes' if (c['p'].get(P['kws']) or '').strip() else 'none'}
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

S2_PROPS = ["name", "domain", "website", "phone", "linkedin_company_page", "city", "state", "country", P["iap_ids"], P["s1d"], EVAL_PROP, P["kws"]] + SRC

def s2_submit(iaps):
    props_needed = S2_PROPS + [P["s2"], P["s2b"], P["enriched"], P["text"], RECORD_TYPE[0], ACTIVE_PROP, BUYER_PROP,
                             OWN_PROP, PARENT_PROP, P["scores"], "legal_name", IND_PROP, SUBIND_PROP, EMP_PROP]   # last line: needed to save answers right away (threads)
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        members = batch_read(list_members(SCOPE[5:]), props_needed)
        rows = sorted([r for r in members if r["properties"].get(P["s2"]) in (ST["pending"], ST["failed"])], key=lambda r: int(r["id"]))[:STAGE2_MAX]
    else:
        rows = search([{"filters": [{"propertyName": P["s2"], "operator": "IN", "values": [ST["pending"], ST["failed"]]}]}],
                      props_needed, STAGE2_MAX, [{"propertyName": "hs_object_id", "direction": "ASCENDING"}])
    others = [r for r in rows if not eligible_company(r["properties"])]
    if others:   # not a Seller Prospect, or Inactive: never enriched; status cleared so it is not picked up again
        log(f"Stage 2: {len(others)} left out (not Seller Prospect, Inactive or Is Buyer).")
        if not DRY_RUN: hs_update([{"id": r["id"], "properties": {P["s2"]: ""}} for r in others])
        rows = [r for r in rows if r not in others]
    if not rows: log("Stage 2: no company waiting."); return
    def crawl(r):
        p = r["properties"]
        domain = clean_domain(p.get("domain") or p.get("website"))
        return {"id": r["id"], "name": p.get("name") or domain, "domain": domain, "website": (p.get("website") or "").strip(),
                "phone": p.get("phone") or "", "li": p.get("linkedin_company_page") or "", "country": p.get("country") or "",
                "loc": ", ".join(x for x in (p.get("city"), p.get("state"), p.get("country")) if x),
                "desc": next((p[x] for x in SRC if p.get(x)), "")[:1500], "ticked": p.get(P["iap_ids"]) or "", "p": p,
                "retry_search": p.get(P["s2b"]) == "search",
                "grade_only": (p.get(P["enriched"]) or "").lower() == "true", "stored": p.get(P["text"]) or "",
                "site": fetch_site(p.get("website"), domain)
                        if (p.get("website") or domain) and (p.get(P["enriched"]) or "").lower() != "true" else None}
    with cf.ThreadPoolExecutor(CONCURRENCY) as ex: crawled = list(ex.map(crawl, rows))
    reqs, updates, dead, graded_only, searched = [], [], 0, 0, 0
    by_id = {i["id"]: i for i in iaps}
    for c in crawled:
        ev = parse_eval(c["p"].get(EVAL_PROP))
        # only what stage 1 ticked and stage 2 has not graded since (no tracking line yet = old record: grade every tick)
        cands = [by_id[x] for x in sorted(set(c["ticked"].split(";")) - {""}) if x in by_id and (not ev or ev.get(x, (0, "stage 1"))[1] != "stage 2")]
        if not cands and not c["desc"] and not c["grade_only"]:
            # no description, so stage 1 could not match: grade from the website every IAP not yet checked
            # (stage 1 has just set the match date, so the date fallback does not apply here)
            cands = due_iaps(c["p"], iaps) if ev else list(iaps)
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
        params = {"model": STAGE2_MODEL, "max_tokens": 9500 + 150 * len(cands), "output_config": {"effort": EFFORT},
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
    log(f"Stage 2: {len(reqs):,} companies go to Claude ({graded_only} score-only, {dead} with a dead website, {searched} with web search).")
    if DRY_RUN:
        if reqs: log("DRY RUN - start of the first question Claude would get:\n", reqs[0]["params"]["messages"][0]["content"][:2500])
        for u in updates[:50]:
            if "phone" in u["properties"]: log("  DRY RUN - would fill empty phone", u["id"], u["properties"]["phone"])
        return
    final = [{"id": u["id"], "properties": u["properties"]} for u in updates if u.get("_final")]
    hs_update(final)
    updates = [u for u in updates if not u.get("_final")]
    if not reqs: log("Stage 2: nothing to send."); return
    bid = send_requests(reqs, 2, len(reqs))
    for u in updates: u["properties"].update({P["s2"]: ST["submitted"], P["s2b"]: bid})
    hs_update(updates)
    log(f"Stage 2: sent to Anthropic ({len(reqs):,} companies, job {bid}).")

def s2_pickup(iaps):
    """Finds companies waiting for a stage 2 Batch API job and saves the answers of the finished jobs."""
    by_id = {i["id"]: i for i in iaps}
    load_matched_counts(iaps)
    rows = search([{"filters": [{"propertyName": P["s2"], "operator": "EQ", "value": ST["submitted"]}]}],
                  [P["s2b"], "website", "domain", "linkedin_company_page", OWN_PROP, PARENT_PROP, "country", "city", "state",
                   P["scores"], "name", "legal_name", IND_PROP, SUBIND_PROP, EMP_PROP, P["iap_ids"], EVAL_PROP], 100000)
    rec = {r["id"]: r["properties"] for r in rows}
    by_batch = {}
    for r in rows: by_batch.setdefault(r["properties"].get(P["s2b"]), []).append(r["id"])
    by_batch.pop(None, None)
    for bid, ids in by_batch.items():
        items = get_answers(bid, ids, 2, P["s2"])
        if items is None: continue
        updates, failed, fixed_sites, acquired, rejected, retry, outside, country_fixed = [], set(ids), [], [], [], [], [], []
        loc_filled, why, usage, renamed = [], {}, new_usage(), []
        for item in items:
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
                old = rec.get(cid, {})
                old_ticks = set(str(old.get(P["iap_ids"]) or "").split(";")) - {""}
                ticks = (old_ticks - set(scores)) | set(keep)            # ticks for IAPs not graded now are kept
                new_scores = append_scores(old.get(P["scores"]), score_lines(scores, by_id)) if scores else (old.get(P["scores"]) or "")
                primary = pick_primary(sorted(ticks), by_id, latest_scores(new_scores, by_id), added=set(keep) - old_ticks)
                props = {P["iap_ids"]: ";".join(sorted(ticks)), P["iap_names"]: ", ".join(by_id[k]["name"] for k in sorted(ticks) if k in by_id),
                    P["scores"]: new_scores, P["primary"]: primary, P["s2"]: ST["done"], P["s2d"]: now_ms(),
                    P["enriched"]: "true", EVAL_PROP: eval_text(old.get(EVAL_PROP), scores.keys(), "stage 2")}
                for key, prop in (("description", P["desc"]), ("keywords", P["kws"])):   # written only when Claude wrote one (none on record)
                    val = (j.get(key) or "").strip()
                    if val: props[prop] = val[:65000]
                if grade_only:
                    props = {k: props[k] for k in (P["iap_ids"], P["iap_names"], P["scores"], P["primary"], P["s2"], P["s2d"], EVAL_PROP)}
                    s2_stat(scores, props); updates.append({"id": cid, "properties": props}); failed.discard(cid); continue
                if kind == "e" and j.get("website_valid") is False and not (j.get("acquired_by") or "").strip():
                    # site loaded but belongs to someone else: redo this company once with web search
                    updates.append({"id": cid, "properties": {P["s2"]: ST["pending"], P["s2b"]: "search"}})
                    retry.append(cid); failed.discard(cid); continue
                # headquarters: write the real country; outside the allowed countries -> untick everything
                hq = (j.get("hq_country") or "").strip()
                if hq:
                    if norm_country(hq) not in HQ_ALLOWED:
                        props[P["iap_ids"]] = ""; props[P["iap_names"]] = ""; props[P["primary"]] = ""   # score history is kept
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
                # name: only from a confirmed own website; old name saved to legal_name when it changes and legal_name is empty
                site_ok = j.get("website_valid") is True and not buyer
                nm = normalize_name((j.get("company_name") or "").strip()) if site_ok else ""
                old_name = (old.get("name") or "").strip()
                if nm and nm != old_name:
                    props["name"] = nm[:250]
                    if old_name and not (old.get("legal_name") or "").strip(): props["legal_name"] = old_name
                    renamed.append((cid, old_name, props["name"]))
                # industry: standard option only; written when blank or the current value is not a standard option
                ind = IND_STD.get((j.get("industry") or "").strip())
                if ind and old.get(IND_PROP) not in IND_STD.values(): props[IND_PROP] = ind
                sub = (j.get("sub_industry") or "").strip()
                if sub and not (old.get(SUBIND_PROP) or "").strip(): props[SUBIND_PROP] = sub[:250]
                # website summary: structured bullets from the site; replaced on every full enrichment
                summ = (j.get("website_summary") or "").strip()
                if summ: props[P["summary"]] = summ[:65000]
                # employees: only when blank and the website states a number
                emp = j.get("employees")
                if isinstance(emp, (int, float)) and emp > 0 and not str(old.get(EMP_PROP) or "").strip(): props[EMP_PROP] = str(int(emp))
                # linkedin: write only a confirmed company URL; if none confirmed, keep what is there
                li = norm_li(j.get("linkedin"))
                if li and j.get("linkedin_evidence") in ("site", "search") and li != norm_li(old.get("linkedin_company_page")):
                    props["linkedin_company_page"] = li
                s2_stat(scores, props); updates.append({"id": cid, "properties": props})
                failed.discard(cid)
            except Exception as e: why[cid] = f"{type(e).__name__}: {e}"[:200] + f" (stop_reason={msg.stop_reason}, {len(raw)} chars)"
        for c in failed:
            if c not in why: why[c] = "no result returned for this company"
        updates += [{"id": c, "properties": {P["s2"]: ST["failed"]}} for c in failed]
        hs_update(updates)
        log(f"Stage 2 saved: {len(updates) - len(failed):,} companies. {len(failed)} failed (retried next run). Websites fixed: {len(fixed_sites)}.")
        for c in list(failed)[:10]: log(f"  stage 2 could not finish company {c}: {why.get(c, '')}")
        for c in failed: record_error("stage 2", c, f"{why.get(c, '')} - retried next run", rec.get(c, {}).get("name"))
        log("stage 2 cost:", usage_line(usage, STAGE2_MODEL))
        for cid, a, b in fixed_sites[:10]: log(f"  website replaced for company {cid}: {a or '(none)'} -> {b}")
        for cid, b in acquired[:10]: log(f"  company {cid} was bought by {b} (its website was left as is)")
        for cid, u in rejected[:10]: log(f"  company {cid}: Claude suggested {u}, not used because it is not the main page of its own website")
        if retry: log(f"Stage 2: {len(retry)} have someone else's website; queued again with web search.")
        log(f"Stage 2: {len(outside)} headquartered outside US/Canada (campaigns removed). Countries corrected: {len(country_fixed)}.")
        for cid, hq in outside[:10]: log(f"  company {cid} is headquartered in {hq}")
        for cid, a, b in country_fixed[:10]: log(f"  country changed for company {cid}: {a or '(blank)'} -> {b}")
        log(f"Stage 2: city/state filled: {len(loc_filled)}.")
        for cid, prop, v in loc_filled[:10]: log(f"  {prop} filled for company {cid}: {v}")
        log(f"Stage 2: Ownership filled {sum(1 for u in updates if OWN_PROP in u['properties'])}, parent filled {sum(1 for u in updates if PARENT_PROP in u['properties'])}.")
        log(f"Stage 2: names cleaned {len(renamed)}, Industry {sum(1 for u in updates if IND_PROP in u['properties'])}, "
            f"Industry group {sum(1 for u in updates if SUBIND_PROP in u['properties'])}, Employees {sum(1 for u in updates if EMP_PROP in u['properties'])}, "
            f"summaries {sum(1 for u in updates if P['summary'] in u['properties'])}.")
        for cid, a, b in renamed[:10]: log(f"  name changed for company {cid}: {a or '(blank)'} -> {b}")

def remaining():
    """What is still in flight: (stage-1 submitted, stage-2 submitted, segment members waiting).
    Segment mode reads the records directly, because HubSpot search lags a few seconds behind writes."""
    if SCOPE.startswith("list:") and not RESEARCH_TAG:
        s1 = s2 = waiting = 0
        for r in batch_read(list_members(SCOPE[5:]), [P["s1"], P["s1d"], P["s2"], RECORD_TYPE[0], ACTIVE_PROP, BUYER_PROP]):
            pr = r["properties"]
            if not eligible_company(pr): continue   # never enters, so never "waiting"
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

ERRORS = {}   # step name -> failures in a row

def step(name, fn, *args):
    """Runs one step. If it fails: one plain log line, full details in errors.log, the run goes on with the next step.
    Nothing is lost: companies not saved keep their status and are picked up again at the next check or run."""
    try:
        fn(*args); ERRORS[name] = 0
    except KeyboardInterrupt: raise
    except Exception as e:
        ERRORS[name] = ERRORS.get(name, 0) + 1
        log(f"ERROR in {name} ({ERRORS[name]} in a row): {type(e).__name__}: {str(e)[:300]}. Saved in errors.csv. Continuing.")
        record_error(name, "", f"{type(e).__name__}: {e} (whole step failed; full details in errors.log)")
        import traceback
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "errors.log"), "a") as fh:
            fh.write(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} {name}\n{traceback.format_exc()}")

def cycle(iaps):
    step("campaign dropdown update", sync_campaign_property, iaps)
    step("primary campaign dropdown update", sync_campaign_property, iaps, P["primary"])
    step("stage 2 save", s2_pickup, iaps)      # save answers of finished stage 2 jobs
    step("stage 1 save", s1_pickup, iaps)      # save answers of finished stage 1 jobs (what matched now waits for stage 2)
    step("stage 2 send", s2_submit, iaps)      # send the companies waiting for stage 2
    step("stage 1 send", s1_submit, iaps)      # send the companies that still need stage 1

if __name__ == "__main__":
    iaps = load_iaps()
    CAMPAIGN_NAMES.update(load_campaign_names())
    scope_txt = (f"companies with Research Tag {RESEARCH_TAG}" if RESEARCH_TAG
                 else f"segment {SCOPE[5:]}" if SCOPE.startswith("list:") else f"SCOPE {SCOPE}")
    log(f"START: {scope_txt}, Batch API, Claude {STAGE1_MODEL}." + (" DRY RUN (nothing saved, nothing paid)." if DRY_RUN else "") + (" RERUN on." if RERUN else ""))
    log(f"Campaigns ({len(iaps)}): " + "; ".join(i["name"] for i in iaps))
    if not iaps: log("STOP: no campaign can be used (each needs Campaign Active, IAP Pipeline Active, and IAP text equal to its doc)"); sys.exit()
    load_ownership_options()
    load_industry_options()
    start, last_w, stuck, rnd = time.time(), None, 0, 0
    while True:
        rnd += 1
        log(f"--- Check {rnd} ---")
        cycle(iaps)
        if not LOOP or DRY_RUN: break
        if any(n >= 3 for n in ERRORS.values()):
            bad = ", ".join(k for k, n in ERRORS.items() if n >= 3)
            log(f"STOP: {bad} failed 3 checks in a row. See errors.csv, fix, then run again (nothing is lost)."); break
        try:
            a1, a2, w = remaining()
            progress(w, start)
        except Exception as e:
            log(f"ERROR reading progress: {type(e).__name__}: {str(e)[:200]}. Trying again at next check.")
            a1, a2, w = 1, 0, 1
        if a1 == 0 and a2 == 0 and w == 0: log("All done."); break
        if (time.time() - start) / 3600 > MAX_HOURS:
            log(f"STOP: this run has gone on for more than {MAX_HOURS:g} hours (safety limit). Run it again to continue"); break
        if a1 == 0 and a2 == 0:   # nothing waiting at Anthropic, yet companies still to do
            if last_w is not None and w >= last_w:
                stuck += 1
                if stuck >= 2:
                    log(f"STOP: the last 2 rounds did not lower the {w} companies still to do (they keep failing). See the errors above, then run again"); break
            else: stuck = 0
            last_w = w
            log("Nothing at Anthropic; next check now."); continue
        log(f"Next check in {POLL_MINUTES:g} minutes."); time.sleep(POLL_MINUTES * 60)
    step("campaign report", campaign_report, iaps); log(f"DONE: cost about ${RUN_COST['usd']:.2f}, {(time.time() - start) / 60:.0f} minutes.")
