"""
One-time backfill of two custom date-time properties on contacts and companies:
  last_contact_date   — latest real conversation: OutboundSync reply, HubSpot sales-email reply,
                        incoming logged email, call with a connected outcome
  last_outreach_date  — latest attempt: OutboundSync send, outbound sent logged email,
                        call with any other outcome except Wrong number
Reads every call, email and contact once, keeps the max per contact in memory,
rolls contacts up to their companies (max across associated contacts, plus activities
associated directly to the company), then writes both dates in batches of 100.

Env:  HUBSPOT_TOKEN   service key with contacts/companies read+write and calls/emails read
      DRY_RUN=true    compute and print counts, write nothing
      WRITE_CONTACTS / WRITE_COMPANIES = false to skip one side
"""
import os, sys, time, json
import requests

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
WRITE_CONTACTS = os.getenv("WRITE_CONTACTS", "true").lower() == "true"
WRITE_COMPANIES = os.getenv("WRITE_COMPANIES", "false").lower() == "true"  # companies now use property sync

P_CONTACT = "last_contact_date"
P_OUTREACH = "last_outreach_date"

# Call outcomes that count as a real conversation. Everything else (incl. Other, empty) = attempt,
# except Wrong number, which is skipped entirely (bad number, not an attempt on the person).
DIRECT_OUTCOMES = {"call successful", "connected", "contact made – no interest",
                   "contact made - no interest", "meeting booked"}
SKIP_OUTCOMES = {"wrong number"}

HS = "https://api.hubapi.com"
hs_headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

def hs(method, path, **kw):
    for attempt in range(6):
        r = requests.request(method, HS + path, headers=hs_headers, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503):
            time.sleep(min(2 ** attempt, 30)); continue
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json() if r.text else {}
    r.raise_for_status()

def to_ms(v):
    """HubSpot datetime values arrive as ISO strings or epoch ms strings."""
    if not v: return None
    if v.isdigit(): return int(v)
    try:
        from datetime import datetime, timezone
        return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None

# ---------- in-memory accumulators ----------
contact_direct, contact_attempt = {}, {}     # contact_id -> ms
company_direct, company_attempt = {}, {}     # company_id -> ms
contact_company = {}                         # contact_id -> primary company id

def bump(d, key, ms):
    if ms and (key not in d or ms > d[key]): d[key] = ms

def page_objects(obj, properties, associations=None, limit=100):
    """Iterate every record of an object via the list endpoint with cursor paging."""
    params = {"limit": limit, "properties": ",".join(properties)}
    if associations: params["associations"] = ",".join(associations)
    after, n = None, 0
    while True:
        if after: params["after"] = after
        data = hs("GET", f"/crm/v3/objects/{obj}", params=params)
        for rec in data.get("results", []):
            n += 1
            yield rec
        after = data.get("paging", {}).get("next", {}).get("after")
        if n % 20000 < limit: log(f"  {obj}: {n:,} read")
        if not after: break
    log(f"{obj}: {n:,} total")

def assoc_ids(rec, obj):
    out = []
    for a in rec.get("associations", {}).get(obj, {}).get("results", []):
        out.append(str(a["id"]))
    return out

def apply(rec, ms, direct):
    cd, ca = (contact_direct, company_direct) if direct else (contact_attempt, company_attempt)
    for cid in assoc_ids(rec, "contacts"): bump(cd, cid, ms)
    for coid in assoc_ids(rec, "companies"): bump(ca, coid, ms)

# ---------- 1. calls ----------
def load_dispositions():
    m = {}
    for d in hs("GET", "/calling/v1/dispositions") or []:
        m[d["id"]] = d["label"]
    log("dispositions:", m)
    return m

def scan_calls(dispo):
    for rec in page_objects("calls", ["hs_timestamp", "hs_call_disposition"], ["contacts", "companies"]):
        p = rec["properties"]
        ms = to_ms(p.get("hs_timestamp"))
        if not ms: continue
        label = dispo.get(p.get("hs_call_disposition") or "", "").strip().lower()
        if label in SKIP_OUTCOMES: continue
        apply(rec, ms, direct=label in DIRECT_OUTCOMES)

# ---------- 2. emails ----------
def scan_emails():
    for rec in page_objects("emails", ["hs_timestamp", "hs_email_direction", "hs_email_status"], ["contacts", "companies"]):
        p = rec["properties"]
        ms = to_ms(p.get("hs_timestamp"))
        if not ms: continue
        direction, status = p.get("hs_email_direction"), p.get("hs_email_status")
        if direction == "INCOMING_EMAIL":
            apply(rec, ms, direct=True)
        elif direction == "EMAIL" and status == "SENT":
            apply(rec, ms, direct=False)
        # FORWARDED_EMAIL and non-SENT statuses (drafts, bounces, scheduled) are ignored

# ---------- 3. tasks ----------
def scan_tasks():
    for rec in page_objects("tasks", ["hs_timestamp", "hs_task_type", "hs_task_status", "hs_task_completion_date"], ["contacts", "companies"]):
        p = rec["properties"]
        if p.get("hs_task_type") != "CALL" or p.get("hs_task_status") != "COMPLETED": continue
        ms = to_ms(p.get("hs_task_completion_date")) or to_ms(p.get("hs_timestamp"))
        if ms: apply(rec, ms, direct=False)

# ---------- 4. contact-level properties + company link ----------
def scan_contacts():
    props = ["os_last_reply_time", "hs_sales_email_last_replied", "os_last_sent_time", "associatedcompanyid"]
    for rec in page_objects("contacts", props):
        cid, p = rec["id"], rec["properties"]
        bump(contact_direct, cid, to_ms(p.get("os_last_reply_time")))
        bump(contact_direct, cid, to_ms(p.get("hs_sales_email_last_replied")))
        bump(contact_attempt, cid, to_ms(p.get("os_last_sent_time")))
        if p.get("associatedcompanyid"): contact_company[cid] = str(p["associatedcompanyid"])

def roll_up():
    for cid, coid in contact_company.items():
        bump(company_direct, coid, contact_direct.get(cid))
        bump(company_attempt, coid, contact_attempt.get(cid))

# ---------- 5. write ----------
def write(obj, direct, attempt):
    ids = set(direct) | set(attempt)
    log(f"{obj}: {len(ids):,} records to update")
    if DRY_RUN:
        log("DRY_RUN — not writing"); return
    batch, n = [], 0
    for i in sorted(ids):
        props = {}
        if i in direct: props[P_CONTACT] = str(direct[i])
        if i in attempt: props[P_OUTREACH] = str(attempt[i])
        batch.append({"id": i, "properties": props})
        if len(batch) == 100:
            hs("POST", f"/crm/v3/objects/{obj}/batch/update", json={"inputs": batch}); n += len(batch); batch = []
            if n % 10000 == 0: log(f"  {obj}: {n:,} written")
    if batch:
        hs("POST", f"/crm/v3/objects/{obj}/batch/update", json={"inputs": batch}); n += len(batch)
    log(f"{obj}: {n:,} written")

def main():
    t0 = time.time()
    dispo = load_dispositions()
    scan_calls(dispo)
    scan_emails()
    scan_contacts()
    roll_up()
    log(f"contacts with direct contact: {len(contact_direct):,}  with attempt: {len(contact_attempt):,}")
    log(f"companies with direct contact: {len(company_direct):,}  with attempt: {len(company_attempt):,}")
    if WRITE_CONTACTS: write("contacts", contact_direct, contact_attempt)
    if WRITE_COMPANIES: write("companies", company_direct, company_attempt)
    log(f"done in {(time.time()-t0)/60:.1f} min")

if __name__ == "__main__":
    main()
