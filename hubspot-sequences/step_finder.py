"""step_finder.py — for every contact in a segment, find the last sequence step they received and the step to restart at,
then create one static segment per sequence + restart step, ready for "Enroll in sequence" (up to 50 at a time).

  cd ~/Documents/"Werner Partners"/hubspot/hubspot-sequences && source ../.env
  python3 step_finder.py                       # segment 969, writes the CSV and creates the segments
  CREATE_SEGMENTS=false python3 step_finder.py # CSV only

How the step is found (no use of the contact's step property):
  each sequence email logged in HubSpot has the sequence ID (hs_sequence_id) and the template it used (hs_template_id).
  The sequence's own step list (Sequences API) says which step uses which template. The contact's latest email from
  that sequence -> its step. Restart step = the step after it (it can be a call/task step).
  If a template is not found in the step list, the step falls back to counting that sequence's email steps.
Step numbers in the file are as HubSpot shows them in the sequence editor (step 1 = first step).

Needs scopes: crm.lists.read, crm.lists.write (segments), crm.objects.contacts.read, sales-email-read,
automation.sequences.read, crm.objects.owners.read.
"""
import os, csv, time, datetime, requests

TOKEN = os.environ["HUBSPOT_TOKEN"]
SEGMENT = os.getenv("SEGMENT", "969")
DOMAIN = os.getenv("SENDER_DOMAIN", "meetwernerpartners.com").lower()
CREATE = os.getenv("CREATE_SEGMENTS", "true").lower() == "true"
HS = "https://api.hubapi.com"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
S = requests.Session()
TODAY = datetime.date.today()

def call(method, path, **kw):
    for a in range(7):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        return r
    return r

def hs(method, path, **kw):
    r = call(method, path, **kw)
    if r.status_code >= 400: print(r.status_code, path, r.text[:300])
    r.raise_for_status(); return r.json() if r.text else {}

def members(list_id):
    ids, after = [], None
    while True:
        res = hs("GET", f"/crm/v3/lists/{list_id}/memberships", params={"limit": 250, **({"after": after} if after else {})})
        ids += [str(x["recordId"]) for x in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return list(dict.fromkeys(ids))

def contacts(ids):
    out = {}
    for i in range(0, len(ids), 100):
        res = hs("POST", "/crm/v3/objects/contacts/batch/read", json={"inputs": [{"id": x} for x in ids[i:i+100]],
                 "properties": ["email", "firstname", "lastname", "hs_latest_sequence_enrolled", "hs_sequences_is_enrolled"]})
        for r in res.get("results", []): out[str(r["id"])] = r["properties"]
    return out

def sequence_emails(cid):
    """The contact's logged emails that came from a sequence and from DOMAIN, newest first."""
    ids, after = [], None
    while True:
        res = hs("GET", f"/crm/v4/objects/contacts/{cid}/associations/emails", params={"limit": 500, **({"after": after} if after else {})})
        ids += [str(x["toObjectId"]) for x in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    out = []
    for i in range(0, len(ids), 100):
        res = hs("POST", "/crm/v3/objects/emails/batch/read", json={"inputs": [{"id": x} for x in ids[i:i+100]],
                 "properties": ["hs_sequence_id", "hs_template_id", "hs_timestamp", "hs_email_from_email"]})
        for r in res.get("results", []):
            p = r["properties"]
            if p.get("hs_sequence_id") and (p.get("hs_email_from_email") or "").lower().endswith("@" + DOMAIN): out.append(p)
    return sorted(out, key=lambda p: p.get("hs_timestamp") or "", reverse=True)

_owner = {}
def user_of(email):
    if email and email not in _owner:
        r = call("GET", "/crm/v3/owners", params={"email": email, "limit": 1})
        _owner[email] = str(((r.json().get("results") or [{}])[0] if r.ok else {}).get("userId") or "")
    return _owner.get(email, "")

_seq = {}
def sequence(seq_id, sender):
    """Step list of a sequence: {'name', 'steps': [(stepOrder, actionType, templateId)]}, or None if HubSpot refuses."""
    if seq_id in _seq: return _seq[seq_id]
    r = call("GET", f"/automation/sequences/2026-09/serviceaccounts/sequences/{seq_id}")
    if not r.ok and user_of(sender):
        r = call("GET", f"/automation/sequences/2026-09/{seq_id}", params={"userId": user_of(sender)})
    if not r.ok:
        print(f"  sequence {seq_id}: could not read its steps ({r.status_code} {r.text[:160]})"); _seq[seq_id] = None; return None
    j = r.json()
    steps = []
    for s in j.get("steps") or []:
        tpl = str(((s.get("emailPattern") or {}).get("templateId")) or ((s.get("taskPattern") or {}).get("templateId")) or "")
        steps.append((int(s.get("stepOrder", 0)), s.get("actionType") or "", tpl))
    _seq[seq_id] = {"name": j.get("name") or "", "steps": sorted(steps)}
    return _seq[seq_id]

STEPS_CSV = os.getenv("STEPS_CSV", "")   # reuse a step file already written: skips the 13-minute read, only creates the segments
if STEPS_CSV:
    rows = list(csv.DictReader(open(STEPS_CSV)))
    for r in rows:
        if str(r["restart_at_step"]).isdigit(): r["restart_at_step"] = int(r["restart_at_step"])
    out = STEPS_CSV
    print(f"using {STEPS_CSV}: {len(rows)} contacts (no reading from HubSpot)")
else:
    ids = members(SEGMENT)
    props = contacts(ids)
    print(f"segment {SEGMENT}: {len(ids)} contacts. Reading their sequence emails (about 1 second each)...")
    rows = []
    for n, cid in enumerate(ids, 1):
        p = props.get(cid, {})
        mails = sequence_emails(cid)
        seq_id = str(p.get("hs_latest_sequence_enrolled") or (mails[0]["hs_sequence_id"] if mails else ""))
        mine = [m for m in mails if str(m["hs_sequence_id"]) == seq_id]
        sender = (mine[0]["hs_email_from_email"] if mine else "").lower()
        seq = sequence(seq_id, sender) if seq_id else None
        last_step = restart = ""; how = ""
        if seq and mine:
            steps = [s for s in seq["steps"] if s[1] != "FINISH_ENROLLMENT"]
            by_tpl = {s[2]: s[0] for s in steps if s[2]}
            orders = [by_tpl[str(m.get("hs_template_id") or "")] for m in mine if str(m.get("hs_template_id") or "") in by_tpl]
            if orders:
                last_order = max(orders); how = "template"
            else:   # fall back: the Nth email step, N = emails received from this sequence
                email_steps = [s[0] for s in steps if s[1] == "EMAIL"]
                last_order = email_steps[min(len(mine), len(email_steps)) - 1] if email_steps else None; how = "email count"
            if last_order is not None:
                pos = [s[0] for s in steps].index(last_order)
                last_step = pos + 1
                restart = pos + 2 if pos + 1 < len(steps) else "finished"
        rows.append({"contact_id": cid, "email": p.get("email") or "", "first_name": p.get("firstname") or "", "last_name": p.get("lastname") or "",
                     "sequence_id": seq_id, "sequence_name": (seq or {}).get("name", ""), "steps_in_sequence": len([s for s in (seq or {}).get("steps", []) if s[1] != "FINISH_ENROLLMENT"]) or "",
                     "sequence_emails_received": len(mine), "last_step_received": last_step, "restart_at_step": restart, "step_found_by": how,
                     "enrolled_now": p.get("hs_sequences_is_enrolled") or "", "sender_email": sender})
        if n % 50 == 0: print(f"  {n}/{len(ids)}")

    out = f"sequence_steps_{SEGMENT}_{TODAY}.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
ok = [r for r in rows if isinstance(r["restart_at_step"], int)]
print(f"{out}: restart step found {len(ok)}, finished the whole sequence {sum(1 for r in rows if r['restart_at_step'] == 'finished')}, "
      f"no step found {sum(1 for r in rows if r['restart_at_step'] == '')}, still enrolled now {sum(1 for r in rows if r['enrolled_now'] == 'true')}")

if CREATE and ok:
    groups = {}
    for r in ok: groups.setdefault((r["sequence_id"], r["restart_at_step"]), []).append(r)
    print(f"creating {len(groups)} static segments (one per sequence + restart step)...")
    for (sid, step), rs in sorted(groups.items(), key=lambda g: (g[1][0]["sequence_name"], g[0][1])):
        name = f"Re-enroll | {rs[0]['sequence_name'] or sid} | start step {step} | {TODAY:%b %d, %Y}"[:250]
        lst = hs("POST", "/crm/v3/lists", json={"name": name, "objectTypeId": "0-1", "processingType": "MANUAL"})
        lid = (lst.get("list") or lst).get("listId")
        cids = [r["contact_id"] for r in rs]
        for i in range(0, len(cids), 250):
            hs("PUT", f"/crm/v3/lists/{lid}/memberships/add", json=cids[i:i+250])
        print(f"  {len(cids):4d} contacts  segment {lid}: {name}")
    print("Enroll each segment from HubSpot: open it, select up to 50 contacts, Enroll in sequence, choose the start step shown in the name.")
