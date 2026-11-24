"""snapshot_sequences.py — save, for every contact in a segment, the HubSpot sequence they were in and who sent it,
so they can be re-enrolled later with reenroll_sequences.py.

Default segment: 969 "Re-enroll – meetwernerpartners.com sequence emails, last 14 days, no reply – Nov 21, 2026".

  source ../.env
  python3 snapshot_sequences.py                 # writes sequence_snapshot_<segment>_<date>.csv
  SEGMENT=969 SENDER_DOMAIN=meetwernerpartners.com python3 snapshot_sequences.py

Per contact it records:
  sequence_id     contact property "Last sequence enrolled" (hs_latest_sequence_enrolled)
  sequence_name   from the enrollment API when the contact is still enrolled, else from another contact in the same sequence
  enrolled_now    contact property "Currently in Sequence"
  sender_email    the sender of the contact's latest sequence email from SENDER_DOMAIN (email activity on the contact)
  user_id         the HubSpot user who owns that sender inbox (owners API); enrollments are made as this user
HubSpot's API has no field for the step a contact reached, so it is not in the file.
Needs scopes: crm.objects.contacts.read, crm.lists.read, crm.objects.owners.read, automation.sequences.read (and
sales-email-read for email activity).
"""
import os, csv, time, datetime, requests

TOKEN = os.environ["HUBSPOT_TOKEN"]
SEGMENT = os.getenv("SEGMENT", "969")
DOMAIN = os.getenv("SENDER_DOMAIN", "meetwernerpartners.com").lower()
HS = "https://api.hubapi.com"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
S = requests.Session()

def hs(method, path, ok404=False, **kw):
    for a in range(7):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        if ok404 and r.status_code == 404: return None
        if r.status_code >= 400: print(r.status_code, path, r.text[:300])
        r.raise_for_status(); return r.json() if r.text else {}
    r.raise_for_status()

def members(list_id):
    ids, after = [], None
    while True:
        params = {"limit": 250, **({"after": after} if after else {})}
        res = hs("GET", f"/crm/v3/lists/{list_id}/memberships", params=params)
        ids += [str(x["recordId"]) for x in res.get("results", [])]
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: return list(dict.fromkeys(ids))

PROPS = ["email", "firstname", "lastname", "hs_latest_sequence_enrolled", "hs_latest_sequence_enrolled_date",
         "hs_sequences_is_enrolled", "hs_latest_sequence_ended_date", "hubspot_owner_id"]

def contacts(ids):
    out = {}
    for i in range(0, len(ids), 100):
        res = hs("POST", "/crm/v3/objects/contacts/batch/read",
                 json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": PROPS})
        for r in res.get("results", []): out[str(r["id"])] = r["properties"]
    return out

def last_sender(cid):
    """Sender address of the contact's latest outgoing email from DOMAIN."""
    body = {"filterGroups": [{"filters": [{"propertyName": "associations.contact", "operator": "EQ", "value": cid},
                                          {"propertyName": "hs_email_direction", "operator": "EQ", "value": "EMAIL"}]}],
            "properties": ["hs_email_from_email", "hs_timestamp"], "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
            "limit": 20}
    for r in hs("POST", "/crm/v3/objects/emails/search", json=body).get("results", []):
        f = (r["properties"].get("hs_email_from_email") or "").lower()
        if f.endswith("@" + DOMAIN): return f
    return ""

_owner = {}
def user_of(email):
    if not email: return ""
    if email not in _owner:
        res = hs("GET", "/crm/v3/owners", params={"email": email, "limit": 1})
        r = (res.get("results") or [{}])[0]
        _owner[email] = str(r.get("userId") or "")
    return _owner[email]

def fmt(ms):
    try: return datetime.datetime.fromtimestamp(int(ms) / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError): return ms or ""

ids = members(SEGMENT)
print(f"segment {SEGMENT}: {len(ids)} contacts")
props = contacts(ids)
names, rows = {}, []
for n, cid in enumerate(ids, 1):
    p = props.get(cid, {})
    enr = hs("GET", f"/automation/sequences/2026-09/enrollments/contact/{cid}", ok404=True) if p.get("hs_sequences_is_enrolled") == "true" else None
    if enr and enr.get("sequenceId"): names[str(enr["sequenceId"])] = enr.get("sequenceName") or ""
    sender = last_sender(cid)
    rows.append({"contact_id": cid, "email": p.get("email") or "", "first_name": p.get("firstname") or "", "last_name": p.get("lastname") or "",
                 "sequence_id": str((enr or {}).get("sequenceId") or p.get("hs_latest_sequence_enrolled") or ""),
                 "enrolled_now": p.get("hs_sequences_is_enrolled") or "", "enrolled_at": fmt(p.get("hs_latest_sequence_enrolled_date")),
                 "ended_at": p.get("hs_latest_sequence_ended_date") or "", "sender_email": sender,
                 "enrolled_by_user_id": str((enr or {}).get("enrolledBy") or ""), "user_id": user_of(sender)})
    if n % 50 == 0: print(f"  {n}/{len(ids)}")
for r in rows: r["sequence_name"] = names.get(r["sequence_id"], "")
out = f"sequence_snapshot_{SEGMENT}_{datetime.date.today()}.csv"
cols = ["contact_id", "email", "first_name", "last_name", "sequence_id", "sequence_name", "enrolled_now", "enrolled_at", "ended_at",
        "sender_email", "user_id", "enrolled_by_user_id"]
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)
print(f"written {out}: {len(rows)} contacts, {len({r['sequence_id'] for r in rows if r['sequence_id']})} sequences, "
      f"no sequence id {sum(1 for r in rows if not r['sequence_id'])}, no sender found {sum(1 for r in rows if not r['sender_email'])}, "
      f"sender with no HubSpot user {sum(1 for r in rows if r['sender_email'] and not r['user_id'])}")
