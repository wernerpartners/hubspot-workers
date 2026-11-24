"""reenroll_sequences.py — enroll every contact in a snapshot file again in the same sequence, from step 1.

  source ../.env
  SNAPSHOT=sequence_snapshot_969_2026-11-21.csv TEST_LIMIT=1 python3 reenroll_sequences.py   # one contact only
  SNAPSHOT=sequence_snapshot_969_2026-11-21.csv python3 reenroll_sequences.py                # everyone

API (HubSpot docs, Sequences API 2026-09): POST /automation/sequences/2026-09/enrollments?userId=<user>
  body {"contactId", "sequenceId", "senderEmail"}. One contact per call; HubSpot allows 1,000 enrollments per inbox per
  day. The sender inbox must be connected in HubSpot and belong to that user, and the user needs a Sales Hub or
  Service Hub Professional/Enterprise seat. Scope: automation.sequences.enrollments.write.

Rules:
  - A contact already enrolled in a sequence right now is skipped (HubSpot does not allow two at once).
  - SENDER_MAP=old@x.com:new@y.com,...  sends from a different connected inbox (its HubSpot user is looked up).
  - DAILY_LIMIT (default 1000) per sender inbox; the run stops for that inbox and the rest goes on the next run.
  - Progress is kept in reenroll_state.json, so running it again never enrolls the same contact twice.
"""
import os, csv, json, time, requests

TOKEN = os.environ["HUBSPOT_TOKEN"]
SNAPSHOT = os.environ["SNAPSHOT"]
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "0"))
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "1000"))
SENDER_MAP = dict(x.split(":", 1) for x in os.getenv("SENDER_MAP", "").split(",") if ":" in x)
STATE = "reenroll_state.json"
HS = "https://api.hubapi.com"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
S = requests.Session()

def hs(method, path, ok=(), **kw):
    for a in range(7):
        r = S.request(method, HS + path, headers=H, timeout=60, **kw)
        if r.status_code in (429, 500, 502, 503, 504): time.sleep(min(2 ** a, 30)); continue
        return r
    return r

_owner = {}
def user_of(email):
    if email not in _owner:
        r = hs("GET", "/crm/v3/owners", params={"email": email, "limit": 1})
        res = r.json().get("results") or [{}] if r.ok else [{}]
        _owner[email] = str(res[0].get("userId") or "")
    return _owner[email]

state = json.load(open(STATE)) if os.path.exists(STATE) else {"done": {}, "errors": {}}
rows = list(csv.DictReader(open(SNAPSHOT)))
sent_today, done, skipped, failed = {}, 0, 0, 0
for row in rows:
    cid, seq = row["contact_id"], row["sequence_id"]
    if cid in state["done"]: continue
    if TEST_LIMIT and done >= TEST_LIMIT: break
    sender = SENDER_MAP.get(row["sender_email"], row["sender_email"])
    user = user_of(sender) if sender != row["sender_email"] or not row.get("user_id") else row["user_id"]
    if not (seq and sender and user):
        skipped += 1; state["errors"][cid] = "missing sequence, sender or user"; continue
    if sent_today.get(sender, 0) >= DAILY_LIMIT: continue
    cur = hs("GET", f"/automation/sequences/2026-09/enrollments/contact/{cid}")
    if cur.status_code == 200 and cur.json().get("sequenceId"):
        skipped += 1; state["errors"][cid] = f"already enrolled in {cur.json().get('sequenceId')}"; continue
    r = hs("POST", "/automation/sequences/2026-09/enrollments", params={"userId": user},
           json={"contactId": cid, "sequenceId": seq, "senderEmail": sender})
    if r.ok:
        state["done"][cid] = {"sequence_id": seq, "sender": sender, "enrollment_id": r.json().get("id"), "at": time.strftime("%Y-%m-%d %H:%M")}
        state["errors"].pop(cid, None); done += 1; sent_today[sender] = sent_today.get(sender, 0) + 1
    else:
        failed += 1; state["errors"][cid] = f"{r.status_code} {r.text[:200]}"
    json.dump(state, open(STATE, "w"), indent=1)
json.dump(state, open(STATE, "w"), indent=1)
left = sum(1 for r in rows if r["contact_id"] not in state["done"])
print(f"enrolled {done}, skipped {skipped}, failed {failed}, not yet enrolled {left}. Details per contact in {STATE}.")
for cid, e in list(state["errors"].items())[:20]: print(" ", cid, e)
