"""z_strip_and_merge.py - remove the retired "z" prefix from contact names, then rerun the contacts dedupe with
per-field rules.

STEP 1  every contact whose first or last name starts with the z prefix (zJim, zzzJames, z Jim, ZZJim, ZRobert;
        real names like Zhao or Zumbrun are left alone): the z is removed from first and last name and
        retired_contact is set to true. z_strip.csv keeps the old and new names.
STEP 2  contacts dedupe on the cleaned names: same full name + same company merges. Email, phone, city and job
        title are never used to match. Groups with anyone in an active sequence wait for a later run (z_review.csv).
        Per-field rules for each merge:
          Main record          the contact with the most recent real email reply (incoming email or OutboundSync
                               reply; out-of-office, automatic replies and bounces don't count). Its email stays
                               primary, the others stay as secondary emails, and all its other values win.
                               No real reply -> latest Last Contact Date, then latest Last Outreach Date, then older record.
          Phone                the number of the most recent connected call on any contact of the group;
                               no connected call -> the main record keeps its own phone.
          retired_contact      true when any contact of the group is Retired (or still has the z prefix).
        HubSpot moves all calls, emails, notes and associations onto the main record when merging.

Before running: create the contact property "Retired contact" (internal name retired_contact, single checkbox).
Env: HUBSPOT_TOKEN, DRY_RUN (default true = report only, nothing changes), STEP (all | strip | merge, default all),
     MAX_MERGES (default 500; the nightly job runs without a limit)
Outputs (in this folder): z_strip.csv, z_merges.csv, z_review.csv
"""
import re
from common import *

STEP = os.getenv("STEP", "all").lower()
RETIRED = "retired_contact"
URL = "https://app-na2.hubspot.com/contacts/243636394/record/0-1/{}"
Z = re.compile(r"^(z+(?=[A-Z0-9\s\-_.])|Z{2,}|Z(?=[A-Z][a-z]))")
CONNECTED = {"call successful", "connected", "contact made – no interest", "contact made - no interest", "meeting booked"}
PROPS = ["firstname", "lastname", "jobtitle", "company", "associatedcompanyid", "phone", "mobilephone", "city", "email",
         "last_contact_date", "last_outreach_date", "createdate", "hs_sequences_is_enrolled",
         "os_last_reply_time", "os_last_reply_subject", "os_last_reply_message", "os_last_reply_address", RETIRED]
AUTO = re.compile(r"out of (the )?office|automatic reply|auto[- ]?reply|autoreply|away from (the |my )?(office|desk)|"
                  r"on (annual |parental |maternity )?(vacation|leave|holiday)|currently (out|away|travel)|"
                  r"undeliverable|delivery (status notification|has failed|failure)|mail delivery (failed|subsystem)|"
                  r"returned mail|failure notice|message not delivered|no longer (with|employed|works)|"
                  r"this mailbox is (not|no longer) monitored|do not reply", re.I)
AUTO_SENDER = re.compile(r"mailer-daemon|postmaster|no-?reply|donotreply|bounce", re.I)

def real_reply(subject, text, sender=""):
    if AUTO_SENDER.search(sender or ""): return False
    return not AUTO.search(f"{subject or ''} {(text or '')[:1500]}")

def strip_z(s):
    s = (s or "").strip()
    m = Z.match(s)
    return re.sub(r"^[\s\-_.]+", "", s[m.end():]) if m else s

def hs_try(method, dated, legacy, **kw):
    try: return hs(method, dated, **kw)
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
        return hs(method, legacy, **kw)

def batch_update(rows):
    if DRY_RUN or not rows: return
    for i in range(0, len(rows), 100):
        chunk = rows[i:i+100]
        try: hs("POST", f"/crm/objects/{V}/contacts/batch/update", json={"inputs": chunk})
        except requests.HTTPError as e:
            log("batch update rejected:", e.response.text[:300], "- retrying one by one")
            for u in chunk:
                try: hs("POST", f"/crm/objects/{V}/contacts/batch/update", json={"inputs": [u]})
                except requests.HTTPError as e2: log("  skipped", u["id"], e2.response.text[:200])

def assoc(ids, to):
    """contact id -> list of associated <to> ids."""
    out = {}
    for i in range(0, len(ids), 100):
        res = hs("POST", f"/crm/v4/associations/contacts/{to}/batch/read", json={"inputs": [{"id": x} for x in ids[i:i+100]]})
        for r in res.get("results", []):
            out[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to", [])]
        if i and i % 5000 == 0: log(f"  {to} associations: {i:,} contacts read")
    return out

def read_objects(obj, ids, props):
    out, ids = {}, list(dict.fromkeys(ids))
    for i in range(0, len(ids), 100):
        res = hs_try("POST", f"/crm/objects/{V}/{obj}/batch/read", f"/crm/v3/objects/{obj}/batch/read",
                     json={"inputs": [{"id": x} for x in ids[i:i+100]], "properties": props})
        for r in res.get("results", []): out[r["id"]] = r["properties"]
        if i and i % 20000 == 0: log(f"  {obj}: {i:,} read")
    return out

def merge_final(primary, loser, hop=0):
    """Merge loser into primary; returns the id that survives (follows HubSpot's forward reference), or None."""
    if DRY_RUN: return primary
    try:
        hs("POST", f"/crm/objects/{V}/contacts/merge", json={"primaryObjectId": primary, "objectIdToMerge": loser})
        return primary
    except requests.HTTPError as e:
        m = re.search(r"forward reference to (\d+)", e.response.text)
        if m and hop < 3: return merge_final(m.group(1), loser, hop + 1)
        log("merge failed", primary, loser, e.response.text[:200]); return None

def main():
    log("DRY_RUN:", DRY_RUN, "| STEP:", STEP, "| MAX_MERGES:", MAX_MERGES)
    try: hs("GET", f"/crm/v3/properties/contacts/{RETIRED}")
    except requests.HTTPError:
        log(f"contact property {RETIRED} is missing: create 'Retired contact' (single checkbox) first"); return

    cname = {r["id"]: norm_name(r["properties"].get("name")) for r in page_all("companies", ["name"], "company names")}
    recs = {r["id"]: r for r in page_all("contacts", PROPS)}

    # ---------- STEP 1: strip the z ----------
    was_z, strip_rows, updates = set(), [], []
    for i, r in recs.items():
        p = r["properties"]
        fn, ln = p.get("firstname") or "", p.get("lastname") or ""
        if not (Z.match(fn.strip()) or Z.match(ln.strip())): continue
        was_z.add(i)
        nf, nl = strip_z(fn), strip_z(ln)
        strip_rows.append([i, fn, ln, nf, nl, p.get("jobtitle"), p.get("company"), URL.format(i)])
        p["firstname"], p["lastname"] = nf, nl           # the merge step uses the cleaned names
        updates.append({"id": i, "properties": {"firstname": nf, "lastname": nl, RETIRED: "true"}})
    write_csv("z_strip.csv", strip_rows, ["id", "old_firstname", "old_lastname", "new_firstname", "new_lastname", "jobtitle", "company", "hubspot_link"])
    log(f"z contacts: {len(was_z):,}")
    if STEP in ("all", "strip"):
        batch_update(updates); log(f"{'would update' if DRY_RUN else 'updated'} {len(updates):,} contacts")
    if STEP == "strip": return

    # ---------- STEP 2: group on full name + company ----------
    retired = {i for i, r in recs.items() if str(r["properties"].get(RETIRED)).lower() == "true"} | was_z
    by_key = {}
    for i, r in recs.items():
        p = r["properties"]
        full = norm_name(f"{p.get('firstname') or ''} {p.get('lastname') or ''}")
        comp = norm_name(p.get("company")) or cname.get(p.get("associatedcompanyid") or "", "")
        if full and comp and len(full.split()) >= 2: by_key.setdefault((full, comp), []).append(i)
    groups, review = [], []
    for (full, comp), g in by_key.items():
        if len(g) < 2: continue
        if any(str(recs[i]["properties"].get("hs_sequences_is_enrolled")).lower() == "true" for i in g):
            for j in g:
                q = recs[j]["properties"]
                review.append(["in active sequence - next run", j, q.get("firstname"), q.get("lastname"), q.get("company"),
                               "yes" if j in retired else "", URL.format(j)])
        else: groups.append(g)
    log(f"groups to merge: {len(groups):,} ({sum(1 for g in groups if set(g) & retired):,} with a Retired contact) | "
        f"waiting for sequences: {len(review):,} contacts")

    # ---------- evidence: real replies and connected calls ----------
    members = [i for g in groups for i in g]
    log("reading emails and calls for", f"{len(members):,}", "contacts")
    e_of, c_of = assoc(members, "emails"), assoc(members, "calls")
    emails = read_objects("emails", [x for v in e_of.values() for x in v],
                          ["hs_email_direction", "hs_timestamp", "hs_email_subject", "hs_email_text", "hs_email_from_email"])
    calls = read_objects("calls", [x for v in c_of.values() for x in v],
                         ["hs_timestamp", "hs_call_disposition", "hs_call_direction", "hs_call_to_number", "hs_call_from_number"])
    dispo = {d["id"]: d["label"].strip().lower() for d in (hs("GET", "/calling/v1/dispositions") or [])}

    def reply_ms(i):
        p = recs[i]["properties"]
        ms = 0
        if p.get("os_last_reply_time") and real_reply(p.get("os_last_reply_subject"), p.get("os_last_reply_message"),
                                                       p.get("os_last_reply_address")):
            ms = to_ms(p.get("os_last_reply_time"))
        for e in e_of.get(i, []):
            q = emails.get(e) or {}
            if q.get("hs_email_direction") != "INCOMING_EMAIL": continue
            if real_reply(q.get("hs_email_subject"), q.get("hs_email_text"), q.get("hs_email_from_email")):
                ms = max(ms, to_ms(q.get("hs_timestamp")))
        return ms

    def connected_phone(g):
        best, num = 0, ""
        for i in g:
            for c in c_of.get(i, []):
                q = calls.get(c) or {}
                if dispo.get(q.get("hs_call_disposition") or "", "") not in CONNECTED: continue
                n = q.get("hs_call_from_number") if q.get("hs_call_direction") == "INBOUND" else q.get("hs_call_to_number")
                ts = to_ms(q.get("hs_timestamp"))
                if n and ts > best: best, num = ts, n
        return num, best

    # ---------- plan ----------
    plan = []
    for g in groups:
        rs = {i: recs[i]["properties"] for i in g}
        rep = {i: reply_ms(i) for i in g}
        if max(rep.values()) > 0:
            main_id, rule = max(g, key=lambda i: rep[i]), "latest real email reply"
        else:
            main_id, rule = survivor([recs[i] for i in g])["id"], "no reply: latest contact / outreach / older"
        phone, phone_ms = connected_phone(g)
        plan.append({"main": main_id, "losers": [i for i in g if i != main_id], "rule": rule, "email": rs[main_id].get("email"),
                     "reply": rep[main_id], "phone": phone, "phone_ms": phone_ms,
                     "retired": "true" if set(g) & retired else "",
                     "name": f"{rs[main_id].get('firstname')} {rs[main_id].get('lastname')}", "company": rs[main_id].get("company")})
    fmt = lambda ms: datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d") if ms else ""
    write_csv("z_merges.csv", [[x["main"], ";".join(x["losers"]), x["name"], x["company"], x["rule"], x["email"], fmt(x["reply"]),
               x["phone"], fmt(x["phone_ms"]), x["retired"], URL.format(x["main"])] for x in plan],
              ["main_id", "merged_ids", "name", "company", "main_record_rule", "primary_email", "last_real_reply",
               "phone_set", "connected_call", "retired_contact", "hubspot_link"])
    write_csv("z_review.csv", review, ["reason", "id", "firstname", "lastname", "company", "retired", "hubspot_link"])

    # ---------- merge + set phone and Retired ----------
    done = 0
    for x in plan:
        if done >= MAX_MERGES: log(f"MAX_MERGES {MAX_MERGES} reached - stopping; rerun for the next batch"); break
        final = x["main"]
        for l in x["losers"]:
            r = merge_final(final, l)
            if r: final = r; done += 1
        props = {}
        if x["phone"]: props["phone"] = x["phone"]
        if x["retired"]: props[RETIRED] = "true"
        if props: batch_update([{"id": final, "properties": props}])
        if done and done % 100 == 0: log(f"merged {done:,}")
    log(f"{'would merge' if DRY_RUN else 'merged'} {done:,} contacts. done")

if __name__ == "__main__": main()
