"""dedupe-contacts-worker
Certain duplicate = same normalised full name + same normalised company name (required).
Phone and city are helpers: if BOTH records have a phone and they differ, or BOTH have a city and they differ,
the pair goes to the review sheet instead. Email is never used.
Survivor = latest Last Contact Date, then latest Last Outreach Date, then the older record.
Run order: dedupe_companies.py first, then this.
Env: HUBSPOT_TOKEN, DRY_RUN (default true = report only), MAX_MERGES (default 500), OUT_DIR
"""
from common import *

PROPS = ["firstname", "lastname", "company", "associatedcompanyid", "phone", "mobilephone", "city",
         "last_contact_date", "last_outreach_date", "createdate", "hs_sequences_is_enrolled"]

def main():
    log("DRY_RUN:", DRY_RUN, "| MAX_MERGES:", MAX_MERGES)
    cname = {r["id"]: norm_name(r["properties"].get("name")) for r in page_all("companies", ["name"], "company names")}
    recs, by_key = {}, {}
    for r in page_all("contacts", PROPS):
        p = r["properties"]; i = r["id"]
        full = norm_name(f"{p.get('firstname') or ''} {p.get('lastname') or ''}")
        comp = norm_name(p.get("company")) or cname.get(p.get("associatedcompanyid") or "", "")
        if not full or not comp or len(full.split()) < 2: continue
        recs[i] = r
        by_key.setdefault((full, comp), []).append(i)

    uf = UnionFind(); review = []
    for (full, comp), ids in by_key.items():
        if len(ids) < 2: continue
        base = recs[ids[0]]["properties"]
        for i in ids[1:]:
            p = recs[i]["properties"]
            ph_a = norm_phone(base.get("phone") or base.get("mobilephone")); ph_b = norm_phone(p.get("phone") or p.get("mobilephone"))
            ci_a = norm_city(base.get("city")); ci_b = norm_city(p.get("city"))
            if (ph_a and ph_b and ph_a != ph_b) or (ci_a and ci_b and ci_a != ci_b):
                for j in (ids[0], i):
                    q = recs[j]["properties"]
                    review.append([f"{full} @ {comp}", j, q.get("firstname"), q.get("lastname"), q.get("company"), q.get("phone") or q.get("mobilephone"), q.get("city"), q.get("last_contact_date"), q.get("last_outreach_date"), q.get("createdate")])
            else:
                uf.union(ids[0], i)
    write_csv("contacts_review.csv", review, ["match_key", "id", "firstname", "lastname", "company", "phone", "city", "last_contact_date", "last_outreach_date", "createdate", "hs_sequences_is_enrolled"])

    certain = groups_from(uf, recs.keys())
    plan, in_seq = [], 0
    for g in certain:
        # HubSpot refuses to merge a contact that is enrolled in an active sequence: skip the group for now.
        if any(str(recs[i]["properties"].get("hs_sequences_is_enrolled")).lower() == "true" for i in g):
            in_seq += 1
            for j in g:
                q = recs[j]["properties"]
                review.append(["IN ACTIVE SEQUENCE - rerun later", j, q.get("firstname"), q.get("lastname"), q.get("company"), q.get("phone") or q.get("mobilephone"), q.get("city"), q.get("last_contact_date"), q.get("last_outreach_date"), q.get("createdate")])
            continue
        s = survivor([recs[i] for i in g])
        for i in g:
            if i == s["id"]: continue
            p = recs[i]["properties"]
            plan.append([s["id"], i, p.get("firstname"), p.get("lastname"), p.get("company")])
    log(f"groups skipped because a contact is in an active sequence: {in_seq:,}")
    write_csv("contacts_review.csv", review, ["match_key", "id", "firstname", "lastname", "company", "phone", "city", "last_contact_date", "last_outreach_date", "createdate"])
    write_csv("contacts_merges.csv", plan, ["survivor_id", "loser_id", "loser_firstname", "loser_lastname", "loser_company"])
    log(f"certain groups: {len(certain):,} | merges planned: {len(plan):,} | review rows: {len(review):,}")

    done = 0
    for row in plan:
        if done >= MAX_MERGES: log(f"MAX_MERGES {MAX_MERGES} reached - stopping; rerun for the next batch"); break
        if merge("contacts", row[0], row[1]): done += 1
        if done and done % 50 == 0: log(f"merged {done:,}")
    log(f"{'would merge' if DRY_RUN else 'merged'} {done:,} contacts. done")

if __name__ == "__main__": main()
