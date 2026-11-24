"""dedupe-companies-worker
Certain duplicate = same normalised domain shared by at most 3 companies whose names also agree,
OR same normalised name + same city. Domains shared by 4+ companies (franchises) never merge; directory, social, shortener and
site-builder domains never merge even when only 2 share them.
Same normalised name but city missing/different = review sheet, no merge,
unless they also share a phone number, a LinkedIn company page, or a contact with the same full name:
then they merge. Domains are compared without http://, https://, www. and paths.
Survivor = latest Last Contact Date, then latest Last Outreach Date, then the older record.
Skipped (companies_skipped.csv, left in the plan): a contact of either company is in an active sequence,
Lead Status kept = the higher of the two: Public Pool < Campaign Queue < New < Working < Closed Lead < Open Deal < Closed Deal.
Run order: this first, then dedupe_contacts.py.
Env: HUBSPOT_TOKEN, DRY_RUN (default true = report only), MAX_MERGES (default 500), OUT_DIR
     PLAN=companies_merges.csv  -> live run straight from a reviewed plan file, no re-read of HubSpot
"""
from common import *

PROPS = ["name", "domain", "city", "state", "phone", "linkedin_company_page", "last_contact_date", "last_outreach_date", "createdate"]

PUBLIC = {"public pool", ""}
skipped = []

def contacts_of(cid):
    res = hs("POST", "/crm/v4/associations/companies/contacts/batch/read", json={"inputs": [{"id": str(cid)}]})
    out = []
    for r in res.get("results", []):
        out += [str(t["toObjectId"]) for t in r.get("to", [])]
    return out

def in_sequence(cid):
    ids = contacts_of(cid)
    for k in range(0, len(ids), 100):
        res = hs("POST", "/crm/v3/objects/contacts/batch/read",
                 json={"properties": ["hs_sequences_is_enrolled"], "inputs": [{"id": i} for i in ids[k:k + 100]]})
        if any(str(r["properties"].get("hs_sequences_is_enrolled")).lower() == "true" for r in res.get("results", [])):
            return True
    return False

def lead_status(cid):
    r = hs("GET", f"/crm/v3/objects/companies/{cid}", params={"properties": "hs_lead_status,name"})
    return (r.get("properties") or {}).get("hs_lead_status") or ""

def safe_merge(survivor_id, loser_id):
    """Merge only when no contact of either company is in an active sequence.
    Lead Status: the higher one wins, Public Pool < Campaign Queue < New < Working < Closed Lead < Open Deal < Closed Deal."""
    if in_sequence(survivor_id) or in_sequence(loser_id):
        skipped.append([survivor_id, loser_id, "contact in active sequence", "", ""]); return None
    st_s, st_l = lead_status(survivor_id), lead_status(loser_id)
    order = ["", "public pool", "campaign queue", "new", "working", "closed lead", "open_deal", "closed deal"]
    def rank(v):                       # higher wins; unknown values count as Public Pool
        v = v.strip().lower()
        return order.index(v) if v in order else 1
    keep = st_s
    if rank(st_l) > rank(st_s): keep = st_l
    ok = merge("companies", survivor_id, loser_id)
    if ok and not DRY_RUN and keep != st_s:
        try: hs("PATCH", f"/crm/v3/objects/companies/{survivor_id}", json={"properties": {"hs_lead_status": keep}})
        except Exception as e: log("lead status write failed", survivor_id, e)
    return ok

def write_skipped():
    write_csv("companies_skipped.csv", skipped, ["survivor_id", "loser_id", "reason", "survivor_lead_status", "loser_lead_status"])

PLACEHOLDER_CO = re.compile(r"\b(closed|out of business|unknown|do not|delete|duplicate|dupe|test|none)\b")
PLACEHOLDER_PERSON = re.compile(r"\b(unknown|owner|office|manager|desk|front|info|sales|admin|accounts?|payable|"
                                r"reception|main|general|contact|team|support|service|customer|billing|hr|department|none|test)\b")

# Directory, social, link-shortener and parked/ad domains: never proof that two companies are the same,
# even when only 2 companies share one. Any subdomain of these is blocked too (m.facebook.com ...).
DIRECTORY_DOMAINS = {
    "hub.biz", "all.biz", "manta.com", "yelp.com", "yellowpages.com", "yp.com", "superpages.com", "bbb.org",
    "mapquest.com", "chamberofcommerce.com", "buzzfile.com", "bizapedia.com", "opencorporates.com", "dnb.com",
    "zoominfo.com", "bloomberg.com", "crunchbase.com", "uscompaniesdata.com", "ontariobusinessdir.com",
    "h1bdata.info", "charitynavigator.org", "guidestar.org", "franchisedirect.com", "mathale.co.za", "cylex.us.com",
    "hotfrog.com", "merchantcircle.com", "citysearch.com", "foursquare.com", "alignable.com", "angi.com",
    "angieslist.com", "homeadvisor.com", "thumbtack.com", "houzz.com", "nextdoor.com", "buildzoom.com", "porch.com",
    "glassdoor.com", "indeed.com", "kompass.com", "thomasnet.com", "made-in-china.com", "alibaba.com",
    "facebook.com", "fb.com", "linkedin.com", "instagram.com", "twitter.com", "x.com", "youtube.com", "tiktok.com",
    "pinterest.com", "bit.ly", "tinyurl.com", "goo.gl", "ow.ly", "linktr.ee", "t.co", "googlesyndication.com",
    "sedoparking.com", "godaddy.com", "hugedomains.com", "dan.com", "afternic.com",
}
# Site builders: blocked only when the whole site collapses to the builder itself (wix.com/acme -> wix.com).
# acme.wixsite.com stays a real, company-specific domain.
BUILDER_DOMAINS = {"wix.com", "wixsite.com", "weebly.com", "squarespace.com", "wordpress.com", "blogspot.com",
                   "sites.google.com", "google.com", "business.site", "square.site", "myshopify.com", "webflow.io",
                   "yolasite.com", "godaddysites.com", "jimdo.com", "strikingly.com", "carrd.co"}

def is_directory_domain(d):
    d = (d or "").lower()
    return d in BUILDER_DOMAINS or any(d == x or d.endswith("." + x) for x in DIRECTORY_DOMAINS)

def norm_li(v):
    """LinkedIn company page -> slug, e.g. https://www.linkedin.com/company/acme-inc/ -> acme-inc"""
    v = (v or "").strip().lower()
    m = re.search(r"linkedin\.com/company/([^/?#\s]+)", v)
    return m.group(1) if m else ""

def same_name_signals(by_name, recs, uf, reason):
    """Same company name + same phone (last 10 digits) or same LinkedIn company page -> same company."""
    n_ph = n_li = 0
    for n, ids in by_name.items():
        if len(ids) < 2 or len(n) < 3 or PLACEHOLDER_CO.search(n): continue
        by_ph, by_li = {}, {}
        for i in ids:
            q = recs[i]["properties"]
            ph = norm_phone(q.get("phone"))
            if len(ph) == 10 and len(set(ph)) > 2: by_ph.setdefault(ph, []).append(i)
            li = norm_li(q.get("linkedin_company_page"))
            if li: by_li.setdefault(li, []).append(i)
        for key, g, label in [(k, g, "phone") for k, g in by_ph.items()] + [(k, g, "linkedin") for k, g in by_li.items()]:
            for x in g[1:]:
                if uf.find(x) != uf.find(g[0]):
                    uf.union(g[0], x); reason.setdefault(x, f"same name + same {label} {key}")
                    if label == "phone": n_ph += 1
                    else: n_li += 1
    log(f"linked by same name + same phone: {n_ph:,} | same name + same LinkedIn page: {n_li:,}")

def contact_link(by_name, uf, reason):
    """Same company name + at least one contact with the same full name on both companies -> same company.
    Only checks the same-name groups that would otherwise go to review."""
    groups = [ids for n, ids in by_name.items() if len(ids) > 1 and len(n) >= 3 and not PLACEHOLDER_CO.search(n)
              and len({uf.find(i) for i in ids}) > 1]
    cids = [str(i) for g in groups for i in g]
    log(f"same-name groups to check by contacts: {len(groups):,} ({len(cids):,} companies)")
    c_of = {}
    for k in range(0, len(cids), 100):
        res = hs("POST", "/crm/v4/associations/companies/contacts/batch/read", json={"inputs": [{"id": x} for x in cids[k:k + 100]]})
        for r in res.get("results", []):
            c_of[str(r["from"]["id"])] = [str(t["toObjectId"]) for t in r.get("to", [])]
        if k and k % 5000 == 0: log(f"  contact links read for {k:,} companies")
    contact_ids = list({x for v in c_of.values() for x in v})
    names = {}
    for k in range(0, len(contact_ids), 100):
        res = hs("POST", "/crm/v3/objects/contacts/batch/read",
                 json={"properties": ["firstname", "lastname"], "inputs": [{"id": x} for x in contact_ids[k:k + 100]]})
        for r in res.get("results", []):
            q = r["properties"]
            full = norm_name(f"{q.get('firstname') or ''} {q.get('lastname') or ''}")
            if len(full.split()) >= 2 and not PLACEHOLDER_PERSON.search(full): names[str(r["id"])] = full
    linked = 0
    for g in groups:
        by_person = {}
        for cid in g:
            for ct in c_of.get(str(cid), []):
                if ct in names: by_person.setdefault(names[ct], set()).add(cid)
        for person, cos in by_person.items():
            cos = sorted(cos)
            for x in cos[1:]:
                if uf.find(x) != uf.find(cos[0]):
                    uf.union(cos[0], x); reason.setdefault(x, f"same name + same contact {person}"); linked += 1
    log(f"linked by a shared contact name: {linked:,} companies")

def merge_from_plan(path):
    """Live run from an existing companies_merges.csv (no re-read). Skips survivors/losers already merged."""
    import csv as _csv
    rows = list(_csv.DictReader(open(path)))
    log(f"plan {path}: {len(rows):,} merges | MAX_MERGES {MAX_MERGES}")
    done = failed = 0; gone = set()
    for row in rows:
        if done >= MAX_MERGES: log(f"MAX_MERGES {MAX_MERGES} reached - rerun for the next batch"); break
        if row["loser_id"] in gone or row["survivor_id"] in gone: continue
        ok = safe_merge(row["survivor_id"], row["loser_id"])
        if ok is None: continue
        if ok: done += 1; gone.add(row["loser_id"])
        else: failed += 1
        if done and done % 50 == 0: log(f"merged {done:,}")
    log(f"{'would merge' if DRY_RUN else 'merged'} {done:,} companies, {failed:,} failed, {len(skipped):,} skipped. done")
    write_skipped()
    # drop the merged rows from the plan so the next run continues where this one stopped
    if not DRY_RUN and done:
        rest = [r for r in rows if r["loser_id"] not in gone]
        with open(path, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rest)
        log(f"plan updated: {len(rest):,} merges left")

def main():
    log("DRY_RUN:", DRY_RUN, "| MAX_MERGES:", MAX_MERGES)
    plan_file = os.getenv("PLAN")
    if plan_file: return merge_from_plan(plan_file)
    recs, by_domain, by_name_city, by_name = {}, {}, {}, {}
    for r in page_all("companies", PROPS):
        p = r["properties"]; i = r["id"]; recs[i] = r
        d = norm_domain(p.get("domain")); n = norm_name(p.get("name")); c = norm_city(p.get("city"))
        if d and "." in d: by_domain.setdefault(d, []).append(i)
        if n:
            by_name.setdefault(n, []).append(i)
            if c: by_name_city.setdefault((n, c), []).append(i)

    uf = UnionFind(); reason = {}
    # A domain only counts as proof when few companies share it; directory, franchise and site-builder
    # domains (hub.biz, weebly.com, papajohns.com ...) are shared by many unrelated businesses.
    # Shared by 2-3: still require the normalised names to agree. Shared by 4+: never merge on it.
    shared = []
    for d, ids in by_domain.items():
        if len(ids) < 2: continue
        if is_directory_domain(d):
            shared.extend(ids); continue
        if len(ids) >= 4:
            shared.extend(ids); continue
        names = {norm_name(recs[i]["properties"].get("name")) for i in ids}
        if len(names) == 1:
            for i in ids[1:]: uf.union(ids[0], i); reason[i] = f"domain {d}"
        else:
            shared.extend(ids)
    for (n, c), ids in by_name_city.items():
        for i in ids[1:]: uf.union(ids[0], i); reason.setdefault(i, f"name+city {n} / {c}")
    same_name_signals(by_name, recs, uf, reason)
    contact_link(by_name, uf, reason)
    certain = groups_from(uf, recs.keys())
    certain_ids = {i for g in certain for i in g}

    review = []
    for i in shared:
        p = recs[i]["properties"]
        review.append([f"shared domain {norm_domain(p.get('domain'))}", i, p.get("name"), p.get("domain"), p.get("city"), p.get("state"), p.get("last_contact_date"), p.get("last_outreach_date"), p.get("createdate")])
    for n, ids in by_name.items():
        if len(ids) > 1 and len([i for i in ids if i not in certain_ids]) > 0 and len(set(uf.find(i) for i in ids)) > 1:
            for i in ids:
                p = recs[i]["properties"]
                review.append([n, i, p.get("name"), p.get("domain"), p.get("city"), p.get("state"), p.get("last_contact_date"), p.get("last_outreach_date"), p.get("createdate")])
    write_csv("companies_review.csv", review, ["match_key", "id", "name", "domain", "city", "state", "last_contact_date", "last_outreach_date", "createdate"])

    plan = []
    for g in certain:
        s = survivor([recs[i] for i in g])
        for i in g:
            if i == s["id"]: continue
            plan.append([s["id"], s["properties"].get("name"), i, recs[i]["properties"].get("name"), recs[i]["properties"].get("domain"), reason.get(i) or reason.get(s["id"], "group")])
    write_csv("companies_merges.csv", plan, ["survivor_id", "survivor_name", "loser_id", "loser_name", "loser_domain", "reason"])
    log(f"certain groups: {len(certain):,} | merges planned: {len(plan):,} | review rows: {len(review):,}")

    done = 0
    for row in plan:
        if done >= MAX_MERGES: log(f"MAX_MERGES {MAX_MERGES} reached - stopping; rerun for the next batch"); break
        if safe_merge(row[0], row[2]): done += 1
        if done and done % 50 == 0: log(f"merged {done:,}")
    log(f"{'would merge' if DRY_RUN else 'merged'} {done:,} companies, {len(skipped):,} skipped. done")
    write_skipped()

if __name__ == "__main__": main()
