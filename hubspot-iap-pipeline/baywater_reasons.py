"""Pull every Baywater-tagged company's stage 1 reason from HubSpot and group the rejections by cause.

Run from the pipeline folder with HUBSPOT_TOKEN set:
    python3 baywater_reasons.py
Reads only. Writes baywater_reasons.csv next to the script and prints the counts.
"""
import csv, json, os, re, sys, urllib.request

TOKEN = os.environ["HUBSPOT_TOKEN"]
TAG = os.getenv("TAG", "baywater - add-on")
PROPS = ["name", "domain", "state", "numberofemployees", "iap_match_reasons", "enrichment_iap_scores", "enrichment_iap_campaigns", "enrichment_status"]

def hs_search(after=None):
    body = {"filterGroups": [{"filters": [{"propertyName": "grata_research_tag", "operator": "EQ", "value": TAG},
                                          {"propertyName": "iap_match_reasons", "operator": "HAS_PROPERTY"}]}],
            "properties": PROPS, "limit": 200}
    if after: body["after"] = after
    req = urllib.request.Request("https://api.hubapi.com/crm/v3/objects/companies/search", data=json.dumps(body).encode(),
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req).read())

rows, after = [], None
while True:
    data = hs_search(after)
    rows += data["results"]
    after = data.get("paging", {}).get("next", {}).get("after")
    if not after: break
print(f"pulled {len(rows)} companies tagged '{TAG}' with a stage 1 reason")

# Rejection causes, checked in this order; the first that matches wins.
CATS = [
    ("not a contractor (services, software, finance, healthcare, etc.)",
     r"not a (construction |power |electrical )?contractor|collection|consult|software|SAP |IT |analytics|laborator|property management|healthcare|insurance|staffing|call center|advertis|logistic|rental|laundry|benefit|audit|technology (solutions|provider)"),
    ("engineering / surveying / design only", r"engineer(ing)?[- ]only|surveying|surveyor|design firm|drafting|does not build|testing"),
    ("telecom / fiber", r"telecom|fiber|broadband|wireless"),
    ("pipeline / oil & gas", r"pipeline|oil and gas|o&g|petroleum"),
    ("commercial / building / residential electrician", r"building electric|commercial (and industrial |building )?electrical|residential|electrician"),
    ("civil / sitework / water-sewer / general contractor", r"civil|sitework|site work|earthwork|grading|water|sewer|highway|bridge|general contractor|design-build|drilling|concrete|paving"),
    ("traffic signals / lighting / ITS", r"traffic|signal|street light|roadway lighting|ITS"),
    ("generation / renewables (solar, wind)", r"solar|wind|renewable|generation"),
    ("manufacturer / distributor", r"manufactur|distribut|supplier|remanufactur"),
    ("union", r"union"),
    ("diversity designation (MBE/WBE/DBE)", r"MBE|WBE|DBE|minority|woman-owned|women-owned|disadvantaged"),
    ("power-delivery scope unclear / too general", r"too general|unclear|not clear|cannot be confirmed|not evident|review"),
]

out, counts, examples = [], {}, {}
for r in rows:
    p = r["properties"]
    reason = (p.get("iap_match_reasons") or "")
    line = next((l for l in reason.split("\n") if l.lower().startswith("baywater")), reason)
    verdict = "MATCH" if re.search(r":\s*MATCH\b", line) else "NO"
    cat = "stage 1 MATCH"
    if verdict == "NO":
        cat = "other"
        for name, rx in CATS:
            if re.search(rx, line, re.I): cat = name; break
    score = ""
    m = re.search(r"Baywater[^:]*:\s*(\d)", p.get("enrichment_iap_scores") or "")
    if m: score = m.group(1)
    counts[cat] = counts.get(cat, 0) + 1
    examples.setdefault(cat, []).append(f"{p.get('name')} — {line[:140]}")
    out.append({"id": r["id"], "name": p.get("name"), "domain": p.get("domain"), "state": p.get("state"), "employees": p.get("numberofemployees"),
                "stage1": verdict, "category": cat, "stage2_score": score, "ticked": "yes" if "325749324474" in (p.get("enrichment_iap_campaigns") or "") else "",
                "reason": line})

with open("baywater_reasons.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)

print("\nREJECTION CAUSES (stage 1):")
for cat, n in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"  {n:5d}  {cat}")
print("\nSTAGE 2 SCORES for the stage 1 MATCH companies:")
sc = {}
for o in out:
    if o["stage1"] == "MATCH": sc[o["stage2_score"] or "no score yet"] = sc.get(o["stage2_score"] or "no score yet", 0) + 1
for k, n in sorted(sc.items()): print(f"  score {k}: {n}")
print("\nTHREE EXAMPLES PER CAUSE:")
for cat in sorted(counts, key=lambda c: -counts[c]):
    print(f"\n[{cat}]")
    for e in examples[cat][:3]: print("  -", e)
print("\nfull detail in baywater_reasons.csv")
