"""Shared helpers for the dedupe workers."""
import os, re, time, csv, requests
from datetime import datetime

TOKEN = os.environ["HUBSPOT_TOKEN"]
API = "https://api.hubapi.com"
V = "2026-09"  # HubSpot date-based API version; legacy /crm/v3/ paths stop working September 2027
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MAX_MERGES = int(os.getenv("MAX_MERGES", "500"))
OUT_DIR = os.getenv("OUT_DIR", "/mnt/session/outputs" if os.path.isdir("/mnt/session/outputs") else ".")

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

S = requests.Session()

def hs(method, path, **kw):
    err = None
    for attempt in range(8):
        try:
            r = S.request(method, API + path, headers=H, timeout=60, **kw)
        except (requests.ConnectionError, requests.Timeout) as e:
            err = e; time.sleep(min(2 ** attempt, 60)); continue
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt, 60)); continue
        r.raise_for_status()
        return r.json() if r.text else {}
    if err: raise err
    r.raise_for_status()

def page_all(obj, props, label=None):
    """Yield every record of an object with the given properties."""
    after, n = None, 0
    while True:
        params = {"limit": 100, "properties": ",".join(props)}
        if after: params["after"] = after
        res = hs("GET", f"/crm/objects/{V}/{obj}", params=params)
        for r in res.get("results", []):
            n += 1
            yield r
        if n and n % 20000 == 0: log(f"{label or obj}: {n:,} read")
        after = res.get("paging", {}).get("next", {}).get("after")
        if not after: break
    log(f"{label or obj}: {n:,} total")

SUFFIX = re.compile(r"\b(incorporated|inc|llc|l l c|ltd|limited|corp|corporation|co|company|plc|lp|llp|gmbh|sa|the)\b")

def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()

def norm_domain(s):
    s = (s or "").strip().lower()
    s = re.sub(r"^https?://", "", s)
    s = re.sub(r"^www\.", "", s)
    return s.split("/")[0].split("?")[0]

def norm_city(s): return re.sub(r"\s+", " ", (s or "").lower().strip())

def norm_phone(s): return re.sub(r"\D", "", s or "")[-10:]

def to_ms(v):
    if not v: return 0
    if str(v).isdigit(): return int(v)
    try: return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return 0

def survivor(records):
    """Latest last_contact_date, then latest last_outreach_date, then the older record."""
    def key(r):
        p = r["properties"]
        return (to_ms(p.get("last_contact_date")), to_ms(p.get("last_outreach_date")), -to_ms(p.get("createdate")))
    return max(records, key=key)

class UnionFind:
    def __init__(self): self.p = {}
    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def union(self, a, b): self.p[self.find(a)] = self.find(b)

def groups_from(uf, ids):
    g = {}
    for i in ids: g.setdefault(uf.find(i), []).append(i)
    return [v for v in g.values() if len(v) > 1]

def write_csv(name, rows, header):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    log(f"wrote {path} ({len(rows):,} rows)")
    return path

def merge(obj, primary, loser, _hop=0):
    """Merge loser into primary. Primary keeps its values; blanks are filled from loser.
    If primary was itself merged away since the plan was made, HubSpot answers "forward reference to <id>":
    the loser is then merged into that newer record instead."""
    if DRY_RUN: return True
    try:
        hs("POST", f"/crm/objects/{V}/{obj}/merge", json={"primaryObjectId": primary, "objectIdToMerge": loser})
        return True
    except requests.HTTPError as e:
        msg = e.response.text
        m = re.search(r"forward reference to (\d+)", msg)
        if m and _hop < 3:
            log(f"{primary} was already merged into {m.group(1)}; merging {loser} there instead")
            return merge(obj, m.group(1), loser, _hop + 1)
        log("merge failed", primary, loser, msg[:200]); return False
