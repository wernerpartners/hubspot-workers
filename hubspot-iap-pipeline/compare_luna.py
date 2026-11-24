"""
Side-by-side test: Haiku vs OpenAI Luna on the same companies, writing NOTHING to HubSpot.

Takes SAMPLE (default 100) companies from segment 908 that stage 2 already finished (enrichment_status Done and
enrichment_website_text stored). Both models do the same two steps from the same material:
  step 1  match the company description against all active IAPs (same prompt as the pipeline's stage 1)
  step 2  grade the IAPs step 1 ticked from the stored website text (same prompt as the pipeline's grade-only pass)
Result: compare_luna.csv with each model's ticks, scores, and where they disagree, plus the tokens used.

Env: HUBSPOT_TOKEN, ANTHROPIC_API_KEY, OPENAI_API_KEY, SAMPLE=100, LUNA_MODEL=gpt-5.6-luna (check the exact id on
https://developers.openai.com/api/docs/pricing before running), HAIKU_MODEL=claude-haiku-5-5
Run:  python3 compare_luna.py
"""
import os, sys, json, time, csv, io, requests
os.environ.setdefault("LOOP", "false")
import pipeline as pl

SAMPLE = int(os.getenv("SAMPLE", "100"))
LUNA = os.getenv("LUNA_MODEL", "gpt-5.6-luna")
HAIKU = os.getenv("HAIKU_MODEL", "claude-haiku-5-5")
OAI = {"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"}

def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)

# ---------- sample ----------
iaps = pl.load_iaps(); by_id = {i["id"]: i for i in iaps}
props = ["name", pl.P["text"], pl.P["iap_ids"], pl.P["scores"], pl.P["s2"]] + pl.SRC
rows = pl.batch_read(pl.list_members(pl.SCOPE[5:]), props)
rows = [r for r in rows if r["properties"].get(pl.P["s2"]) == pl.ST["done"] and r["properties"].get(pl.P["text"])]
rows.sort(key=lambda r: int(r["id"])); rows = rows[:SAMPLE]
if not rows: log("no finished companies in the segment yet"); sys.exit()
comps = []
for r in rows:
    p = r["properties"]
    comps.append({"id": r["id"], "name": p.get("name") or "", "text": next((p[x] for x in pl.SRC if p.get(x)), "") or "",
                  "site": p.get(pl.P["text"]) or "", "hs_ticks": set((p.get(pl.P["iap_ids"]) or "").split(";")) - {""},
                  "hs_scores": p.get(pl.P["scores"]) or "", "loc": "", "desc": next((p[x] for x in pl.SRC if p.get(x)), "") or ""})
log(f"sample: {len(comps)} companies, {len(iaps)} active IAPs")

# ---------- step builders (same prompts as the pipeline) ----------
def step1_requests():
    out = []
    for n in range(0, len(comps), pl.PACK):
        chunk = [c for c in comps[n:n+pl.PACK] if c["text"]]
        if chunk: out.append((f"s1_{n}", pl.s1_system(iaps), pl.s1_user(chunk)))
    return out

def step2_requests(ticks):
    out = []
    for c in comps:
        cands = [by_id[x] for x in sorted(ticks.get(c["id"], set())) if x in by_id]
        if cands: out.append((f"s2_{c['id']}", pl.s2_system(iaps, grade_only=True), pl.s2_grade_user(c, c["site"], cands)))
    return out

def parse_json(raw):
    try: return json.loads(raw[raw.find("{"): raw.rfind("}") + 1])
    except Exception: return {}

# ---------- Anthropic (Haiku) ----------
def run_haiku(reqs):
    batch = pl.client.messages.batches.create(requests=[{"custom_id": cid, "params": {"model": HAIKU, "max_tokens": 4000,
             "system": pl.cached_system(sysm), "messages": [{"role": "user", "content": user}]}} for cid, sysm, user in reqs])
    log("haiku batch", batch.id, "waiting")
    while pl.client.messages.batches.retrieve(batch.id).processing_status != "ended": time.sleep(30)
    out, usage = {}, [0, 0]
    for item in pl.client.messages.batches.results(batch.id):
        if item.result.type != "succeeded": continue
        m = item.result.message
        usage[0] += m.usage.input_tokens + (getattr(m.usage, "cache_read_input_tokens", 0) or 0) + (getattr(m.usage, "cache_creation_input_tokens", 0) or 0)
        usage[1] += m.usage.output_tokens
        out[item.custom_id] = parse_json("".join(x.text for x in m.content if x.type == "text"))
    return out, usage

# ---------- OpenAI (Luna) via Batch API ----------
def run_luna(reqs):
    lines = [json.dumps({"custom_id": cid, "method": "POST", "url": "/v1/chat/completions", "body": {
                "model": LUNA, "max_completion_tokens": 4000, "response_format": {"type": "json_object"},
                "messages": [{"role": "system", "content": sysm}, {"role": "user", "content": user}]}}) for cid, sysm, user in reqs]
    f = requests.post("https://api.openai.com/v1/files", headers=OAI, files={"file": ("batch.jsonl", "\n".join(lines))},
                      data={"purpose": "batch"}, timeout=120); f.raise_for_status()
    b = requests.post("https://api.openai.com/v1/batches", headers=OAI, json={"input_file_id": f.json()["id"],
                      "endpoint": "/v1/chat/completions", "completion_window": "24h"}, timeout=60); b.raise_for_status()
    bid = b.json()["id"]; log("luna batch", bid, "waiting")
    while True:
        st = requests.get(f"https://api.openai.com/v1/batches/{bid}", headers=OAI, timeout=60).json()
        if st["status"] in ("completed", "failed", "expired", "cancelled"): break
        time.sleep(30)
    if st["status"] != "completed": log("luna batch", st["status"], st.get("errors")); return {}, [0, 0]
    txt = requests.get(f"https://api.openai.com/v1/files/{st['output_file_id']}/content", headers=OAI, timeout=120).text
    out, usage = {}, [0, 0]
    for line in txt.splitlines():
        j = json.loads(line); body = (j.get("response") or {}).get("body") or {}
        if not body.get("choices"): continue
        u = body.get("usage", {}); usage[0] += u.get("prompt_tokens", 0); usage[1] += u.get("completion_tokens", 0)
        out[j["custom_id"]] = parse_json(body["choices"][0]["message"]["content"])
    return out, usage

def ticks_from(step1_out):
    t = {}
    for res in step1_out.values():
        for cid, ids in res.items(): t[str(cid)] = {str(x) for x in ids if str(x) in by_id}
    return t

def final_from(step2_out):
    f, sc = {}, {}
    for cid, res in step2_out.items():
        c = cid[3:]; scores = {k: v for k, v in (res.get("scores") or {}).items() if k in by_id}
        sc[c] = scores; f[c] = {k for k, v in scores.items() if int(v.get("score", 0)) >= 4}
    return f, sc

# ---------- run both ----------
s1 = step1_requests()
h1, hu1 = run_haiku(s1); l1, lu1 = run_luna(s1)
ht, lt = ticks_from(h1), ticks_from(l1)
log(f"step 1 ticked: haiku {sum(1 for c in comps if ht.get(c['id']))}, luna {sum(1 for c in comps if lt.get(c['id']))}")
h2, hu2 = run_haiku(step2_requests(ht)) if any(ht.values()) else ({}, [0, 0])
l2, lu2 = run_luna(step2_requests(lt)) if any(lt.values()) else ({}, [0, 0])
hf, hs = final_from(h2); lf, ls = final_from(l2)

# ---------- report ----------
names = lambda ids: ", ".join(by_id[x]["name"] for x in sorted(ids))
with open("compare_luna.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["company_id", "name", "pipeline_final (in HubSpot)", "haiku_step1", "haiku_final", "haiku_scores", "luna_step1", "luna_final", "luna_scores", "haiku_vs_luna"])
    agree = 0
    for c in comps:
        i = c["id"]; a, b = hf.get(i, set()), lf.get(i, set())
        same = a == b; agree += same
        fmt = lambda sc: "; ".join(f"{by_id[k]['name']}={v.get('score')}" for k, v in sc.items())
        w.writerow([i, c["name"], names(c["hs_ticks"]), names(ht.get(i, set())), names(a), fmt(hs.get(i, {})),
                    names(lt.get(i, set())), names(b), fmt(ls.get(i, {})), "same" if same else "DIFFERENT"])
log(f"final campaigns agree on {agree} of {len(comps)} companies")
log(f"tokens  haiku: {hu1[0]+hu2[0]} in / {hu1[1]+hu2[1]} out   luna: {lu1[0]+lu2[0]} in / {lu1[1]+lu2[1]} out")
log("written compare_luna.csv")
