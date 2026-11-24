"""
test_step_lookup.py — READ ONLY. Checks the Smartlead "filter" endpoint support described (it is not in the docs).
It changes nothing in Smartlead or HubSpot.
  source ../.env && python3 test_step_lookup.py              # uses campaign 3833247
  source ../.env && python3 test_step_lookup.py 3994052      # another campaign
"""
import os, sys, json, requests
SL = "https://server.smartlead.ai/api/v1"; K = {"api_key": os.environ["SMARTLEAD_API_KEY"]}
cid = sys.argv[1] if len(sys.argv) > 1 else "3833247"

steps = requests.get(f"{SL}/campaigns/{cid}/sequences", params=K, timeout=60).json()
print(f"Campaign {cid} has {len(steps)} steps. Fields Smartlead returns for a step: {sorted(steps[0].keys())}")
for s in steps: print(f"  step {s.get('seq_number')}: step id {s.get('id')}, subject '{s.get('subject') or s.get('follow_up_subject')}'")
by_id = {str(s.get("id")): s for s in steps}

r = requests.get(f"{SL}/campaigns/{cid}/leads", params={**K, "limit": 5, "offset": 0}, timeout=60).json()
print(f"\nCampaign has {r.get('total_leads')} leads. Looking up the first {len(r.get('data') or [])} with the filter endpoint:")
for x in r.get("data") or []:
    email = (x.get("lead") or {}).get("email")
    f = requests.post(f"{SL}/campaigns/{cid}/leads/filter", params=K, json={"searchText": email, "fieldSet": "active_table"}, timeout=60)
    print(f"\n  lead with campaign_lead_map_id {x.get('campaign_lead_map_id')} (status {x.get('status')}): filter endpoint answered HTTP {f.status_code}")
    try: body = f.json()
    except Exception: print("   reply was not JSON:", f.text[:300]); continue
    rows = body if isinstance(body, list) else next((v for v in body.values() if isinstance(v, list)), []) if isinstance(body, dict) else []
    if not rows: print("   no rows found. Raw reply:", json.dumps(body)[:600].replace(email or "~", "<email>")); continue
    row = rows[0]; print("   fields in the reply:", sorted(row.keys()))
    sid = str(row.get("email_campaign_seq_id")); st = by_id.get(sid)
    print(f"   id={row.get('id')} status={row.get('status')} current_seq_num={row.get('current_seq_num')} email_campaign_seq_id={sid}")
    print(f"   matches step: {'step ' + str(st.get('seq_number')) + ' (' + str(st.get('subject') or st.get('follow_up_subject')) + ')' if st else 'NO MATCH with any step id'}")
