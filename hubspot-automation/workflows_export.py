#!/usr/bin/env python3
"""
Export all ACTIVE HubSpot workflows to an Excel file.

Setup:
  pip install requests openpyxl
  export HUBSPOT_TOKEN="your-service-key"
Run:
  python3 workflows_export.py

Scopes: automation (required). settings.users.read (optional, to turn creator user IDs into emails).
"""
import os, sys, json, time, requests
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not TOKEN:
    sys.exit("Set HUBSPOT_TOKEN first.")

BASE = "https://api.hubapi.com"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
OUT = "active_workflows.xlsx"

OBJECT_TYPES = {
    "0-1": "Contact", "0-2": "Company", "0-3": "Deal", "0-5": "Ticket",
    "2-175770805": "Campaign Members", "2-175770723": "Campaigns (Telemarketing)",
    "2-224178753": "Originators",
}

def get(url, params=None):
    for attempt in range(5):
        r = requests.get(url, headers=H, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt); continue
        return r
    return r

# 1) List every workflow (paginated)
flows, after = [], None
while True:
    p = {"limit": 100}
    if after: p["after"] = after
    r = get(f"{BASE}/automation/v4/flows", p)
    if r.status_code != 200:
        sys.exit(f"List failed {r.status_code}: {r.text}")
    d = r.json()
    flows += d.get("results", [])
    after = d.get("paging", {}).get("next", {}).get("after")
    if not after: break
print(f"Total workflows in account: {len(flows)}")

# 2) Keep only active ones starting with FW
fw = [f for f in flows if f.get("isEnabled")]
print(f"Active workflows: {len(fw)}")

# 3) Pull full detail for each one
user_cache = {}
def user_email(uid):
    if uid in (None, ""): return ""
    if uid in user_cache: return user_cache[uid]
    r = get(f"{BASE}/settings/v3/users/{uid}")
    user_cache[uid] = r.json().get("email", "") if r.status_code == 200 else f"(lookup failed {r.status_code})"
    return user_cache[uid]

rows = []
for f in fw:
    fid = f["id"]
    r = get(f"{BASE}/automation/v4/flows/{fid}")
    det = r.json() if r.status_code == 200 else {}
    err = "" if r.status_code == 200 else f"{r.status_code}: {r.text[:200]}"

    # Creator: report whatever creator/updater fields the API actually returns
    creator_fields = {k: v for k, v in det.items()
                      if any(s in k.lower() for s in ("createdby", "creator", "updatedby", "userid", "author"))}
    creator_ids = [v for k, v in creator_fields.items() if "creat" in k.lower() or "author" in k.lower()]
    creator_id = creator_ids[0] if creator_ids else ""
    creator_email = user_email(creator_id) if creator_id else ""

    actions = det.get("actions", []) or []
    enroll = det.get("enrollmentCriteria", {}) or {}

    rows.append({
        "Flow ID": fid,
        "Name": f.get("name"),
        "Active": f.get("isEnabled"),
        "Object type": OBJECT_TYPES.get(f.get("objectTypeId"), f.get("objectTypeId")),
        "Flow type": f.get("flowType"),
        "Created at": f.get("createdAt"),
        "Updated at": f.get("updatedAt"),
        "Revision": f.get("revisionId"),
        "Creator user ID": creator_id or "(not returned by API)",
        "Creator email": creator_email,
        "Forgeworks creator": "Yes" if "@forgeworks.ai" in str(creator_email).lower() else "",
        "All creator/updater fields": json.dumps(creator_fields) if creator_fields else "",
        "Description": det.get("description", ""),
        "Enrollment type": enroll.get("type", ""),
        "Re-enrollment": "Yes" if enroll.get("shouldReEnroll") else "No",
        "Unenroll if criteria not met": enroll.get("unEnrollObjectsNotMeetingCriteria", ""),
        "Number of actions": len(actions),
        "Action type IDs": ", ".join(sorted({a.get("actionTypeId", "") for a in actions if a.get("actionTypeId")})),
        "Suppression list IDs": ", ".join(map(str, det.get("suppressionListIds", []) or [])),
        "Goal criteria set": "Yes" if det.get("goalFilterBranch") else "No",
        "Time windows": json.dumps(det.get("timeWindows", [])) if det.get("timeWindows") else "",
        "Blocked dates": json.dumps(det.get("blockedDates", [])) if det.get("blockedDates") else "",
        "Can enroll from Salesforce": det.get("canEnrollFromSalesforce", ""),
        "Detail error": err,
    })

# 4) Write Excel: summary sheet + raw JSON sheet
wb = Workbook()
ws = wb.active; ws.title = "Active Workflows"
cols = list(rows[0].keys()) if rows else ["No active workflows found"]
ws.append(cols)
for row in rows: ws.append([str(v) if isinstance(v, (dict, list)) else v for v in row.values()])
for i, c in enumerate(cols, 1):
    ws.column_dimensions[get_column_letter(i)].width = min(60, max(12, len(c) + 4))
ws.freeze_panes = "A2"

raw = wb.create_sheet("Raw JSON")
raw.append(["Flow ID", "Full API response"])
for f in fw:
    r = get(f"{BASE}/automation/v4/flows/{f['id']}")
    raw.append([f["id"], r.text[:32000]])

wb.save(OUT)
print(f"Saved {OUT}")
