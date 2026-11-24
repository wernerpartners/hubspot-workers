"""Export every contact whose first or last name starts with a retired "z" prefix.
Matches: zJim, zzzJames, z Jim, z-Jim, z_Jim, ZJim, ZZJim. Does NOT match real Z names (Zumbrun, Zhao, Zan).
Columns include Last Contact Date, Last Outreach Date and the HubSpot link.
Run from ~/Documents/WP/hubspot-dedupe:  python3 export_z_contacts.py   -> z_contacts.csv
Read-only: changes nothing in HubSpot."""
import re
from common import page_all, write_csv, log

Z = re.compile(r"^(z+(?=[A-Z0-9\s\-_.])|Z{2,}|Z(?=[A-Z][a-z]))")
PROPS = ["firstname", "lastname", "email", "company", "jobtitle", "last_contact_date", "last_outreach_date",
         "createdate", "hubspot_owner_id"]
URL = "https://app-na2.hubspot.com/contacts/243636394/record/0-1/{}"

rows = []
for r in page_all("contacts", PROPS, "contacts"):
    p = r["properties"]
    fn, ln = (p.get("firstname") or "").strip(), (p.get("lastname") or "").strip()
    if Z.match(fn) or Z.match(ln):
        rows.append([r["id"], fn, ln, p.get("email"), p.get("company"), p.get("jobtitle"),
                     (p.get("last_contact_date") or "")[:10], (p.get("last_outreach_date") or "")[:10],
                     (p.get("createdate") or "")[:10], p.get("hubspot_owner_id"), URL.format(r["id"])])
log(f"z contacts found: {len(rows):,}")
write_csv("z_contacts.csv", rows, ["id", "firstname", "lastname", "email", "company", "jobtitle",
          "last_contact_date", "last_outreach_date", "createdate", "owner_id", "hubspot_link"])
