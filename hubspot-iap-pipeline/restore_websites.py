"""
Puts back the original website + domain on companies where the 10:23 run replaced it with a buyer's site,
a deep page or a hosting subdomain. Run once:  python3 restore_websites.py
"""
import os, requests
from urllib.parse import urlparse
H = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
OLD = {
    "167202147030": "http://www.linxtechnologies.com", "167202016966": "http://www.stearnsstafford.com",
    "167202119388": "http://www.protechbayarea.com", "167202080493": "http://www.heavydutyinc.com",
    "167201691331": "http://www.hkgcpa.com", "167202155196": "http://www.capaccio.com",
    "167202075339": "http://www.ecksupply.com", "167202148061": "http://www.reddevilequipment.com",
    "167202104045": "http://www.combinedrefrigeration.com", "167202106057": "http://www.piroxllc.com",
    "167202184942": "http://www.tbssoutheast.com", "167202020071": "http://www.lpadesign.com",
    "167202184908": "http://www.spacecoastcableandharness.com",
}
def dom(u):
    h = urlparse(u).netloc.lower(); return h[4:] if h.startswith("www.") else h
inputs = [{"id": k, "properties": {"website": v, "domain": dom(v)}} for k, v in OLD.items()]
r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/batch/update", headers=H, json={"inputs": inputs}, timeout=60)
print(r.status_code, "restored", len(r.json().get("results", [])) if r.ok else r.text[:500])
