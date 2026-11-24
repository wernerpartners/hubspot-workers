# hubspot-dedupe

Uses HubSpot date-based API version 2026-09 (`/crm/objects/2026-09/...`). Legacy `/crm/v3/` paths in the other workers
(match, enrich, hygiene) must be migrated before September 2027.

Two workers, run in order:

1. `dedupe_companies.py` — certain duplicate = same normalised domain, or same normalised name + same city.
   Same name with a missing or different city goes to `companies_review.csv`, never merged.
2. `dedupe_contacts.py` — certain duplicate = same full name + same company name. Phone and city only block a
   merge when both records have one and they differ (those pairs go to `contacts_review.csv`). Email is never used.

Survivor = latest Last Contact Date, then latest Last Outreach Date, then the older record. The survivor keeps its
values; HubSpot fills its blanks from the deleted record.

Every run writes `*_merges.csv` (what was or would be merged) and `*_review.csv` (needs a human), in the folder it runs from.

## Run locally

    pip3 install requests
    export HUBSPOT_TOKEN=pat-na2-...
    DRY_RUN=true python3 dedupe_companies.py                    # report only, nothing changes
    DRY_RUN=false MAX_MERGES=100 PLAN=companies_merges.csv python3 dedupe_companies.py   # merges from the reviewed plan, no re-read
    # each live run removes what it merged from the plan file; rerun with a higher MAX_MERGES to finish
    DRY_RUN=true python3 dedupe_contacts.py
    DRY_RUN=false MAX_MERGES=500 python3 dedupe_contacts.py

The key needs companies and contacts read + write.
