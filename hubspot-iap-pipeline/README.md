# hubspot-iap-pipeline

One run, two matching passes, over a scope you choose.

1. Stage 1, cheap: Haiku matches every company in scope against all active IAPs from the description
   already on the record. Ticks `enrichment_iap_campaigns`.
2. Stage 2, deep: only companies stage 1 ticked. Crawls the website, rewrites `enrichment_description` and
   `enrichment_keywords`, grades every candidate IAP 1-5 with a reason (Sonnet), REPLACES the ticks with
   the 4s and 5s, writes `enrichment_iap_scores` and `iap_primary_campaign` (best priority, then best score).

Scope: `RESEARCH_TAG="value"` runs only companies whose Research Tags contain that value and re-matches
them even if already Done. Without it the default is `SCOPE=1` (companies created in the last 24 hours);
also `SCOPE=all` (whole CRM, resumable, 5,000 per run), `SCOPE=15` (last 15 days), `SCOPE=list:818`
(a segment). `RERUN=true` re-matches everything in scope even if already Done (use after adding IAPs).

Batches take up to an hour, so a company flows stage 1 -> stage 2 across 2-3 hourly runs.

## HubSpot properties (create once)
Companies: `enrichment_iap_scores` multi-line text; `iap_primary_campaign` single-line text;
`iap_match_date` and `enrichment_date` date picker with time (stamped when each stage finishes, used to
find stale companies later); `enriched` single checkbox (set to true when stage 2 finishes).

Stage 2 also cleans the record. Website: if it does not load or does not match the company name/description,
Claude web search finds the official site and `website` + `domain` are overwritten. LinkedIn Company Page:
written only when the company's own site links to it or search confirms name and domain; otherwise the
existing value stays. Phone Number: filled only when blank, from the site's phone links and contact page,
checked with the phonenumbers library. Web search costs $10 per 1,000 searches, max 4 per company.
Campaigns object: `campaign_priority` radio select 1 to 5, 1 = highest (optional; missing = 3). Inactive
campaigns (campaign_active off) are never ticked.

## Deploy
    pip3 install anthropic
    export ANTHROPIC_API_KEY=...  WORKER_ANTHROPIC_KEY=...  HUBSPOT_TOKEN=...
    python3 setup_pipeline.py                  # last 24 hours of new companies, hourly schedule
    RESEARCH_TAG="value" python3 setup_pipeline.py   # only companies with that Research Tag
    SCOPE=all python3 setup_pipeline.py        # one-off full pass (separate deployment)

## Local test (no deployment)
    pip3 install -r requirements.txt
    export ANTHROPIC_API_KEY=...  HUBSPOT_TOKEN=...
    python3 pipeline.py          # submits stage 1 for the last 24 hours
    python3 pipeline.py          # run again about an hour later: picks up stage 1, submits stage 2
    python3 pipeline.py          # another hour later: picks up stage 2

Cost guide: stage 1 about $0.04 per 1,000 companies; stage 2 about 2-3 cents per company on Sonnet.
STAGE2_MAX (default 500) caps stage 2 per run.
Uses HubSpot date-based API 2026-09.
