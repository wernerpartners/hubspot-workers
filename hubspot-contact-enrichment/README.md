# contact-enrichment-worker

Two scripts, same repo:

- `find_emails.py` (existing): fills missing emails with the Enrichley Email Finder across the whole CRM.
- `enrich_contacts.py` (new): the waterfall for the contacts of the IAP-matched companies of ONE campaign.

## enrich_contacts.py

Scope: companies with the campaign ticked in `enrichment_iap_campaigns` -> their associated contacts,
skipping `retired_contact = true` and `enriched = true`. Each contact is processed once.

Order per contact:

1. LinkedIn URL, only when blank. One Claude web search. Written only when the result shows the exact name and the company.
2. Email, only when blank.
   a. Enrichley Email Finder (name + domain + LinkedIn). Found emails are already verified.
   b. Findymail by name + domain, then Enrichley validation. Written only for status `ok` or `catch_all_validated`.
   c. Findymail by LinkedIn URL, then Enrichley validation. Skipped when the contact has no LinkedIn URL.
   d. If `CLAY_WEBHOOK_URL` is set, the contact is posted to Clay (see below). Otherwise the email stays empty.
3. Phone, only when `phone` and `mobilephone` are both blank.
   a. One Claude web search; a number is written only when the page ties it to the person. Company main lines are rejected.
   b. Findymail phone by LinkedIn URL (10 credits per number) until `PHONE_CREDIT_CAP` is reached. Cap 0 = off.
4. `enriched = true`, `enrichment_date`, `enrichment_status`, and `email_source` / `linkedin_source` / `phone_source`.

### One-time setup

```
cd ~/Documents/"Werner Partners"/hubspot/contact-enrichment-worker
pip3 install requests anthropic
export HUBSPOT_TOKEN=...
SETUP=true python3 enrich_contacts.py
```

Creates the contact properties `enriched`, `enrichment_date`, `enrichment_status`, `email_source`, `linkedin_source`, `phone_source`
if they do not exist. If the token lacks the property-write scope, create them by hand at
https://app.hubspot.com/property-settings/243636394/properties?type=0-1 with those exact internal names
(enriched = single checkbox, enrichment_date = date picker, the rest single-line text).

### Run

```
export HUBSPOT_TOKEN=... ENRICHLEY_API_KEY=... FINDYMAIL_API_KEY=... ANTHROPIC_API_KEY=...
CAMPAIGN=325749324474 DRY_RUN=true python3 enrich_contacts.py      # lists scope, calls no provider, writes nothing
CAMPAIGN=325749324474 LIMIT=10 python3 enrich_contacts.py          # first real run on 10 contacts
CAMPAIGN=325749324474 PHONE_CREDIT_CAP=100 python3 enrich_contacts.py   # allows up to 10 Findymail phone numbers
```

### Cost per contact (worst case, every step runs)

- Claude web search: 1 for LinkedIn + 1 for phone = $0.02 (web search is $10 per 1,000 searches) plus a few cents of tokens per 100 contacts.
- Enrichley finder: 1 credit only when an email is found. Enrichley validation: 1 credit per Findymail hit validated.
- Findymail: 1 credit per email found (not charged on misses), 10 credits per phone found.

### Clay waterfall (step 2d)

1. In Clay, create a table with source **Webhook** (Add source -> Webhook). Copy the webhook URL into `CLAY_WEBHOOK_URL`.
   The script posts: `hubspot_contact_id, first_name, last_name, full_name, company, domain, linkedin_url`.
2. Add a **Find work email** waterfall column (Enrichment -> Work email waterfall) and pick the providers you want
   (for example Prospeo, Datagma, Hunter, LeadMagic). Map name, domain, linkedin_url from the webhook columns.
3. Add a **HubSpot -> Update contact** action column: record id = `hubspot_contact_id`, Email = the waterfall result,
   `email_source` = "clay". Run it only when the waterfall returned an email.
4. Nothing else is needed on the HubSpot side: the contact already has `enriched = true` and `email_source = sent to clay`;
   Clay overwrites `email_source` when it writes the email.

### Notes

- `DRY_RUN=true` makes no provider call at all, so it costs nothing; it only shows the scope.
- Contacts with no name or no company domain are marked enriched with `enrichment_status = skipped: no name or company domain`.
- The Enrichley Email Finder is still in beta; endpoint and fields are the ones from their beta notice.
  The validation endpoint defaults to `https://api.enrichley.io/api/v1/validate-single-email`; override with `ENRICHLEY_VALIDATE_URL` if your dashboard shows a different path.
- Findymail response shapes are parsed defensively (`contact.email`, `contact.phones`); unexpected shapes are logged, not written.
