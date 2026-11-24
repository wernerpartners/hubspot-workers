/**
 * IAP Review app — reviewers judge whether a company fits a campaign's IAP; verdicts are saved to the Campaign Config sheet.
 *
 * Standalone Apps Script project (script.google.com), deployed as a web app:
 *   Execute as: Me    Who has access: Anyone    (no login; reviewers pick their name)
 *
 * Script properties (Project Settings > Script properties):
 *   HUBSPOT_TOKEN   required. Private app token with crm.objects.companies.read and crm.objects.custom.read
 *                   (the same token the IAP pipeline uses works).
 *   REVIEW_TARGET   optional. Reviews wanted per campaign (default 50). The app says "done" when it is reached.
 *
 * Tabs it uses in the Campaign Config sheet (created by setup(), always added at the END so the sync script's first tab
 * is never touched):
 *   Reviewers       column A: reviewer names (row 1 is the header). You edit this list.
 *   Reviews         one row per verdict. Written by the app only.
 *   Review Summary  per campaign: reviewed, fit / not fit / can't tell, agreement with the pipeline. Rebuilt after each verdict.
 *
 * Queue per campaign: companies nobody has reviewed yet for that campaign, alternating between
 *   A) companies the pipeline ticked for it (enrichment_iap_campaigns, stage 2 Done)  -> pipeline says Fit
 *   B) companies with a score line for it in enrichment_iap_scores but not ticked     -> pipeline says Not a fit
 * so the team checks both directions. A company shown to one reviewer is held for 15 minutes so two people
 * don't get the same one.
 */

const CONFIG_SHEET_ID = '1dFFGlNayD_RUjFiSSpFgru63D2qUYpHhlczk1ONHeGs';
const CONFIG_TAB = 'Config File';
const REVIEWERS_TAB = 'Reviewers';
const REVIEWS_TAB = 'Reviews';
const SUMMARY_TAB = 'Review Summary';
const HS = 'https://api.hubapi.com';
const PORTAL_ID = '243636394';
const CAMPAIGN_OBJECT = '2-175770723';
const MAX_PER_GROUP = 1000;          // candidates read from HubSpot per group
const CLAIM_SECONDS = 900;           // a company shown to a reviewer is held this long
const REVIEW_HEADERS = ['Timestamp', 'Reviewer', 'Campaign ID', 'Campaign Name', 'Company ID', 'Company Name', 'Website',
  'Pipeline Score', 'Pipeline Reason', 'Pipeline Verdict', 'Reviewer Verdict', 'Reviewer Reason', 'Agreement', 'HubSpot URL'];
const COMPANY_PROPS = ['name', 'domain', 'website', 'city', 'state', 'country', 'numberofemployees', 'industry',
  'hs_industry_group', 'ownership', 'linkedin_company_page', 'enrichment_description', 'enrichment_keywords',
  'enrichment_iap_scores', 'enrichment_iap_campaigns', 'enrichment_website_summary', 'phone'];

// ---------- web app ----------
function doGet(e) {
  if (e && e.parameter && e.parameter.page === 'change') {        // "Request change" link from the Config sheet
    const t = HtmlService.createTemplateFromFile('ChangeRequest');
    t.campaignId = str_(e.parameter.id).replace(/\D/g, '');        // digits only: it is printed into the page
    return t.evaluate().setTitle('Request a change').addMetaTag('viewport', 'width=device-width, initial-scale=1');
  }
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('IAP Review')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// ---------- change requests (Config sheet "Request change" links) ----------
const CHANGE_FIELDS = [   // the only values a request can change, with their dropdown options
  { name: 'Campaign Status', options: ['Active', 'Inactive'] },
  { name: 'IAP Pipeline', options: ['Active', 'Inactive'] },
  { name: 'Campaign Priority (1-5)', options: ['1', '2', '3', '4', '5'], help: '1 is the highest, 5 is the lowest.' },
  { name: 'Campaign Batch Size', number: true, help: 'Most companies ever sent to the Smartlead campaign.' }
];

function configRow_(campaignId) {
  const sh = ss_().getSheetByName(CONFIG_TAB) || ss_().getSheets()[0];
  const vals = sh.getDataRange().getDisplayValues();
  const head = vals[0].map(str_);
  const idCol = head.indexOf('Campaign ID');
  if (idCol < 0) throw new Error('The Config sheet has no "Campaign ID" column.');
  for (let r = 1; r < vals.length; r++) if (str_(vals[r][idCol]).replace(/\D/g, '') === campaignId) return { sheet: sh, row: r + 1, head: head, values: vals[r] };
  throw new Error('Campaign ' + campaignId + ' is not in the Config sheet any more.');
}

function cell_(r, header) { const i = r.head.indexOf(header); return i < 0 ? '' : str_(r.values[i]); }

function getChangeRow(campaignId) {
  const r = configRow_(str_(campaignId));
  return {
    campaignId: str_(campaignId), campaignName: cell_(r, 'Campaign Name'), docLink: cell_(r, 'IAP Description Doc Link'),
    fields: CHANGE_FIELDS.map(function (f) { return { name: f.name, value: cell_(r, f.name), options: f.options || null, number: !!f.number, help: f.help || '' }; })
  };
}

function submitChangeRequest(req) {
  const who = str_(req.name), email = str_(req.email), note = str_(req.note), iap = str_(req.iapChanges);
  if (!who) throw new Error('Please type your name.');
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) throw new Error('Please type a valid email.');
  const r = configRow_(str_(req.campaignId));
  const changes = [];
  CHANGE_FIELDS.forEach(function (f) {
    if (!req.values || req.values[f.name] === undefined) return;
    const now = cell_(r, f.name), want = str_(req.values[f.name]);
    if (f.number && want && !/^\d+$/.test(want)) throw new Error(f.name + ' must be a whole number.');
    if (f.options && want && f.options.indexOf(want) < 0) return;
    if (now !== want) changes.push([f.name, now, want]);
  });
  if (!changes.length && !iap && !note) throw new Error('Nothing to send: change a value, describe the IAP changes, or add a note.');
  const name = cell_(r, 'Campaign Name') || req.campaignId, doc = cell_(r, 'IAP Description Doc Link');
  const link = 'https://docs.google.com/spreadsheets/d/' + CONFIG_SHEET_ID + '/edit#gid=' + r.sheet.getSheetId() + '&range=A' + r.row;
  const esc = function (t) { return String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br>'); };
  const td = 'padding:6px;border:1px solid #ddd';
  const rows = changes.map(function (c) {
    return '<tr><td style="' + td + '"><b>' + esc(c[0]) + '</b></td><td style="' + td + '">' + esc(c[1] || '(empty)') +
      '</td><td style="' + td + ';background:#fff4e5">' + esc(c[2] || '(empty)') + '</td></tr>';
  }).join('');
  const html = '<div style="font-family:sans-serif;font-size:14px"><p><b>' + esc(who) + '</b>' + (email ? ' (' + esc(email) + ')' : '') +
    ' requests a change to <b>' + esc(name) + '</b> (campaign ' + esc(req.campaignId) + ').</p>' +
    (rows ? '<table style="border-collapse:collapse"><tr><th style="' + td + '">Field</th><th style="' + td + '">Now</th><th style="' + td +
      '">Requested</th></tr>' + rows + '</table>' : '') +
    (iap ? '<p><b>Requested changes to the IAP document</b>' + (doc ? ' (<a href="' + esc(doc) + '">open the IAP doc</a>)' : '') + ':<br>' + esc(iap) + '</p>' : '') +
    (note ? '<p><b>Note:</b><br>' + esc(note) + '</p>' : '') +
    '<p><a href="' + link + '">Open the row in the Campaign Config sheet</a></p></div>';
  const text = changes.map(function (c) { return c[0] + ': ' + (c[1] || '(empty)') + ' -> ' + (c[2] || '(empty)'); }).join('\n') +
    (iap ? '\n\nIAP document changes' + (doc ? ' (' + doc + ')' : '') + ':\n' + iap : '') + (note ? '\n\nNote: ' + note : '') + '\n\n' + link;
  const to = props_().getProperty('NOTIFY_EMAIL') || 'blanca@blackorchidio.com';
  const opts = { htmlBody: html, name: 'Campaign Config' };
  if (/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) opts.replyTo = email;
  MailApp.sendEmail(to, 'Change request: ' + name + ' (from ' + who + ')', text, opts);
  return { sent: true, changes: changes.length, iap: !!iap };
}

// ---------- called from the page ----------
function getInit() {
  const reviewers = sheetValues_(REVIEWERS_TAB).slice(1).map(function (r) { return str_(r[0]); }).filter(String);
  return { reviewers: reviewers, campaigns: campaigns_(), target: target_() };
}

function getNext(campaignId, reviewer, skipIds) {
  campaignId = str_(campaignId);
  const camp = campaigns_().filter(function (c) { return c.id === campaignId; })[0];
  if (!camp) throw new Error('Campaign ' + campaignId + ' is not in the Config sheet.');
  const prog = progress_(campaignId);
  if (prog.reviewed >= target_()) return { done: true, progress: prog };

  const cand = candidates_(campaignId);
  const reviewed = reviewedIds_(campaignId);
  const skip = {};
  (skipIds || []).forEach(function (id) { skip[str_(id)] = true; });
  const cache = CacheService.getScriptCache();
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);
  let pick = null, group = '';
  try {
    // alternate: take from the group with fewer reviews so far
    const order = prog.pipelineFit <= prog.pipelineNotFit ? ['a', 'b'] : ['b', 'a'];
    for (let g = 0; g < 2 && !pick; g++) {
      const list = cand[order[g]];
      for (let i = 0; i < list.length; i++) {
        const id = list[i];
        if (reviewed[id] || skip[id]) continue;
        const held = cache.get('claim:' + campaignId + ':' + id);
        if (held && held !== reviewer) continue;
        pick = id; group = order[g]; break;
      }
    }
    if (pick) cache.put('claim:' + campaignId + ':' + pick, reviewer || '?', CLAIM_SECONDS);
  } finally { lock.releaseLock(); }
  if (!pick) return { done: true, empty: true, progress: prog };

  const r = hs_('GET', '/crm/v3/objects/companies/' + pick + '?properties=' + COMPANY_PROPS.join(','));
  const p = r.properties || {};
  const sc = parseScore_(p.enrichment_iap_scores, campaignId, camp.name);
  const ticked = str_(p.enrichment_iap_campaigns).split(';').indexOf(campaignId) >= 0;
  let verdict = sc ? (sc.score >= 3 ? 'Fit' : 'Not a fit') : (ticked ? 'Fit' : 'Unknown');
  const site = siteUrl_(p.website, p.domain);
  return {
    done: false,
    progress: prog,
    campaign: { id: camp.id, name: camp.name, iap: iapText_(camp.id) },
    company: {
      id: pick, name: str_(p.name), website: site, domain: str_(p.domain),
      location: [p.city, p.state, p.country].map(str_).filter(String).join(', '),
      employees: str_(p.numberofemployees), industry: str_(p.industry), industryGroup: str_(p.hs_industry_group),
      ownership: str_(p.ownership), linkedin: str_(p.linkedin_company_page), phone: str_(p.phone),
      description: str_(p.enrichment_description), keywords: str_(p.enrichment_keywords),
      summary: str_(p.enrichment_website_summary),
      hubspotUrl: 'https://app.hubspot.com/contacts/' + PORTAL_ID + '/record/0-2/' + pick
    },
    pipeline: { score: sc ? sc.score : '', reason: sc ? sc.reason : '', date: sc ? sc.date : '', verdict: verdict, group: group }
  };
}

function submitReview(v) {
  const verdict = { fit: 'Fit', notfit: 'Not a fit', cant: "Can't tell" }[v.verdict];
  if (!verdict) throw new Error('Unknown verdict.');
  const reason = str_(v.reason);
  if (v.verdict === 'notfit' && !reason) throw new Error('A reason is required for "Not a fit".');
  if (!str_(v.reviewer)) throw new Error('Pick your name first.');
  let agreement = '';
  if (verdict !== "Can't tell" && (v.pipelineVerdict === 'Fit' || v.pipelineVerdict === 'Not a fit')) {
    agreement = verdict === v.pipelineVerdict ? 'Agree' : 'Disagree';
  }
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    if (reviewedIds_(str_(v.campaignId))[str_(v.companyId)]) return { progress: progress_(str_(v.campaignId)), duplicate: true };
    const sh = tab_(REVIEWS_TAB, REVIEW_HEADERS);
    sh.appendRow([new Date(), str_(v.reviewer), "'" + str_(v.campaignId), str_(v.campaignName), "'" + str_(v.companyId),
      str_(v.companyName), str_(v.website), v.pipelineScore === '' ? '' : Number(v.pipelineScore), str_(v.pipelineReason),
      str_(v.pipelineVerdict), verdict, reason, agreement, str_(v.hubspotUrl)]);
    CacheService.getScriptCache().remove('claim:' + v.campaignId + ':' + v.companyId);
    rebuildSummary_();
  } finally { lock.releaseLock(); }
  return { progress: progress_(str_(v.campaignId)) };
}

function releaseClaim(campaignId, companyId) {
  CacheService.getScriptCache().remove('claim:' + str_(campaignId) + ':' + str_(companyId));
}

// ---------- data ----------
function campaigns_() {
  const vals = sheetValues_(CONFIG_TAB, true);
  if (!vals.length) return [];
  const h = vals[0].map(str_);
  const ni = h.indexOf('Campaign Name'), ii = h.indexOf('Campaign ID');
  if (ni < 0 || ii < 0) throw new Error('Config sheet needs "Campaign Name" and "Campaign ID" headers.');
  return vals.slice(1).map(function (r) { return { id: str_(r[ii]), name: str_(r[ni]) }; })
    .filter(function (c) { return /^\d+$/.test(c.id) && c.name; });
}

function candidates_(campaignId) {
  const cache = CacheService.getScriptCache();
  const hit = cache.get('cand:' + campaignId);
  if (hit) return JSON.parse(hit);
  const a = searchIds_([{ propertyName: 'enrichment_iap_campaigns', operator: 'CONTAINS_TOKEN', value: campaignId },
                        { propertyName: 'enrichment_status', operator: 'EQ', value: 'Done' }]);
  let b = [];
  try {
    const inA = {};
    a.forEach(function (id) { inA[id] = true; });
    b = searchIds_([{ propertyName: 'enrichment_iap_scores', operator: 'CONTAINS_TOKEN', value: campaignId }])
      .filter(function (id) { return !inA[id]; });
  } catch (err) { console.log('score search failed: ' + err); }
  const out = { a: a, b: b };
  try { cache.put('cand:' + campaignId, JSON.stringify(out), 1800); } catch (err) {}
  return out;
}

function searchIds_(filters) {
  const ids = [];
  let after = null;
  while (ids.length < MAX_PER_GROUP) {
    const body = { filterGroups: [{ filters: filters }], properties: ['name'], limit: 200,
                   sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'DESCENDING' }] };
    if (after) body.after = after;
    const res = hs_('POST', '/crm/v3/objects/companies/search', body);
    (res.results || []).forEach(function (r) { ids.push(String(r.id)); });
    after = res.paging && res.paging.next && res.paging.next.after;
    if (!after) break;
  }
  return ids;
}

/** Latest score for this campaign. New lines: "ID - score - reason - YYYY-MM-DD". Old lines: "Campaign name: score - reason". */
function parseScore_(text, campaignId, campaignName) {
  let found = null;
  str_(text).split('\n').forEach(function (line) {
    line = line.trim();
    let m = line.match(/^(\d+)\s+-\s+([1-5])\s+-\s+(.*?)(?:\s+-\s+(\d{4}-\d{2}-\d{2}))?$/);
    if (m && m[1] === campaignId) { found = { score: Number(m[2]), reason: m[3], date: m[4] || '' }; return; }
    if (!found || !found.date) {
      if (campaignName && line.indexOf(campaignName + ':') === 0) {
        m = line.slice(campaignName.length + 1).match(/^\s*([1-5])(?:\s+-\s+(.*))?$/);
        if (m) found = { score: Number(m[1]), reason: m[2] || '', date: '' };
      }
    }
  });
  return found;
}

function iapText_(campaignId) {
  const cache = CacheService.getScriptCache();
  const hit = cache.get('iap:' + campaignId);
  if (hit !== null) return hit;
  let t = '';
  try {
    const r = hs_('GET', '/crm/v3/objects/' + CAMPAIGN_OBJECT + '/' + campaignId + '?properties=iap_description');
    t = str_((r.properties || {}).iap_description);
  } catch (err) { t = '(IAP description could not be loaded: ' + err.message + ')'; }
  try { cache.put('iap:' + campaignId, t.slice(0, 90000), 21600); } catch (err) {}
  return t;
}

function reviewedIds_(campaignId) {
  const out = {};
  sheetValues_(REVIEWS_TAB).slice(1).forEach(function (r) {
    if (str_(r[2]) === campaignId) out[str_(r[4])] = true;
  });
  return out;
}

function progress_(campaignId) {
  const p = { reviewed: 0, fit: 0, notFit: 0, cant: 0, agree: 0, disagree: 0, pipelineFit: 0, pipelineNotFit: 0, target: target_() };
  sheetValues_(REVIEWS_TAB).slice(1).forEach(function (r) {
    if (str_(r[2]) !== campaignId) return;
    p.reviewed++;
    if (r[10] === 'Fit') p.fit++; else if (r[10] === 'Not a fit') p.notFit++; else p.cant++;
    if (r[12] === 'Agree') p.agree++; else if (r[12] === 'Disagree') p.disagree++;
    if (r[9] === 'Fit') p.pipelineFit++; else if (r[9] === 'Not a fit') p.pipelineNotFit++;
  });
  return p;
}

function rebuildSummary_() {
  const rows = sheetValues_(REVIEWS_TAB).slice(1);
  const by = {};
  rows.forEach(function (r) {
    const id = str_(r[2]);
    if (!id) return;
    const s = by[id] || (by[id] = { name: str_(r[3]), n: 0, fit: 0, nf: 0, ct: 0, ag: 0, dis: 0, fp: 0, fn: 0, last: '' });
    s.n++;
    if (r[10] === 'Fit') s.fit++; else if (r[10] === 'Not a fit') s.nf++; else s.ct++;
    if (r[12] === 'Agree') s.ag++;
    if (r[12] === 'Disagree') { s.dis++; if (r[9] === 'Fit') s.fp++; else s.fn++; }
    s.last = r[0];
  });
  const out = [['Campaign ID', 'Campaign Name', 'Reviewed', 'Fit', 'Not a fit', "Can't tell", 'Agree', 'Disagree',
    'Agreement %', 'Pipeline Fit, team Not a fit', 'Pipeline Not a fit, team Fit', 'Last Review']];
  Object.keys(by).forEach(function (id) {
    const s = by[id], judged = s.ag + s.dis;
    out.push(["'" + id, s.name, s.n, s.fit, s.nf, s.ct, s.ag, s.dis, judged ? s.ag / judged : '', s.fp, s.fn, s.last]);
  });
  const sh = tab_(SUMMARY_TAB, out[0]);
  sh.clearContents();
  sh.getRange(1, 1, out.length, out[0].length).setValues(out);
  sh.getRange(1, 1, 1, out[0].length).setFontWeight('bold');
  if (out.length > 1) sh.getRange(2, 9, out.length - 1, 1).setNumberFormat('0%');
}

// ---------- helpers ----------
function props_() { return PropertiesService.getScriptProperties(); }
function ss_() { return SpreadsheetApp.openById(CONFIG_SHEET_ID); }
function str_(v) { return v === null || v === undefined ? '' : String(v).trim(); }
function target_() { return Number(props_().getProperty('REVIEW_TARGET')) || 50; }

function tab_(name, headers) {
  const ss = ss_();
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name, ss.getSheets().length);   // always at the end: the first tab belongs to the sync script
    sh.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold');
    sh.setFrozenRows(1);
  }
  return sh;
}

function sheetValues_(name, fallbackFirst) {
  const ss = ss_();
  let sh = ss.getSheetByName(name);
  if (!sh && fallbackFirst) sh = ss.getSheets()[0];
  if (!sh || sh.getLastRow() < 1) return [];
  return sh.getRange(1, 1, sh.getLastRow(), Math.max(sh.getLastColumn(), 1)).getValues();
}

function siteUrl_(website, domain) {
  let u = str_(website) || str_(domain);
  if (!u) return '';
  if (!/^https?:\/\//i.test(u)) u = 'https://' + u;
  return u.replace(/^http:\/\//i, 'https://');
}

function hs_(method, path, body) {
  const token = props_().getProperty('HUBSPOT_TOKEN');
  if (!token) throw new Error('Script property HUBSPOT_TOKEN is not set.');
  for (let attempt = 0; attempt < 5; attempt++) {
    const opt = { method: method, muteHttpExceptions: true, headers: { Authorization: 'Bearer ' + token } };
    if (body) { opt.contentType = 'application/json'; opt.payload = JSON.stringify(body); }
    const res = UrlFetchApp.fetch(HS + path, opt);
    const code = res.getResponseCode();
    if (code === 429 || code >= 500) { Utilities.sleep(Math.min(1000 * Math.pow(2, attempt), 8000)); continue; }
    if (code >= 400) throw new Error('HubSpot ' + code + ': ' + res.getContentText().slice(0, 300));
    return res.getContentText() ? JSON.parse(res.getContentText()) : {};
  }
  throw new Error('HubSpot kept returning errors for ' + path);
}
