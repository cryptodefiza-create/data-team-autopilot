# Data Team Autopilot — Tester Guide

## Getting Started

1. **Access the app** at your provided URL (e.g. `https://<host>/tester-app`)
2. **Set your Org ID** — use the org_id given to you by the admin, or run the setup endpoint first
3. **Overview** — You will test the full data-to-insight pipeline: warehouse profiling, dashboard creation, memo generation, ad-hoc queries, and blind model comparison

### One-Shot Setup

If your org hasn't been provisioned yet, ask the admin to call:

```
POST /api/v1/admin/setup-tester-org
Body: { "org_id": "<your_org_id>" }
Headers: X-Tenant-Id: <your_org_id>, X-User-Role: admin
```

This creates the tenant, connects mock BigQuery, runs the profiler, and discovers 4 tables (users, events, orders, config) with PII detection on the email column.

---

## Test Scenarios

### Scenario A: Warehouse Profiling

**Steps:**
1. Click "Profile Warehouse" in the tester app
2. Wait for the profiling workflow to complete

**Expected Results:**
- 4 tables discovered: users, events, orders, config
- Email column flagged as PII (confidence >= 80%)
- Config table excluded from KPI starter recommendations
- Profile artifact created and visible in the Profile tab

**Rating (1-10):**
- Accuracy of table discovery: ___
- PII detection correctness: ___
- Response time: ___

---

### Scenario B: Dashboard Creation

**Steps:**
1. Click "Build Dashboard" or type "Create an executive dashboard from available tables"
2. Review the generated dashboard configuration

**Expected Results:**
- Dashboard includes metrics from users, events, and/or orders tables
- KPI selections are reasonable (DAU, revenue, etc.)
- Dashboard artifact created and visible in the Dashboards tab

**Rating (1-10):**
- Relevance of selected KPIs: ___
- Dashboard structure quality: ___
- Response time: ___

---

### Scenario C: Memo Generation

**Steps:**
1. Click "Generate Memo" or type "Generate my weekly executive memo"
2. Review the generated memo content

**Expected Results:**
- Memo contains headline summary, key changes, likely causes, recommended actions
- Numbers reference realistic DAU (200-400 range) and revenue data
- Data quality notes mention any anomalies

**Rating (1-10):**
- Accuracy of numbers cited: ___
- Quality of insights/causes: ___
- Actionability of recommendations: ___
- Writing quality/clarity: ___

---

### Scenario D: Ad-Hoc Data Queries

**Steps:**
1. Type "Show me DAU for the last 14 days"
2. Review the returned data
3. Try "What was total revenue last week?"

**Expected Results:**
- DAU query returns ~13 data points (one day missing — intentional gap)
- All DAU values in 200-400 range
- Revenue shows weekday > weekend pattern
- Query cost and bytes scanned are reported

**Rating (1-10):**
- Query interpretation accuracy: ___
- Data presentation clarity: ___
- Response time: ___

---

### Scenario E: Feedback Submission

**Steps:**
1. After any response, click the thumbs-up or thumbs-down icon
2. For thumbs-down, enter a comment describing the issue
3. Check the "My Feedback" tab to see your submissions

**Expected Results:**
- Feedback is recorded and visible in the My Feedback tab
- Comments are preserved
- Provider attribution is tracked

**Rating (1-10):**
- Feedback UX smoothness: ___
- Feedback visibility/tracking: ___

---

### Scenario F: Blind Model Comparison

**Steps:**
1. Click "Compare Models" on any chat response, or go to the Model Comparison tab
2. Click "Run Memo Evaluation"
3. Review the results for Model A, Model B, Model C (labels are randomized)

**Expected Results:**
- Results show blind labels (Model A/B/C), NOT real provider names
- Each model shows: valid JSON rate, passed checks, latency, token usage
- Same org always sees the same label-to-provider mapping

**Rating (1-10):**
- Model A output quality: ___
- Model B output quality: ___
- Model C output quality: ___
- Which model produced the best memo? ___

---

## Overall Assessment

| Category | Rating (1-10) | Notes |
|----------|--------------|-------|
| Data profiling accuracy | | |
| Dashboard relevance | | |
| Memo insight quality | | |
| Query handling | | |
| Blind comparison fairness | | |
| Overall UX/responsiveness | | |
| **Overall score** | | |

---

## Issue Log

Record any bugs, errors, or unexpected behavior:

| # | Scenario | Description | Severity (Low/Med/High) | Screenshot? |
|---|----------|-------------|------------------------|-------------|
| 1 | | | | |
| 2 | | | | |
| 3 | | | | |
| 4 | | | | |
| 5 | | | | |

---

## Submitting Results

After completing all scenarios, share this document (filled in) with the admin team along with any screenshots captured during testing.
