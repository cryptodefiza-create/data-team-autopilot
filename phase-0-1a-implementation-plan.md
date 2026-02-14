# Data Team Autopilot - Finalized Implementation Plan (Shared Phases)

This document finalizes only the phases shared so far:
- Phase 0 - Decision Lock
- Phase 1A - Minimal Platform + Contracts + Agent Loop v0

It incorporates the critical fixes required before build:
- explicit security gating for real query execution
- tenant-isolated Metabase strategy
- plan validation before execution
- deterministic failure handling policy
- PII confidence-tier workflow
- compliance-safe tenant deletion and audit retention policy
- measurable load/perf acceptance criteria

## Phase 0 - Decision Lock (2-3 days)

### Purpose
Freeze architecture and operational decisions that would cause rework if deferred.

### 0.1 Orchestrator
Decision: `Prefect Cloud` for v1.

Rationale:
- managed control plane
- no self-hosted infra during MVP window
- faster delivery than Temporal for initial scope

Future migration:
- consider `Temporal Cloud` at v1.5+ only if workflow fanout, long-running saga complexity, or retry/state needs exceed Prefect constraints.

Deliverable:
- ADR-001 `orchestrator.md` with choice, known limits, migration triggers.

### 0.2 Multi-Tenant Isolation
Decision: schema-per-tenant in Postgres.

Structure:
- `public`: tenants, users, billing, global config
- `tenant_<id>`: connections, catalogs, artifacts, feedback, audit

Required from day one:
- tenant-aware migration runner across all `tenant_<id>` schemas
- middleware-enforced `org_id` boundary checks
- explicit DB grants to prevent cross-schema read/write from app role

Deliverable:
- ADR-002 `tenancy.md`.

### 0.3 Warehouse Scope
Decision: BigQuery-only for v1 query execution.

Rationale:
- dry-run bytes estimates are reliable and directly enforceable for safety/cost gating.

Deliverable:
- ADR-003 `warehouse.md` with non-goal statement: no Snowflake/Postgres execution in MVP.

### 0.4 BI Strategy (Metabase Isolation Included)
Decision: programmatic dashboard creation via Metabase API.

Security boundary:
- one Metabase DB connection per tenant
- each connection restricted to tenant schema only (schema-limited credentials or enforced search path + DB permissions)
- no shared browse path across tenant schemas

Layout engine v1:
- two-column grid
- half-width and full-width cards

Deliverable:
- ADR-004 `metabase.md` including isolation model and API approach.

### 0.5 Timezone Standard
Decision:
- query windows computed in user timezone
- warehouse timestamps converted at query boundary
- UI and memo show the timezone used

Deliverable:
- ADR-005 `timezone.md`.

### 0.6 Internal Event Contract
Define immutable event schema used across agent, workflows, and observability.

Required events:
- `tool_invocation`
- `artifact_generated`
- `workflow_state_transition`
- `security_gate_decision`

Required fields:
- `org_id`, `session_id`, `workflow_id`, `step_id`, `status`, `error_code`, `started_at`, `finished_at`

Deliverable:
- `event-contract.md` and JSON schema files checked into repo.

### 0.7 Compliance + Retention Lock
Decision:
- tenant disconnect/purge flow must define retention policy explicitly.
- audit records are retained only in pseudonymized form with configurable retention window (default 90 days) unless contractual/legal requirements override.

Deliverable:
- `data-retention-policy.md` with delete and retention matrix.

### 0.8 Real-Query Feature Gate (Hard Dependency)
Decision:
- `ALLOW_REAL_QUERY_EXECUTION=false` by default.
- can only be enabled after Phase 2 security criteria pass.

Deliverable:
- feature flag documented in `runtime-flags.md`.

### Phase 0 Deliverables Checklist
- PRD v1 + Definition of Done
- non-goals list
- ADR-001..005
- event schemas
- retention policy
- runtime flag spec

### Phase 0 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| ADR freeze | All required ADRs committed and signed off |
| Non-goals lock | Out-of-scope features explicitly listed and approved |
| Event contract validation | Sample events pass JSON schema validation |
| Retention policy review | Tenant purge and audit retention behavior is unambiguous |
| Feature gate existence | App boot fails if `ALLOW_REAL_QUERY_EXECUTION` unset |

---

## Phase 1A - Minimal Platform + Contracts + Agent Loop v0 (2 weeks)

### Purpose
Deliver a working app skeleton and deterministic agent pipeline against stubs only. No production data queries.

### 1A.1 App Skeleton + Module Boundaries
Create modular monolith structure:

```text
src/
  api/
  agents/
  services/
  tools/
  workers/
  models/
  config/
  migrations/
tests/
  unit/
  integration/
  e2e/
```

Definition:
- `agents`: reasoning stages only
- `tools`: typed interfaces + adapter bindings
- `services`: orchestration/business rules

### 1A.2 Auth + RBAC + Tenant Boundary
Roles:
- `admin`
- `member`
- `viewer`

Rules:
- viewer cannot run generation/execution tools
- every API call requires tenant context
- every DB query scoped to tenant schema

### 1A.3 Tenant-Aware Migration Runner
Behavior:
- migrate `public`
- enumerate tenant schemas
- run schema migrations per tenant
- continue on per-tenant failure
- emit migration status report

Requirement:
- idempotent and resumable per schema.

### 1A.4 Agent Runtime Contract (Black Box Removed)
Pipeline:
1. Planner
2. Plan Validator
3. Critic (pre-exec)
4. Executor
5. Critic (post-exec)
6. Composer

Strict contract:
- all stages exchange typed JSON objects
- no raw freeform tool calls
- each stage logs transition event

#### Planner
Output:
- goal
- ordered step list
- declared risks
- required approvals

#### Plan Validator (must run before critic)
Checks:
- table exists in cached catalog
- column exists
- join key type compatibility
- required time column present

Failure action:
- reject plan with explicit validation errors and replan with corrected context.

#### Critic (pre-exec)
Checks:
- SQL operation safety class
- cost policy decision object (stub in 1A)
- PII policy decision object (stub in 1A)
- required partition/time filters present

Actions:
- allow
- rewrite (e.g., add `LIMIT`, narrow window)
- block and request approval

#### Executor
Behavior:
- execute validated step sequence against stub tool registry
- persist step output hashes
- map tool errors to normalized error codes

#### Critic (post-exec)
Checks:
- empty result anomalies
- null-heavy output
- shape mismatch (expected daily series but got scalar)

Actions:
- retry strategy suggestion
- escalate to user with safe explanation

#### Composer
Output format:
- `summary`
- `result` (typed)
- `provenance` (query/tool hashes)
- `freshness`
- `warnings`

### 1A.5 Failure Mode Catalog (for 1B parallel testing readiness)
Even in stub mode, standardize failure classes now:
- `TIMEOUT`
- `QUOTA_EXCEEDED`
- `PERMISSION_DENIED`
- `TRANSIENT_NETWORK`
- `MALFORMED_RESPONSE`
- `VALIDATION_FAILED`

Handling policy:
- retryable: `TIMEOUT`, `TRANSIENT_NETWORK` (bounded retries with backoff)
- terminal user-action: `QUOTA_EXCEEDED`, `PERMISSION_DENIED`
- terminal system-error: `MALFORMED_RESPONSE`
- terminal planning-error: `VALIDATION_FAILED`

### 1A.6 Audit Immutability + Security Logging
Requirements:
- app runtime role cannot update/delete audit rows
- dedicated writer path inserts append-only events
- include `security_gate_decision` events for allow/block/rewrite
- never log secrets or raw PII payloads

### 1A.7 Rate Limiting (Volume Active, Cost Stub)
Implement in Redis:
- per-user prompt rate limits
- per-org concurrency limits
- response includes retry metadata

Cost-based limit object exists but is non-enforcing in 1A.

### 1A.8 PII Classification Contract (UI-ready)
Define confidence tiers now:
- high confidence: auto-tag + notify
- medium confidence: require user review queue
- low confidence: suggestion only

Persist override decisions:
- user-approved/denied tags are durable and prevent repeated noisy prompts.

### 1A.9 Connector Lifecycle Spec (Execution Deferred)
Document only:
- connect
- disconnect
- stale artifact behavior
- in-flight workflow handling during disconnect

Disconnect rule:
- if active workflows exist, system must either cancel gracefully or queue disconnect until completion.

### Phase 1A Deliverables Checklist
- FastAPI app skeleton with tenant-scoped auth
- migration runner with per-tenant status report
- full agent pipeline with typed contracts
- validator stage implemented
- failure mode mapping implemented
- audit append-only enforcement
- rate limiting middleware
- PII confidence-tier schema and override persistence
- connector lifecycle specification

### Phase 1A Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Tenant isolation API test | org A cannot access org B resources |
| Tenant isolation DB permission test | app role cannot query another tenant schema directly |
| Migration runner resilience | one tenant migration failure does not block others |
| RBAC enforcement | viewer blocked from execution endpoints |
| Planner validation check | nonexistent table/column rejected before execution |
| Agent happy path (stub) | full planner->composer flow returns typed response |
| Failure class mapping | each simulated failure maps to expected normalized code |
| Retry policy correctness | retryable classes retried; terminal classes not retried |
| Audit immutability | UPDATE/DELETE on audit rows denied for app runtime role |
| Security event logging | critic allow/rewrite/block actions recorded in audit log |
| Rate limit behavior | configured 429 and concurrency behavior observed |
| PII override persistence | user override prevents repeated re-flagging |
| Real-query gate | any real execution endpoint blocked while feature flag is false |
| Baseline load test | 10 concurrent org sessions with stub workflows complete within target SLA and <1% error |

---

## Exit Criteria to Start Next Shared Phase
Proceed to next phase only when all required tests above pass and the hard gate remains enforced:
- `ALLOW_REAL_QUERY_EXECUTION=false`
- no unresolved high-severity findings in Phase 0/1A acceptance tests

---

## Phase 1B - Workflow Engine + Resumability (2-3 weeks)

### Purpose
Introduce durable workflows after core contracts exist. This phase must be fully testable using a mock executor while real query execution remains disabled.

### 1B.1 Mock Query Executor (Contract-Compatible)
Implement a drop-in replacement for BigQuery tools with deterministic, configurable behavior.

Required capabilities:
- configurable schema responses per dataset/table
- configurable profiling stats (row counts, null rates, distinct estimates)
- dry-run response with `bytes_scanned_estimate`
- configurable latency and injected failures

Failure mode catalog (test-controllable):
- `TIMEOUT`
- `QUOTA_EXCEEDED`
- `PERMISSION_DENIED`
- `TRANSIENT_ERROR`
- `MALFORMED_RESPONSE`
- `PARTIAL_RESULTS`

Expected workflow behavior:
- `TIMEOUT`: bounded retry, optional sampling strategy on retry path
- `QUOTA_EXCEEDED`: stop step, expose scope-reduction options
- `PERMISSION_DENIED`: terminal abort + re-auth guidance
- `TRANSIENT_ERROR`: exponential retry then continue on success
- `MALFORMED_RESPONSE`: terminal abort + investigation logging
- `PARTIAL_RESULTS`: post-exec critic flags quality warning

### 1B.2 Prefect Flows (v1 Core)
Implement three flows:
- `ProfileWarehouseFlow`
- `GenerateDashboardFlow`
- `GenerateMemoFlow`

Each flow step must persist standardized `StepResult`:
- `step_name`
- `status` (`success|failed|skipped`)
- `output`
- `output_hash`
- `started_at`
- `finished_at`
- `retry_count`
- `error` (sanitized)

### 1B.3 Idempotency + Resume Rules
Idempotency key:
- `org_id + workflow_id + step_name + SHA256(input_payload)`

Rule:
- if the same key already succeeded, reuse cached output and skip execution.

Resume behavior:
- load prior step states
- resume from first non-success step only
- preserve previous outputs for downstream dependencies

### 1B.4 Retry Policy
Defaults:
- max retries: 3
- backoff: exponential `1s, 4s, 16s`

Retryable:
- `TRANSIENT_ERROR`
- `TIMEOUT` (first occurrence, then sampling fallback path)

Non-retryable:
- `PERMISSION_DENIED`
- `MALFORMED_RESPONSE`
- validation/policy failures from prior stages

### 1B.5 Failure UX Contract
On exhausted failure, response must include:
- overall workflow status (`partial_failure`)
- completed steps with short summaries
- failed step name + sanitized error
- retry count
- available recovery actions

Recovery actions required:
- `retry`
- `retry_with_sampling`
- `skip_and_continue` (only when step dependency graph allows)

### 1B.6 Concurrency + Queueing + DLQ
Controls:
- per-org max 3 concurrent workflows
- overflow queued with queue position surfaced to UI/API

Dead-letter queue requirements:
- store workflow id, org id, full step state history, error history, original input hash, final failure class
- searchable by org and date range
- explicit requeue operation with audit trail

### 1B.7 Mock-to-Real Cutover Guard
Even if 1B is complete, no real execution handoff until:
- Phase 2 security pass complete
- `ALLOW_REAL_QUERY_EXECUTION` explicitly enabled in approved environment
- integration smoke test passes with real connector in non-prod tenant

### Phase 1B Deliverables Checklist
- mock executor with configurable behavior and failure scheduling
- Prefect flow definitions for profile/dashboard/memo
- step persistence model and idempotency enforcement
- resume-from-last-success behavior
- retry/backoff policies implemented
- per-org workflow concurrency limits
- DLQ storage + requeue tooling
- failure UX payload contract
- cutover checklist document

### Phase 1B Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Resume from failure | timeout at step 3, rerun resumes at step 3; steps 1-2 not re-executed |
| Idempotency | identical workflow inputs do not create duplicate artifacts |
| Concurrency enforcement | with 10 workflows and limit 3: 3 run, 7 queue, all complete or terminally fail with traceable reason |
| Timeout handling | timeout retries occur and fallback policy applied correctly |
| Quota handling | quota-exceeded step blocks and surfaces scope-reduction actions |
| Permission handling | permission-denied aborts without retries and returns re-auth guidance |
| Transient recovery | transient error succeeds within retry budget and workflow continues |
| Malformed response | malformed response creates terminal failure and DLQ record |
| Partial results warning | partial results are flagged by post-exec critic and included in workflow warnings |
| Dead-letter integrity | exhausted workflow creates DLQ record with full step/error history |
| Recovery action wiring | `retry_with_sampling` re-runs failed step with sampling flag and records action in audit |
| Failure UX payload | API/UI receives completed step summaries plus failed-step detail and actions |

---

## Exit Criteria to Start Next Shared Phase
Proceed to next phase only when:
- all Phase 1B acceptance tests pass
- mock-to-real cutover checklist is approved
- `ALLOW_REAL_QUERY_EXECUTION=false` remains enforced until Phase 2 completion

---

## Phase 2 - Security and Cost Gates (2 weeks)

### Purpose
This phase is a hard dependency. Until every Phase 2 acceptance test passes, the system must not execute real warehouse queries.

### 2.1 SQL Safety Rule Engine
Implement parsed-SQL policy evaluation using `sqlglot` (no regex-only gatekeeping).

Evaluation order:
1. block rules
2. require rules
3. enforce rules

Block rules (non-overridable):
- multi-statement SQL
- DDL operations: `CREATE`, `ALTER`, `DROP`, `TRUNCATE`
- DML operations: `INSERT`, `UPDATE`, `DELETE`, `MERGE`
- system catalog modification attempts
- comment-embedded dangerous payload attempts

Require rules (auto-fix or explicit approval path):
- apply default `LIMIT` on non-aggregated `SELECT` (default 10000, org-configurable)
- require partition/time filter for partitioned tables
- require time-based `WHERE` filter for large tables (default threshold 1M estimated rows)

Enforce caps (hard limits, org-configurable):
- per-query bytes scan max (default 10 GB)
- approved override bytes scan max (default 100 GB)
- join depth max (default 5)
- subquery nesting max (default 3)
- query timeout (default 120 seconds)

### 2.2 BigQuery Dry-Run Cost Estimation (Mandatory Pre-Execution)
Before any execution:
- run BigQuery dry-run
- extract `totalBytesProcessed`
- compute estimated cost (`bytes / 1TB * $5`)

Decision policy:
- under per-query cap and under hourly org budget: execute
- over per-query cap but under hourly budget: require explicit user approval
- over hourly budget: block and suggest narrower scope/sampling

UI/API payload must include:
- estimated bytes
- estimated dollar cost
- per-query limit status
- hourly budget remaining

### 2.3 Cost-Based Rate Limiting (Sliding Window)
Implement per-org rolling budget with Redis sorted sets (window: last 3600 seconds).

Required behavior:
- remove expired entries continuously
- sum bytes in rolling window
- deny when projected usage exceeds budget
- track actual bytes post-query to reconcile estimate drift

Required fields in budget response:
- `allowed`
- `bytes_used`
- `bytes_remaining`
- `budget`
- `suggestion`

### 2.4 Secrets Management
All connector credentials encrypted at rest via envelope encryption.

Implementations:
- self-hosted: libsodium + env-provided master key
- cloud: managed KMS

Rules:
- secrets only accessed through `SecretsManager` service
- no raw secret reads from app service code
- no secrets in logs, API responses, prompts, or audit payloads
- reconnect/rotation flow must support zero-downtime credential swap with rollback on failed validation

### 2.5 Prompt Injection Defenses
Direct defense requirements:
- strict separation of system instructions, user content, tool schemas
- tool outputs wrapped in explicit bounded sections
- tool input JSON schema validation before execution

Indirect defense requirements (metadata-originating attacks):
- sanitize table/column identifiers to safe allowlist chars
- truncate untrusted values
- strip template/control markers
- do not include raw sampled row values in LLM prompt context

LLM context allowlist:
- column/table identifiers (sanitized)
- aggregated profile stats
- controlled descriptions

### 2.6 PII Detection and Redaction
Implement two-tier confidence model.

High-confidence (auto-tag + protected by default):
- value-pattern-confirmed detections from bounded samples (e.g., email/phone/card/SSN patterns by threshold)

Low-confidence (review queue):
- name-heuristic-only detections without confirming value evidence

Default protections:
- high-confidence PII columns redacted in outputs
- selecting high-confidence PII requires explicit per-query approval
- low-confidence tags do not block until user confirms

Redaction format:
- `[REDACTED:<pii_type>]`

Bulk review requirements:
- confirm/reject all high-confidence in one action
- persist reviewer decisions to avoid repeated prompts

### 2.7 Hard Gate Wiring
Security gate must be enforced centrally in execution path:
- if Phase 2 gate status is not `PASS`, any real query attempt returns blocked response
- only authorized environment config may set `ALLOW_REAL_QUERY_EXECUTION=true`
- every allow/block decision writes `security_gate_decision` audit event

### Phase 2 Deliverables Checklist
- `sqlglot`-based SQL safety engine with block/require/enforce rule sets
- mandatory BigQuery dry-run and cost estimation path
- Redis sliding-window cost limiter
- secrets manager service + encrypted storage + rotation flow
- log redaction middleware
- prompt/metadata sanitization pipeline
- two-tier PII detection + review/override APIs
- central hard-gate enforcement for real execution

### Phase 2 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Multi-statement blocked | `SELECT 1; DROP TABLE x` rejected pre-execution |
| DDL blocked | any `CREATE/ALTER/DROP/TRUNCATE` rejected |
| DML blocked | any `INSERT/UPDATE/DELETE/MERGE` rejected |
| Partition filter enforcement | missing required partition/time filter causes rewrite or block with reason |
| LIMIT enforcement | non-aggregated select without limit rewritten with org default limit |
| Dry-run mandatory | no query executes without prior dry-run estimate record |
| Over-limit approval path | over per-query threshold requires explicit approval action before execution |
| Hourly budget block | query exceeding rolling hourly budget is blocked with remaining budget detail |
| Sliding-window correctness | boundary-time queries still counted in rolling hour |
| Secrets non-disclosure | credential material absent across logs, API responses, audit entries, prompts |
| Secrets rotation | credential swap succeeds without breaking active connection after validation |
| Metadata sanitization | unsafe identifier strings are sanitized before LLM context inclusion |
| Direct injection resilience | adversarial user prompt does not bypass tool/rule pipeline |
| Indirect injection resilience | malicious metadata content does not alter system/tool behavior |
| PII high-confidence policy | high-confidence PII auto-tagged and protected by default |
| PII low-confidence policy | low-confidence fields surfaced for review without default block |
| PII bulk actions | bulk confirm/reject persists tags in one operation |
| Real-query hard gate | with gate disabled, all real execution requests are blocked |

---

## Exit Criteria to Start Next Shared Phase
Proceed only when:
- all Phase 2 acceptance tests pass
- security gate status marked `PASS` in release checklist
- production rollout explicitly approves enabling `ALLOW_REAL_QUERY_EXECUTION=true`

---

## Phase 3 - BigQuery Connector + Profiler + Caching (2 weeks)

### Purpose
Replace mock execution with the real BigQuery connector, produce a reliable profile catalog, and introduce caching to avoid repeated expensive operations.

### 3.1 BigQuery Connector (Real Execution Path)
Authentication:
- service account JSON stored encrypted via `SecretsManager`
- BigQuery client initialized with least-privilege scoped credentials
- read-only protection remains enforced by Phase 2 SQL gate regardless of IAM permissions

Schema introspection:
- discover dataset/table/column metadata via `INFORMATION_SCHEMA`
- include partitioning and clustering metadata
- cache introspection payloads with connection-scoped keys

Execution pipeline (required order):
1. SQL safety rule engine
2. plan validator (table/column existence)
3. BigQuery dry-run + estimated bytes/cost
4. cost budget check
5. PII policy check
6. execute with timeout
7. audit log write

### 3.2 Redis Caching Layer
Use cache-aside pattern for all read-heavy metadata/profile queries.

Required cache domains:
- schema introspection
- partition metadata
- row count estimates
- column stats
- query results

Required behavior:
- include `cache_hit` flag in service response metadata
- manual invalidation endpoint per connection (`refresh`)
- automatic invalidation on reconnect/credential rotation
- TTLs configurable with sane defaults per cache type

### 3.3 Data Profiler
Persist outputs into tenant-scoped catalog tables (`catalog_tables`, `catalog_columns`).

Table-level profile:
- row count estimate
- table size bytes
- partition/clustering metadata
- freshness (latest timestamp in chosen time column)
- hours since last arrival

Column-level profile:
- null percentage
- approximate distinct count
- min/max for numeric/time columns
- top values for low-cardinality columns only
- bounded sample handling for PII detection (no raw sampled values in LLM context)

Sampling strategy:
- `<1M rows`: full profile
- `1M-100M`: sampled profile (default 10%)
- `>100M`: sampled profile (default 1%) with explicit full-profile option and cost preview

### 3.4 Starter Table Recommendations
Heuristic classifier suggests likely KPI source tables.

Required categories:
- user/account entity
- transaction/order entity
- event stream entity
- state-tracking entity
- config/reference table (excluded from KPI defaults)

UX requirements:
- show confidence + explanation for each recommendation
- user confirms/rejects each recommendation
- rejected tables excluded from subsequent recommendation runs for that connection

### 3.5 Load Handling + Profiling Orchestration
Profiling runs via Prefect workflows.

Controls:
- max 2 concurrent profiling workflows per org (configurable)
- large schemas processed in batches (default 50 tables per batch)
- per-table profiling timeout (default 60 seconds) with sampling fallback
- progress updates include batch progress and processed table counts

### 3.6 Metabase Tenant Scoping (Implementation)
For each tenant:
- create dedicated Metabase DB connection
- constrain schema visibility to tenant schema only
- place created cards/dashboards in tenant-specific collection

Verification on provisioning:
- run guard query proving other tenant schemas are inaccessible
- mark connection active only after isolation test passes

### Phase 3 Deliverables Checklist
- real BigQuery connector integrated
- full Phase 2 safety pipeline enforced on real execution path
- Redis cache layer with configurable TTLs and invalidation hooks
- profiler implementation for table/column stats
- large-table sampling strategy + cost preview for full profile option
- starter-table recommendation engine with user confirmation workflow
- batched profiling with progress reporting
- per-tenant Metabase connection provisioning with isolation verification

### Phase 3 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Introspection accuracy | discovered schema matches known BigQuery dataset structure |
| Cache hit behavior | repeated metadata/profile lookup within TTL returns `cache_hit=true` |
| Cache invalidation | manual refresh causes next lookup to hit BigQuery, then repopulate cache |
| Cache efficiency | simulated 20-turn session yields >80% cache hit rate for schema/profile lookups |
| Dry-run cost control | oversized query blocked before execution path |
| Profiler small table | full profile on small table completes within target SLA with accurate stats |
| Profiler large table | sampled profile on large table completes within timeout/SLA |
| Column stat quality | null/distinct/min/max remain within accepted tolerance on reference dataset |
| PII integration | profiler tags high-confidence PII columns in catalog as expected |
| Recommendation accuracy | reference dataset maps users/events/config tables to correct categories |
| Concurrency stability | concurrent profiling requests respect org limits and avoid duplicate catalog rows |
| Batch progress reporting | large schema profiling reports batch progress and completes all batches |
| Metabase isolation | tenant A connection cannot access tenant B schema objects |
| Metabase activation guard | provisioning marks active only after successful isolation verification query |

---

## Exit Criteria to Start Next Shared Phase
Proceed only when:
- all Phase 3 acceptance tests pass
- real BigQuery connector runs only through Phase 2 security/cost gates
- tenant-scoped Metabase isolation checks pass in staging

---

## Phase 4 - Metabase Dashboards (2 weeks)

### Purpose
Deliver the first customer-visible value feature: automatic generation of real, interactive dashboards from profiled and confirmed catalog data.

### 4.1 Metabase API Integration
Dashboard generation pipeline:
1. load confirmed catalog fields
2. choose dashboard template(s)
3. generate SQL for each card using confirmed semantic fields
4. validate each query through safety + dry-run checks
5. verify query returns expected shape/data
6. create Metabase cards
7. create/update dashboard and place cards
8. wire global date filter
9. persist artifact version + metadata

Required Metabase API operations:
- create card
- create dashboard
- update dashboard cards/positions
- update dashboard filter parameters

Error handling requirement:
- if a step fails after partial card creation, run compensating cleanup to avoid orphaned cards/dashboards
- all failures must preserve step-level diagnostics for retry

### 4.2 Layout Engine
Grid:
- 18-unit width grid
- support half-width (9) and full-width (18) cards

Card size conventions:
- scalar: height 4
- sparkline: height 6
- line/bar: height 8
- table: height 10
- heatmap: height 12

Layout behavior:
- deterministic top-to-bottom placement
- no overlap
- full-width cards occupy entire row
- half-width cards pair left/right before row advance

### 4.3 Dashboard Templates
Minimum templates:
- `Exec Overview`
- `Revenue/Volume`
- `Data Health`

Template selection rules:
- users + events -> include `Exec Overview`
- amount/revenue signal present -> include `Revenue/Volume`
- always include `Data Health`

Template SQL rules:
- generated SQL must reference only confirmed catalog fields
- all SQL must pass Phase 2 rule engine and cost checks
- card query outputs must match expected visualization schema

### 4.4 Global Filter Wiring
Global date filter requirements:
- default range `last 14 days`
- mapped to each time-series card time column
- user filter changes propagate across mapped cards consistently

### 4.5 Dashboard Idempotency + Versioning
Regeneration behavior:
- one logical dashboard per org + template
- regenerate updates existing dashboard instead of creating duplicates
- artifact version increments atomically
- prior versions retained read-only

Version metadata requirements:
- query hashes
- card definitions/layout snapshot
- generation timestamp
- catalog snapshot hash
- template version id

### 4.6 Empty Data and User Messaging
If query is valid but returns no rows for selected period:
- render explicit empty state (`No data for this period`)
- do not surface internal SQL errors
- preserve filter controls and card shell

### Phase 4 Deliverables Checklist
- Metabase API integration for card/dashboard lifecycle
- deterministic layout engine with half/full width support
- three baseline dashboard templates
- catalog-driven template selection logic
- global date filter wiring across cards
- idempotent regeneration and artifact version history
- compensation/cleanup path for partial Metabase failures
- empty-state rendering behavior for no-data results

### Phase 4 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Dashboard generation time | exec dashboard generation completes within target SLA (<5 minutes on reference dataset) |
| Layout correctness | no overlapping cards; widths/heights follow spec |
| Filter propagation | changing global date filter updates all mapped time-series cards |
| SQL safety compliance | every generated query passes Phase 2 safety/cost gates |
| SQL usefulness | generated card queries return expected shape and non-error outputs on reference dataset |
| Timezone correctness | dashboard metrics reflect user-configured timezone logic |
| Idempotent regeneration | second generation updates existing dashboard and increments version without duplicates |
| Version history | prior dashboard versions remain accessible as read-only snapshots |
| Template selection | template inclusion/exclusion follows catalog signal rules |
| API failure cleanup | simulated Metabase failure leaves no orphaned artifacts and records retryable failure context |
| Empty data behavior | zero-row queries display user-friendly empty state, not error |

---

## Exit Criteria to Start Next Shared Phase
Proceed only when:
- all Phase 4 acceptance tests pass
- dashboard regeneration is idempotent in staging
- Metabase failure compensation path is verified

---

## Phase 5 - Weekly Memo Pipeline (1-2 weeks)

### Purpose
Generate a trustworthy weekly executive narrative grounded in deterministic packet computation and post-generation validation.

### 5.1 Memo Packet Computation (Deterministic, SQL-Driven)
LLM input must be a structured memo packet only. No raw table data in prompt context.

Pipeline:
1. determine current and previous 7-day complete windows in user timezone
2. compute KPI values for both windows using dashboard-linked KPI queries
3. compute absolute and percent deltas with edge-case handling (`previous=0`, `current=0`)
4. assign significance labels from org-configurable thresholds
5. compute segment contribution analysis when dimensions are confirmed
6. append data quality annotations (freshness, missing days, profiler anomalies)

Packet requirements:
- include query hashes for provenance
- include timezone used for all computations
- include anomaly notes as explicit structured list

### 5.2 LLM Memo Generation Contract
Use strict JSON output schema only.

Rules:
- no invented metrics
- all cited numbers must exist in packet
- likely causes must be tagged `data_supported` or `speculative`
- data-quality warnings from packet must be surfaced

Model output must include:
- headline summary bullets
- key changes with packet-matched numeric fields
- likely causes with evidence type and evidence text
- recommended actions
- data quality notes

### 5.3 Automated Memo Validation
Run post-generation validation before memo is accepted.

Required checks:
1. number reconciliation against packet values
2. required coverage for all notable-or-higher KPI changes
3. hallucination check (metric names must exist in packet KPI set)
4. cause-evidence validation (`data_supported` claims must map to packet evidence; downgrade otherwise)

Failure policy:
- retry generation up to 2 times
- if still invalid, return deterministic raw metrics table fallback with explicit message

### 5.4 Memo Versioning + Storage
Store each memo as versioned artifact with:
- artifact id/version
- packet hash
- query hash list
- model identifier/version
- generation timestamp
- validation status/warnings

Week-over-week comparison:
- deterministic packet diff table (not LLM-generated)
- show metric previous/current/change with hashes for provenance

### Phase 5 Deliverables Checklist
- deterministic memo packet pipeline
- significance classification with configurable thresholds
- optional segment contribution module
- strict-schema LLM memo generation
- 4-step validation pipeline with retry/fallback behavior
- memo artifact versioning with packet/query hash linkage
- week-over-week structured comparison view

### Phase 5 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Packet computation correctness | reference dataset produces correct current/previous values and deltas |
| Significance thresholds | threshold behavior aligns with org-configured rules |
| Number reconciliation | mismatched cited numbers are detected and trigger regeneration |
| Hallucination rejection | unknown metric names cause rejection/regeneration |
| Cause evidence validation | invalid `data_supported` claims are rejected or downgraded |
| Speculative labeling | unsupported hypotheses are labeled `speculative` |
| Memo-dashboard consistency | memo KPI values match dashboard query outputs |
| Validation fallback | repeated invalid generations produce raw metrics fallback response |
| Memo versioning | sequential memo runs increment version and store distinct packet hashes |
| WoW comparison accuracy | comparison table reflects exact packet-to-packet changes |
| Missing data propagation | source missing-days anomalies appear in memo quality notes |
| Timezone correctness | packet windows and values match user timezone rules |

---

## Exit Criteria to Start Next Shared Phase
Proceed only when:
- all Phase 5 acceptance tests pass
- fallback behavior is verified in staging
- memo values and dashboard values reconcile on reference dataset

---

## Phase 6 - Feedback + Graceful Degradation + E2E (1 week)

### Purpose
Close quality loop, harden user experience during outages/failures, and prove end-to-end reliability with automated smoke/load tests.

### 6.1 Feedback Capture
Capture feedback on:
- query responses
- dashboards
- memos

Feedback record requirements:
- org/user/artifact/version linkage
- feedback polarity + optional comment
- prompt/tool hash linkage for analysis
- timestamped immutable record

v1 analytics:
- weekly summary by artifact type and feedback polarity
- correlation view: negative feedback vs prompt/tool hashes

### 6.2 Graceful Degradation
LLM outage/latency:
- existing dashboards remain usable
- new AI requests queued with status messaging
- auto-resume queued requests after recovery

BigQuery outage:
- serve cached metadata/results with freshness timestamp
- block new execution with clear temporary-unavailable message

Partial workflow failure:
- preserve completed step results
- show failed step reason + recovery actions

### 6.3 Connector Disconnect Flow (Implementation)
On disconnect:
1. inspect in-flight workflows and wait-or-cancel with timeout policy
2. revoke stored credentials
3. mark dependent artifacts stale
4. purge related cache keys
5. remove catalog entries for connection
6. disable Metabase connection
7. log disconnect audit event

### 6.4 Tenant Data Purge Runbook (Manual v1)
Document and test operator runbook:
- verify no active sessions/workflows
- disable Metabase tenant assets
- drop tenant schema
- delete shared-table tenant references
- purge Redis tenant keys
- verify no residual tenant references

Retention caveat:
- preserve audit records according to retention policy from Phase 0/2 (pseudonymized/contract-aware), not ad-hoc deletion.

### 6.5 End-to-End Smoke Test (Per Deploy)
Automated smoke test must validate full path:
- create org/user
- connect source
- profile + catalog populate
- recommendation confirmation
- dashboard generation
- memo generation + validation pass
- feedback submission
- audit trail presence
- cleanup/purge test org

### 6.6 Load Test Suite
Required scenarios:
- concurrent requests within one org (queueing/throttling behavior)
- concurrent operations across multiple tenants (isolation proof)
- sustained request load with latency/error/resource thresholds

### Phase 6 Deliverables Checklist
- feedback capture endpoints + storage + basic analytics view
- graceful degradation handlers for LLM/warehouse outages
- implemented connector disconnect flow
- tested tenant purge runbook
- automated E2E smoke test in CI/CD deployment path
- load test suite with threshold assertions

### Phase 6 Tests / Acceptance
| Test | Pass Criteria |
|---|---|
| Feedback persistence | submitted feedback stored with artifact/version/hash context |
| Feedback retrieval | artifact-scoped feedback query returns complete set |
| LLM outage behavior | dashboards unaffected, AI requests queued, user sees clear status |
| BigQuery outage behavior | cached data remains available, new execution blocked gracefully |
| Partial failure UX | completed outputs remain visible with actionable recovery options |
| Disconnect in-flight handling | active workflows complete/cancel before credential removal |
| Disconnect staleness marking | dependent artifacts marked stale post-disconnect |
| Disconnect cache purge | connection-scoped cache keys removed |
| E2E smoke | full connect->profile->dashboard->memo->feedback flow passes within SLA |
| Load same-org | concurrency limits enforced, no duplicate/corrupt state |
| Load cross-tenant | simultaneous org activity shows zero cross-tenant leakage |
| Sustained load | p95 latency and error-rate thresholds met without resource exhaustion |
| Tenant purge | no tenant data remains in app schemas/caches post-runbook |
| Audit retention | required audit records remain per retention policy after purge |

---

## Build Complete - Ship Checklist
Before first paying customer:
- all phase acceptance tests pass
- E2E smoke test runs on every deploy
- load test thresholds met
- security suite (SQL safety, PII, secrets, audit) passes
- demo dataset path works end-to-end
- tenant isolation verified automatically
- disconnect flow tested
- graceful degradation tested (LLM + warehouse outages)
- feedback capture active for all artifact types
- onboarding docs published
- landing page + demo workflow available
- monitoring/alerting active for critical paths

---

## Appendix - Consolidated Test Matrix
| Layer | Tests | Phase |
|---|---|---|
| Tenancy | isolation, multi-schema migration, purge completeness | 1A, 6 |
| RBAC | role-based execution/view restrictions | 1A |
| Audit | immutability, completeness, retention behavior | 1A, 6 |
| Rate limiting | volume, concurrency, cost sliding-window | 1A, 2 |
| Workflow engine | resume, idempotency, retries, DLQ, partial results | 1B |
| SQL safety | injection resilience, DDL/DML block, structural guards | 2 |
| Cost controls | dry-run gate, query caps, hourly budget, approval path | 2 |
| Secrets | encryption, non-disclosure, rotation correctness | 2 |
| Injection defense | direct and indirect prompt injection hardening | 2 |
| PII | confidence tiers, review flow, redaction behavior | 2 |
| Connector | introspection and execution through full safety stack | 3 |
| Cache | hit rate, invalidation, TTL behavior | 3 |
| Profiler | small/large-table behavior, stat quality, batch progress | 3 |
| Recommendations | entity classification + exclusion logic | 3 |
| Metabase isolation | tenant scoping enforcement | 3 |
| Dashboards | generation speed, layout, filters, idempotency, versioning | 4 |
| Memo packet | deterministic computation + significance logic | 5 |
| Memo validation | reconciliation, hallucination checks, fallback | 5 |
| Memo versioning | version history + WoW comparisons | 5 |
| Feedback | context-rich storage and retrieval | 6 |
| Degradation | LLM/warehouse outage UX continuity | 6 |
| Disconnect | in-flight handling, staleness, cache purge | 6 |
| E2E smoke | full pipeline validation per deploy | 6 |
| Load | same-org, cross-tenant, sustained throughput | 6 |
